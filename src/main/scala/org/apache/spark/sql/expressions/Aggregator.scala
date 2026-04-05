package org.apache.spark.sql.expressions

import com.google.protobuf.ByteString
import org.apache.spark.connect.proto.{Expression, ScalarScalaUDF, TypedAggregateExpression}
import org.apache.spark.sql.{Encoder, Encoders, TypedColumn}
import org.apache.spark.sql.connect.client.DataTypeProtoConverter
import org.apache.spark.sql.connect.common.UdfPacket

/** A base class for user-defined aggregations, which can be used in `Dataset` operations to take
  * all of the elements of a group and reduce them to a single value.
  *
  * For example, the following aggregator extracts a `Long` from each row and sums them:
  * {{{
  *   import org.apache.spark.sql.{Encoder, Encoders}
  *   import org.apache.spark.sql.expressions.Aggregator
  *
  *   val summer = new Aggregator[Long, Long, Long] {
  *     def zero: Long = 0L
  *     def reduce(b: Long, a: Long): Long = b + a
  *     def merge(b1: Long, b2: Long): Long = b1 + b2
  *     def finish(reduction: Long): Long = reduction
  *     def bufferEncoder: Encoder[Long] = Encoders.scalaLong
  *     def outputEncoder: Encoder[Long] = Encoders.scalaLong
  *   }
  *
  *   val myUdaf = functions.udaf(summer, Encoders.scalaLong)
  *   df.agg(myUdaf(col("value")))
  * }}}
  *
  * @tparam IN
  *   The input type for the aggregation.
  * @tparam BUF
  *   The type of the intermediate value of the reduction.
  * @tparam OUT
  *   The type of the final output result.
  */
@SerialVersionUID(2093413866369130093L)
abstract class Aggregator[-IN, BUF, OUT] extends Serializable:

  /** A zero value for this aggregation. Should satisfy the property that any b + zero = b. */
  def zero: BUF

  /** Combine two values to produce a new value. For performance, the function may modify `b` and
    * return it instead of constructing new object for b.
    */
  def reduce(b: BUF, a: IN): BUF

  /** Merge two intermediate values. */
  def merge(b1: BUF, b2: BUF): BUF

  /** Transform the output of the reduction. */
  def finish(reduction: BUF): OUT

  /** Specifies the `Encoder` for the intermediate value type. */
  def bufferEncoder: Encoder[BUF]

  /** Specifies the `Encoder` for the final output value type. */
  def outputEncoder: Encoder[OUT]

  /** Returns this `Aggregator` as a [[TypedColumn]] that can be used in
    * [[org.apache.spark.sql.KeyValueGroupedDataset.agg]].
    *
    * The Aggregator is serialized into a `TypedAggregateExpression` proto, which the server unpacks
    * and evaluates as a UDAF.
    */
  def toColumn: TypedColumn[IN, OUT] =
    val outEnc = outputEncoder
    val outAg = Encoders.asAgnostic(outEnc)
    val bufAg = Encoders.asAgnostic(bufferEncoder)
    val packet = UdfPacket(this, Seq(bufAg), outAg)
    val payload = UdfPacket.serialize(packet)
    val scalaUdf = ScalarScalaUDF
      .newBuilder()
      .setPayload(ByteString.copyFrom(payload))
      .setOutputType(DataTypeProtoConverter.toProto(outAg.dataType))
      .setNullable(true)
      .setAggregate(true)
    val expr = Expression
      .newBuilder()
      .setTypedAggregateExpression(
        TypedAggregateExpression
          .newBuilder()
          .setScalarScalaUdf(scalaUdf.build())
      )
      .build()
    TypedColumn(expr, outEnc)
