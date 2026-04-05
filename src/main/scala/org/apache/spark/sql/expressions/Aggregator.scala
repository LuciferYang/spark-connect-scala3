package org.apache.spark.sql.expressions

import org.apache.spark.sql.Encoder

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
