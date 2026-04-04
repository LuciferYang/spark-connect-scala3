package org.apache.spark.sql

import com.google.protobuf.ByteString
import org.apache.spark.connect.proto.{
  Expression,
  CommonInlineUserDefinedFunction,
  ScalarScalaUDF
}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.*
import org.apache.spark.sql.connect.client.DataTypeProtoConverter
import org.apache.spark.sql.connect.common.UdfPacket
import org.apache.spark.sql.types.*

/** A user-defined function that can be applied to Columns.
  *
  * Created via `functions.udf(...)` factory methods. The UDF is serialized and sent to the Spark
  * Connect server for execution.
  *
  * {{{
  *   val addOne = udf((x: Int) => x + 1)
  *   df.select(addOne(col("age")))
  * }}}
  */
final class UserDefinedFunction private[sql] (
    private[sql] val func: AnyRef,
    private[sql] val returnType: DataType,
    private[sql] val inputTypes: Seq[DataType],
    private val _name: Option[String] = None,
    private val _nullable: Boolean = true,
    private val _deterministic: Boolean = true
):

  /** Return a copy with the given name. */
  def withName(name: String): UserDefinedFunction =
    new UserDefinedFunction(func, returnType, inputTypes, Some(name), _nullable, _deterministic)

  /** Return a copy with the given nullability. */
  def asNonNullable(): UserDefinedFunction =
    new UserDefinedFunction(func, returnType, inputTypes, _name, false, _deterministic)

  /** Return a copy marked as non-deterministic. */
  def asNondeterministic(): UserDefinedFunction =
    new UserDefinedFunction(func, returnType, inputTypes, _name, _nullable, false)

  /** The function name (used for registration or display). */
  def name: Option[String] = _name

  /** Apply this UDF to the given columns, producing a new Column. */
  def apply(cols: Column*): Column =
    val proto = toProto(
      arguments = cols.map(_.expr).toSeq,
      functionName = _name.getOrElse("")
    )
    Column(Expression.newBuilder().setCommonInlineUserDefinedFunction(proto).build())

  /** Build the proto message for this UDF. */
  private[sql] def toProto(
      arguments: Seq[Expression] = Seq.empty,
      functionName: String = _name.getOrElse("")
  ): CommonInlineUserDefinedFunction =
    val payload = serializeFunction(func)
    val scalaUdfBuilder = ScalarScalaUDF.newBuilder()
      .setPayload(ByteString.copyFrom(payload))
      .setOutputType(DataTypeProtoConverter.toProto(returnType))
      .setNullable(_nullable)
    inputTypes.foreach(dt => scalaUdfBuilder.addInputTypes(DataTypeProtoConverter.toProto(dt)))
    val udfBuilder = CommonInlineUserDefinedFunction.newBuilder()
      .setFunctionName(functionName)
      .setDeterministic(_deterministic)
      .setScalarScalaUdf(scalaUdfBuilder.build())
    arguments.foreach(udfBuilder.addArguments)
    udfBuilder.build()

  /** Serialize a function closure wrapped in a UdfPacket. */
  private def serializeFunction(f: AnyRef): Array[Byte] =
    val inputEncs = inputTypes.map(encoderForType)
    val outputEnc = encoderForType(returnType)
    val packet = UdfPacket(f, inputEncs, outputEnc)
    UdfPacket.serialize(packet)

  /** Map a Spark DataType to the corresponding AgnosticEncoder for UdfPacket serialization. */
  private def encoderForType(dt: DataType): AgnosticEncoder[?] = dt match
    case IntegerType => PrimitiveIntEncoder
    case LongType    => PrimitiveLongEncoder
    case DoubleType  => PrimitiveDoubleEncoder
    case FloatType   => PrimitiveFloatEncoder
    case ShortType   => PrimitiveShortEncoder
    case ByteType    => PrimitiveByteEncoder
    case BooleanType => PrimitiveBooleanEncoder
    case StringType  => StringEncoder
    case BinaryType  => BinaryEncoder
    case _           => StringEncoder // fallback

object UserDefinedFunction:
  /** Create a UserDefinedFunction. Used by inline udf() factory methods. */
  def apply(
      func: AnyRef,
      returnType: DataType,
      inputTypes: Seq[DataType]
  ): UserDefinedFunction =
    new UserDefinedFunction(func, returnType, inputTypes)
