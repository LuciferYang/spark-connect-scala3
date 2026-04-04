package org.apache.spark.sql

import com.google.protobuf.ByteString
import org.apache.spark.connect.proto.expressions.{
  CommonInlineUserDefinedFunction,
  Expression,
  ScalarScalaUDF
}
import org.apache.spark.connect.proto.expressions.Expression.ExprType
import org.apache.spark.sql.connect.client.DataTypeProtoConverter
import org.apache.spark.sql.types.DataType

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

/**
 * A user-defined function that can be applied to Columns.
 *
 * Created via `functions.udf(...)` factory methods. The UDF is serialized
 * and sent to the Spark Connect server for execution.
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
    Column(Expression(exprType = ExprType.CommonInlineUserDefinedFunction(proto)))

  /** Build the proto message for this UDF. */
  private[sql] def toProto(
      arguments: Seq[Expression] = Seq.empty,
      functionName: String = _name.getOrElse("")
  ): CommonInlineUserDefinedFunction =
    val payload = serializeFunction(func)
    val scalaUdf = ScalarScalaUDF(
      payload = ByteString.copyFrom(payload),
      inputTypes = inputTypes.map(DataTypeProtoConverter.toProto),
      outputType = Some(DataTypeProtoConverter.toProto(returnType)),
      nullable = _nullable
    )
    CommonInlineUserDefinedFunction(
      functionName = functionName,
      deterministic = _deterministic,
      arguments = arguments,
      function = CommonInlineUserDefinedFunction.Function.ScalarScalaUdf(scalaUdf)
    )

  /** Serialize a function closure to bytes using Java serialization. */
  private def serializeFunction(f: AnyRef): Array[Byte] =
    val bos = ByteArrayOutputStream()
    val oos = ObjectOutputStream(bos)
    try
      oos.writeObject(f)
      oos.flush()
      bos.toByteArray
    finally
      oos.close()
      bos.close()

object UserDefinedFunction:
  /** Create a UserDefinedFunction. Used by inline udf() factory methods. */
  def apply(
      func: AnyRef,
      returnType: DataType,
      inputTypes: Seq[DataType]
  ): UserDefinedFunction =
    new UserDefinedFunction(func, returnType, inputTypes)
