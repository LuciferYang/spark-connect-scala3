package org.apache.spark.sql

import org.apache.spark.connect.proto.{Command, CommonInlineUserDefinedFunction, JavaUDF}
import org.apache.spark.sql.connect.client.{DataTypeProtoConverter, SparkConnectClient}

/** Functions for registering user-defined functions (UDFs).
  *
  * Access via `spark.udf`.
  *
  * {{{
  *   spark.udf.register("addOne", udf((x: Int) => x + 1))
  *   spark.sql("SELECT addOne(age) FROM people")
  * }}}
  */
final class UDFRegistration private[sql] (
    private val client: SparkConnectClient
):

  /** Register a UserDefinedFunction under the given name. */
  def register(name: String, udf: UserDefinedFunction): UserDefinedFunction =
    val named = udf.withName(name)
    val proto = named.toProto(functionName = name)
    val command = Command.newBuilder()
      .setRegisterFunction(proto)
      .build()
    client.executeCommand(command)
    named

  /** Register a Function0 as a named UDF. */
  inline def register[R](name: String, func: () => R): UserDefinedFunction =
    register(name, functions.udf(func))

  /** Register a Function1 as a named UDF. */
  inline def register[R, A1](name: String, func: A1 => R): UserDefinedFunction =
    register(name, functions.udf(func))

  /** Register a Function2 as a named UDF. */
  inline def register[R, A1, A2](name: String, func: (A1, A2) => R): UserDefinedFunction =
    register(name, functions.udf(func))

  /** Register a Function3 as a named UDF. */
  inline def register[R, A1, A2, A3](
      name: String,
      func: (A1, A2, A3) => R
  ): UserDefinedFunction =
    register(name, functions.udf(func))

  /** Register a Function4 as a named UDF. */
  inline def register[R, A1, A2, A3, A4](
      name: String,
      func: (A1, A2, A3, A4) => R
  ): UserDefinedFunction =
    register(name, functions.udf(func))

  /** Register a Function5 as a named UDF. */
  inline def register[R, A1, A2, A3, A4, A5](
      name: String,
      func: (A1, A2, A3, A4, A5) => R
  ): UserDefinedFunction =
    register(name, functions.udf(func))

  /** Register a Function6 as a named UDF. */
  inline def register[R, A1, A2, A3, A4, A5, A6](
      name: String,
      func: (A1, A2, A3, A4, A5, A6) => R
  ): UserDefinedFunction =
    register(name, functions.udf(func))

  /** Register a Function7 as a named UDF. */
  inline def register[R, A1, A2, A3, A4, A5, A6, A7](
      name: String,
      func: (A1, A2, A3, A4, A5, A6, A7) => R
  ): UserDefinedFunction =
    register(name, functions.udf(func))

  /** Register a Function8 as a named UDF. */
  inline def register[R, A1, A2, A3, A4, A5, A6, A7, A8](
      name: String,
      func: (A1, A2, A3, A4, A5, A6, A7, A8) => R
  ): UserDefinedFunction =
    register(name, functions.udf(func))

  /** Register a Function9 as a named UDF. */
  inline def register[R, A1, A2, A3, A4, A5, A6, A7, A8, A9](
      name: String,
      func: (A1, A2, A3, A4, A5, A6, A7, A8, A9) => R
  ): UserDefinedFunction =
    register(name, functions.udf(func))

  /** Register a Function10 as a named UDF. */
  inline def register[R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10](
      name: String,
      func: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) => R
  ): UserDefinedFunction =
    register(name, functions.udf(func))

  /** Register a Java UDF class by its fully-qualified class name with an explicit return type.
    *
    * {{{
    *   spark.udf.registerJava("strLen", "com.example.StrLen", IntegerType)
    *   spark.sql("SELECT strLen(name) FROM people")
    * }}}
    */
  def registerJava(name: String, className: String, returnDataType: types.DataType): Unit =
    val command = Command.newBuilder()
      .setRegisterFunction(UDFRegistration.buildJavaUdf(name, className, returnDataType))
      .build()
    client.executeCommand(command)

end UDFRegistration

private[sql] object UDFRegistration:
  /** Build the proto for a Java UDF registration. Extracted for unit testing. */
  private[sql] def buildJavaUdf(
      name: String,
      className: String,
      returnDataType: types.DataType
  ): CommonInlineUserDefinedFunction =
    val javaUdf = JavaUDF.newBuilder()
      .setClassName(className)
      .setOutputType(DataTypeProtoConverter.toProto(returnDataType))
      .setAggregate(false)
      .build()
    CommonInlineUserDefinedFunction.newBuilder()
      .setFunctionName(name)
      .setJavaUdf(javaUdf)
      .build()
