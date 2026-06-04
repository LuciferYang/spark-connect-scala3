package org.apache.spark.sql

import org.apache.spark.connect.proto.{Command, CommonInlineUserDefinedFunction, JavaUDF}
import org.apache.spark.sql.api.java.*
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

  /** Register a Function11 as a named UDF. */
  inline def register[R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11](
      name: String,
      func: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11) => R
  ): UserDefinedFunction =
    register(name, functions.udf(func))

  /** Register a Function12 as a named UDF. */
  inline def register[R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12](
      name: String,
      func: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12) => R
  ): UserDefinedFunction =
    register(name, functions.udf(func))

  /** Register a Function13 as a named UDF. */
  inline def register[R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13](
      name: String,
      func: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13) => R
  ): UserDefinedFunction =
    register(name, functions.udf(func))

  /** Register a Function14 as a named UDF. */
  inline def register[
      R,
      A1,
      A2,
      A3,
      A4,
      A5,
      A6,
      A7,
      A8,
      A9,
      A10,
      A11,
      A12,
      A13,
      A14
  ](
      name: String,
      func: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14) => R
  ): UserDefinedFunction =
    register(name, functions.udf(func))

  /** Register a Function15 as a named UDF. */
  inline def register[
      R,
      A1,
      A2,
      A3,
      A4,
      A5,
      A6,
      A7,
      A8,
      A9,
      A10,
      A11,
      A12,
      A13,
      A14,
      A15
  ](
      name: String,
      func: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15) => R
  ): UserDefinedFunction =
    register(name, functions.udf(func))

  /** Register a Function16 as a named UDF. */
  inline def register[
      R,
      A1,
      A2,
      A3,
      A4,
      A5,
      A6,
      A7,
      A8,
      A9,
      A10,
      A11,
      A12,
      A13,
      A14,
      A15,
      A16
  ](
      name: String,
      func: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16) => R
  ): UserDefinedFunction =
    register(name, functions.udf(func))

  /** Register a Function17 as a named UDF. */
  inline def register[
      R,
      A1,
      A2,
      A3,
      A4,
      A5,
      A6,
      A7,
      A8,
      A9,
      A10,
      A11,
      A12,
      A13,
      A14,
      A15,
      A16,
      A17
  ](
      name: String,
      func: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17) => R
  ): UserDefinedFunction =
    register(name, functions.udf(func))

  /** Register a Function18 as a named UDF. */
  inline def register[
      R,
      A1,
      A2,
      A3,
      A4,
      A5,
      A6,
      A7,
      A8,
      A9,
      A10,
      A11,
      A12,
      A13,
      A14,
      A15,
      A16,
      A17,
      A18
  ](
      name: String,
      func: (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18) => R
  ): UserDefinedFunction =
    register(name, functions.udf(func))

  /** Register a Function19 as a named UDF. */
  inline def register[
      R,
      A1,
      A2,
      A3,
      A4,
      A5,
      A6,
      A7,
      A8,
      A9,
      A10,
      A11,
      A12,
      A13,
      A14,
      A15,
      A16,
      A17,
      A18,
      A19
  ](
      name: String,
      func: (
          A1,
          A2,
          A3,
          A4,
          A5,
          A6,
          A7,
          A8,
          A9,
          A10,
          A11,
          A12,
          A13,
          A14,
          A15,
          A16,
          A17,
          A18,
          A19
      ) => R
  ): UserDefinedFunction =
    register(name, functions.udf(func))

  /** Register a Function20 as a named UDF. */
  inline def register[
      R,
      A1,
      A2,
      A3,
      A4,
      A5,
      A6,
      A7,
      A8,
      A9,
      A10,
      A11,
      A12,
      A13,
      A14,
      A15,
      A16,
      A17,
      A18,
      A19,
      A20
  ](
      name: String,
      func: (
          A1,
          A2,
          A3,
          A4,
          A5,
          A6,
          A7,
          A8,
          A9,
          A10,
          A11,
          A12,
          A13,
          A14,
          A15,
          A16,
          A17,
          A18,
          A19,
          A20
      ) => R
  ): UserDefinedFunction =
    register(name, functions.udf(func))

  /** Register a Function21 as a named UDF. */
  inline def register[
      R,
      A1,
      A2,
      A3,
      A4,
      A5,
      A6,
      A7,
      A8,
      A9,
      A10,
      A11,
      A12,
      A13,
      A14,
      A15,
      A16,
      A17,
      A18,
      A19,
      A20,
      A21
  ](
      name: String,
      func: (
          A1,
          A2,
          A3,
          A4,
          A5,
          A6,
          A7,
          A8,
          A9,
          A10,
          A11,
          A12,
          A13,
          A14,
          A15,
          A16,
          A17,
          A18,
          A19,
          A20,
          A21
      ) => R
  ): UserDefinedFunction =
    register(name, functions.udf(func))

  /** Register a Function22 as a named UDF. */
  inline def register[
      R,
      A1,
      A2,
      A3,
      A4,
      A5,
      A6,
      A7,
      A8,
      A9,
      A10,
      A11,
      A12,
      A13,
      A14,
      A15,
      A16,
      A17,
      A18,
      A19,
      A20,
      A21,
      A22
  ](
      name: String,
      func: (
          A1,
          A2,
          A3,
          A4,
          A5,
          A6,
          A7,
          A8,
          A9,
          A10,
          A11,
          A12,
          A13,
          A14,
          A15,
          A16,
          A17,
          A18,
          A19,
          A20,
          A21,
          A22
      ) => R
  ): UserDefinedFunction =
    register(name, functions.udf(func))

  /** Register a Java UDF0 instance as a named UDF. */
  def register(name: String, f: UDF0[?], returnType: types.DataType): Unit =
    registerJavaInstance(name, f, returnType)

  /** Register a Java UDF1 instance as a named UDF. */
  def register(name: String, f: UDF1[?, ?], returnType: types.DataType): Unit =
    registerJavaInstance(name, f, returnType)

  /** Register a Java UDF2 instance as a named UDF. */
  def register(name: String, f: UDF2[?, ?, ?], returnType: types.DataType): Unit =
    registerJavaInstance(name, f, returnType)

  /** Register a Java UDF3 instance as a named UDF. */
  def register(name: String, f: UDF3[?, ?, ?, ?], returnType: types.DataType): Unit =
    registerJavaInstance(name, f, returnType)

  /** Register a Java UDF4 instance as a named UDF. */
  def register(name: String, f: UDF4[?, ?, ?, ?, ?], returnType: types.DataType): Unit =
    registerJavaInstance(name, f, returnType)

  /** Register a Java UDF5 instance as a named UDF. */
  def register(name: String, f: UDF5[?, ?, ?, ?, ?, ?], returnType: types.DataType): Unit =
    registerJavaInstance(name, f, returnType)

  /** Register a Java UDF6 instance as a named UDF. */
  def register(name: String, f: UDF6[?, ?, ?, ?, ?, ?, ?], returnType: types.DataType): Unit =
    registerJavaInstance(name, f, returnType)

  /** Register a Java UDF7 instance as a named UDF. */
  def register(name: String, f: UDF7[?, ?, ?, ?, ?, ?, ?, ?], returnType: types.DataType): Unit =
    registerJavaInstance(name, f, returnType)

  /** Register a Java UDF8 instance as a named UDF. */
  def register(name: String, f: UDF8[?, ?, ?, ?, ?, ?, ?, ?, ?], returnType: types.DataType): Unit =
    registerJavaInstance(name, f, returnType)

  /** Register a Java UDF9 instance as a named UDF. */
  def register(
      name: String,
      f: UDF9[?, ?, ?, ?, ?, ?, ?, ?, ?, ?],
      returnType: types.DataType
  ): Unit =
    registerJavaInstance(name, f, returnType)

  /** Register a Java UDF10 instance as a named UDF. */
  def register(
      name: String,
      f: UDF10[?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?],
      returnType: types.DataType
  ): Unit =
    registerJavaInstance(name, f, returnType)

  /** Register a Java UDF11 instance as a named UDF. */
  def register(
      name: String,
      f: UDF11[?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?],
      returnType: types.DataType
  ): Unit =
    registerJavaInstance(name, f, returnType)

  /** Register a Java UDF12 instance as a named UDF. */
  def register(
      name: String,
      f: UDF12[?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?],
      returnType: types.DataType
  ): Unit =
    registerJavaInstance(name, f, returnType)

  /** Register a Java UDF13 instance as a named UDF. */
  def register(
      name: String,
      f: UDF13[?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?],
      returnType: types.DataType
  ): Unit =
    registerJavaInstance(name, f, returnType)

  /** Register a Java UDF14 instance as a named UDF. */
  def register(
      name: String,
      f: UDF14[?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?],
      returnType: types.DataType
  ): Unit =
    registerJavaInstance(name, f, returnType)

  /** Register a Java UDF15 instance as a named UDF. */
  def register(
      name: String,
      f: UDF15[?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?],
      returnType: types.DataType
  ): Unit =
    registerJavaInstance(name, f, returnType)

  /** Register a Java UDF16 instance as a named UDF. */
  def register(
      name: String,
      f: UDF16[?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?],
      returnType: types.DataType
  ): Unit =
    registerJavaInstance(name, f, returnType)

  /** Register a Java UDF17 instance as a named UDF. */
  def register(
      name: String,
      f: UDF17[?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?],
      returnType: types.DataType
  ): Unit =
    registerJavaInstance(name, f, returnType)

  /** Register a Java UDF18 instance as a named UDF. */
  def register(
      name: String,
      f: UDF18[?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?],
      returnType: types.DataType
  ): Unit =
    registerJavaInstance(name, f, returnType)

  /** Register a Java UDF19 instance as a named UDF. */
  def register(
      name: String,
      f: UDF19[?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?],
      returnType: types.DataType
  ): Unit =
    registerJavaInstance(name, f, returnType)

  /** Register a Java UDF20 instance as a named UDF. */
  def register(
      name: String,
      f: UDF20[?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?],
      returnType: types.DataType
  ): Unit =
    registerJavaInstance(name, f, returnType)

  /** Register a Java UDF21 instance as a named UDF. */
  def register(
      name: String,
      f: UDF21[?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?],
      returnType: types.DataType
  ): Unit =
    registerJavaInstance(name, f, returnType)

  /** Register a Java UDF22 instance as a named UDF. */
  def register(
      name: String,
      f: UDF22[?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?],
      returnType: types.DataType
  ): Unit =
    registerJavaInstance(name, f, returnType)

  private def registerJavaInstance(
      name: String,
      func: AnyRef,
      returnType: types.DataType
  ): Unit =
    register(name, UserDefinedFunction(func, returnType, Seq.empty))
    ()

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
