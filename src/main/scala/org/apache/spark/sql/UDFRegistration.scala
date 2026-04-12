package org.apache.spark.sql

import org.apache.spark.connect.proto.Command
import org.apache.spark.sql.connect.client.SparkConnectClient

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
