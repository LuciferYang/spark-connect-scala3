package org.apache.spark.sql

/** Lightweight Spark exception for the Scala 3 Connect client.
  *
  * Carries optional `errorClass`, `sqlState`, and `messageParameters` extracted from gRPC error
  * metadata or the FetchErrorDetails RPC, mirroring the upstream SparkException contract without
  * pulling in the full SparkThrowable hierarchy.
  */
class SparkException(
    message: String,
    cause: Throwable = null,
    val errorClass: Option[String] = None,
    val sqlState: Option[String] = None,
    val messageParameters: Map[String, String] = Map.empty
) extends Exception(message, cause):

  /** Prepend server-side stack trace elements to this exception's stack trace. */
  private[sql] def setServerStackTrace(trace: Array[StackTraceElement]): Unit =
    if trace.nonEmpty then setStackTrace(trace ++ getStackTrace)
