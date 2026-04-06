package org.apache.spark.sql

import java.io.ObjectStreamException

/** Lightweight Spark exception for the Scala 3 Connect client.
  *
  * Carries optional `errorClass`, `sqlState`, and `messageParameters` extracted from gRPC error
  * metadata or the FetchErrorDetails RPC, mirroring the upstream SparkException contract without
  * pulling in the full SparkThrowable hierarchy.
  *
  * Implements `Serializable` so that forked test JVMs can send exceptions back to the parent
  * process. Uses `writeReplace` to strip non-serializable causes (e.g. gRPC StatusRuntimeException).
  */
class SparkException(
    message: String,
    cause: Throwable = null,
    val errorClass: Option[String] = None,
    val sqlState: Option[String] = None,
    val messageParameters: Map[String, String] = Map.empty
) extends Exception(message, cause)
    with Serializable:

  /** Prepend server-side stack trace elements to this exception's stack trace. */
  private[sql] def setServerStackTrace(trace: Array[StackTraceElement]): Unit =
    if trace.nonEmpty then setStackTrace(trace ++ getStackTrace)

  /** Replace this exception with a serializable proxy that drops the non-serializable cause. */
  @throws[ObjectStreamException]
  private def writeReplace(): AnyRef =
    val proxy = SparkException(message, null, errorClass, sqlState, messageParameters)
    proxy.setStackTrace(getStackTrace)
    proxy
