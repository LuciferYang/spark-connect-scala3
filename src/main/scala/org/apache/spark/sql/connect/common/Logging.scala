package org.apache.spark.sql.connect.common

/** Minimal Logging stub for the vendored ClosureCleaner.
  *
  * Upstream's ClosureCleaner extends `org.apache.spark.internal.Logging`, which provides
  * `logTrace`/`logDebug`/`logInfo`/`logWarning`/`logError` and a `log` value with
  * `isDebugEnabled` / `isTraceEnabled`. SC3 doesn't depend on Spark, so we provide a no-op
  * shim with the same surface. This keeps the vendored ClosureCleaner source unchanged.
  *
  * Diagnostic output is gated on the `spark.connect.scala3.closureCleaner.debug` system
  * property — set to `true` to print to stderr. Default is silent so production usage has
  * zero overhead.
  */
trait Logging:
  protected val log: Log = Log.instance
  protected def logTrace(msg: => String): Unit = if log.isTraceEnabled then Log.print("TRACE", msg)
  protected def logTrace(msg: => String, throwable: Throwable): Unit =
    if log.isTraceEnabled then Log.print("TRACE", s"$msg ${throwable.getClass.getName}: ${throwable.getMessage}")
  protected def logDebug(msg: => String): Unit = if log.isDebugEnabled then Log.print("DEBUG", msg)
  protected def logDebug(msg: => String, throwable: Throwable): Unit =
    if log.isDebugEnabled then Log.print("DEBUG", s"$msg ${throwable.getClass.getName}: ${throwable.getMessage}")
  protected def logInfo(msg: => String): Unit = Log.print("INFO ", msg)
  protected def logWarning(msg: => String): Unit = Log.print("WARN ", msg)
  protected def logError(msg: => String): Unit = Log.print("ERROR", msg)

final class Log:
  private val debug = java.lang.Boolean.getBoolean("spark.connect.scala3.closureCleaner.debug")
  def isDebugEnabled: Boolean = debug
  def isTraceEnabled: Boolean = debug

object Log:
  val instance: Log = Log()
  private[common] def print(level: String, msg: String): Unit =
    System.err.println(s"[$level] [ClosureCleaner] $msg")
