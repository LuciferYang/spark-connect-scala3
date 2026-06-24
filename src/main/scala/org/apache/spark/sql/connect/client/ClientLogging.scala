package org.apache.spark.sql.connect.client

/** Pluggable sink for the client's own diagnostic warnings (retry/release failures, the streaming
  * listener handler, etc.).
  *
  * Defaults to `System.err`. Applications can route these messages into their own logging system
  * (SLF4J, Log4j, …) or silence them:
  *
  * {{{
  *   ClientLogging.setHandler(line => myLogger.warn(line))
  * }}}
  *
  * The handler is `@volatile`, so updates are visible across threads.
  */
object ClientLogging:

  private val DefaultHandler: String => Unit = msg => System.err.println(msg)

  @volatile private var handler: String => Unit = DefaultHandler

  /** Route warning lines to `h` instead of `System.err`. */
  def setHandler(h: String => Unit): Unit = handler = h

  /** Restore the default `System.err` sink. */
  def resetHandler(): Unit = handler = DefaultHandler

  /** Emit a warning formatted as `[WARN] [component] message`. */
  def warn(component: String, message: String): Unit =
    handler(s"[WARN] [$component] $message")
