package org.apache.spark.sql.connect.client

import com.google.protobuf.GeneratedMessage
import io.grpc.StatusRuntimeException

/** Validates server-side session ID consistency across all responses.
  *
  * The first response that carries a `server_side_session_id` sets the expected value. Subsequent
  * responses must match that ID; otherwise an `IllegalStateException` is thrown.
  */
class ResponseValidator:

  @volatile private var serverSideSessionId: Option[String] = None
  @volatile private var sessionValid: Boolean = true

  /** Verify a single (unary) response. Extracts `server_side_session_id` via protobuf reflection.
    */
  def verifyResponse[RespT <: GeneratedMessage](fn: => RespT): RespT =
    val resp =
      try fn
      catch
        case e: StatusRuntimeException if isSessionChanged(e) =>
          sessionValid = false
          throw e
    extractServerSideSessionId(resp).foreach(trackSessionId)
    resp

  /** Wrap a streaming iterator so that every `next()` call is validated. */
  def wrapIterator[RespT <: GeneratedMessage](
      iter: Iterator[RespT] & AutoCloseable
  ): Iterator[RespT] & AutoCloseable =
    new Iterator[RespT] with AutoCloseable:
      def hasNext: Boolean = iter.hasNext
      def next(): RespT =
        val resp = iter.next()
        extractServerSideSessionId(resp).foreach(trackSessionId)
        resp
      def close(): Unit = iter.close()

  def getServerSideSessionId: Option[String] = serverSideSessionId

  def isSessionValid: Boolean = sessionValid

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  private def trackSessionId(id: String): Unit =
    serverSideSessionId match
      case None =>
        serverSideSessionId = Some(id)
      case Some(existing) if existing != id =>
        sessionValid = false
        throw IllegalStateException(
          s"Server-side session ID changed from $existing to $id"
        )
      case _ => // matches — ok

  private def extractServerSideSessionId(msg: GeneratedMessage): Option[String] =
    val fd = msg.getDescriptorForType.findFieldByName("server_side_session_id")
    if fd != null then
      val value = msg.getField(fd)
      value match
        case s: String if s.nonEmpty => Some(s)
        case _                       => None
    else None

  private def isSessionChanged(e: StatusRuntimeException): Boolean =
    Option(e.getStatus.getDescription)
      .exists(_.contains("INVALID_HANDLE.SESSION_CHANGED"))
