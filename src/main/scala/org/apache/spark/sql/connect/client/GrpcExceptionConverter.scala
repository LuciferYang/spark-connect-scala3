package org.apache.spark.sql.connect.client

import com.google.rpc.ErrorInfo
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import org.apache.spark.connect.proto.FetchErrorDetailsResponse
import org.apache.spark.sql.SparkException

import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Converts gRPC `StatusRuntimeException` into [[SparkException]].
  *
  * Extracts `ErrorInfo` from the gRPC status details (via `StatusProto`) to populate `errorClass`
  * and `sqlState`. When a `fetchDetails` callback is provided, attempts to call the
  * `FetchErrorDetails` RPC to reconstruct the full exception chain with server-side stack traces.
  */
object GrpcExceptionConverter:

  /** Wrap `fn` so that any `StatusRuntimeException` is converted to a `SparkException`. */
  def convert[T](fn: => T): T =
    try fn
    catch case e: StatusRuntimeException => throw toSparkException(e)

  /** Wrap `fn` with FetchErrorDetails support. */
  def convert[T](fn: => T, fetchDetails: String => Option[FetchErrorDetailsResponse]): T =
    try fn
    catch case e: StatusRuntimeException => throw toSparkException(e, fetchDetails)

  /** Convert a `StatusRuntimeException` into a `SparkException`. */
  def toSparkException(
      e: StatusRuntimeException,
      fetchDetails: String => Option[FetchErrorDetailsResponse] = _ => None
  ): SparkException =
    val errorInfoOpt = extractErrorInfo(e)
    errorInfoOpt match
      case Some(info) =>
        val metadata = info.getMetadataMap.asScala

        // Try FetchErrorDetails if errorId is available
        val enrichedOpt = metadata.get("errorId").flatMap { errorId =>
          try fetchDetails(errorId).flatMap(resp => enrichFromResponse(resp, e))
          catch case NonFatal(_) => None
        }

        enrichedOpt match
          case Some(enriched) => enriched
          case None           =>
            val errorClass = metadata.get("errorClass")
            val sqlState = metadata.get("sqlState")
            val message = metadata.getOrElse(
              "message",
              e.getStatus.getDescription match
                case null => e.getMessage
                case desc => desc
            )
            SparkException(message, cause = e, errorClass = errorClass, sqlState = sqlState)
      case None =>
        val message = e.getStatus.getDescription match
          case null => e.getMessage
          case desc => desc
        SparkException(message, cause = e)

  /** Try to extract the first `ErrorInfo` from the gRPC status details. */
  private def extractErrorInfo(e: StatusRuntimeException): Option[ErrorInfo] =
    try
      val status = StatusProto.fromThrowable(e)
      if status == null then return None
      status.getDetailsList.asScala.collectFirst {
        case any if any.is(classOf[ErrorInfo]) =>
          any.unpack(classOf[ErrorInfo])
      }
    catch case NonFatal(_) => None

  /** Reconstruct a SparkException from a FetchErrorDetailsResponse. */
  private def enrichFromResponse(
      resp: FetchErrorDetailsResponse,
      originalCause: StatusRuntimeException
  ): Option[SparkException] =
    if !resp.hasRootErrorIdx || resp.getErrorsCount == 0 then return None
    try Some(errorsToException(resp, resp.getRootErrorIdx, originalCause))
    catch case NonFatal(_) => None

  /** Recursively build a SparkException from the error list. */
  private def errorsToException(
      resp: FetchErrorDetailsResponse,
      errorIdx: Int,
      originalCause: StatusRuntimeException
  ): SparkException =
    val error = resp.getErrors(errorIdx)
    val message = error.getMessage

    // Recursive cause
    val cause: Throwable =
      if error.hasCauseIdx then errorsToException(resp, error.getCauseIdx, originalCause)
      else originalCause

    // SparkThrowable fields
    val (errorClass, sqlState, msgParams) =
      if error.hasSparkThrowable then
        val st = error.getSparkThrowable
        (
          if st.hasErrorClass then Some(st.getErrorClass) else None,
          if st.hasSqlState then Some(st.getSqlState) else None,
          st.getMessageParametersMap.asScala.toMap
        )
      else (None, None, Map.empty[String, String])

    // Server-side stack trace
    val serverTrace = error.getStackTraceList.asScala.map { ste =>
      java.lang.StackTraceElement(
        ste.getDeclaringClass,
        ste.getMethodName,
        if ste.hasFileName then ste.getFileName else null,
        ste.getLineNumber
      )
    }.toArray

    val ex = SparkException(message, cause, errorClass, sqlState, msgParams)
    ex.setServerStackTrace(serverTrace)
    ex
