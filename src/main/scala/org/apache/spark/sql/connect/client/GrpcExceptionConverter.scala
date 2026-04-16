package org.apache.spark.sql.connect.client

import com.google.rpc.ErrorInfo
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import org.apache.spark.connect.proto.FetchErrorDetailsResponse
import org.apache.spark.sql.SparkException
import org.apache.spark.sql.*

import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Converts gRPC `StatusRuntimeException` into [[SparkException]].
  *
  * Extracts `ErrorInfo` from the gRPC status details (via `StatusProto`) to populate `errorClass`
  * and `sqlState`. When a `fetchDetails` callback is provided, attempts to call the
  * `FetchErrorDetails` RPC to reconstruct the full exception chain with server-side stack traces.
  */
object GrpcExceptionConverter:

  /** Factory constructors keyed by upstream fully-qualified class names. */
  private type ExFactory =
    (String, Throwable, Option[String], Option[String], Map[String, String]) => SparkException

  private val errorFactory: Map[String, ExFactory] = Map(
    "org.apache.spark.sql.AnalysisException" ->
      ((m, c, ec, ss, mp) => AnalysisException(m, c, ec, ss, mp)),
    "org.apache.spark.sql.catalyst.parser.ParseException" ->
      ((m, c, ec, ss, mp) => ParseException(m, c, ec, ss, mp)),
    "org.apache.spark.sql.streaming.StreamingQueryException" ->
      ((m, c, ec, ss, mp) => StreamingQueryException(m, c, ec, ss, mp)),
    "org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException" ->
      ((m, c, ec, ss, mp) => NamespaceAlreadyExistsException(m, c, ec, ss, mp)),
    "org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException" ->
      ((m, c, ec, ss, mp) => TableAlreadyExistsException(m, c, ec, ss, mp)),
    "org.apache.spark.sql.catalyst.analysis.TempTableAlreadyExistsException" ->
      ((m, c, ec, ss, mp) => TempTableAlreadyExistsException(m, c, ec, ss, mp)),
    "org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException" ->
      ((m, c, ec, ss, mp) => NoSuchDatabaseException(m, c, ec, ss, mp)),
    "org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException" ->
      ((m, c, ec, ss, mp) => NoSuchNamespaceException(m, c, ec, ss, mp)),
    "org.apache.spark.sql.catalyst.analysis.NoSuchTableException" ->
      ((m, c, ec, ss, mp) => NoSuchTableException(m, c, ec, ss, mp)),
    "java.lang.NumberFormatException" ->
      ((m, c, ec, ss, mp) => SparkNumberFormatException(m, c, ec, ss, mp)),
    "java.lang.IllegalArgumentException" ->
      ((m, c, ec, ss, mp) => SparkIllegalArgumentException(m, c, ec, ss, mp)),
    "java.lang.ArithmeticException" ->
      ((m, c, ec, ss, mp) => SparkArithmeticException(m, c, ec, ss, mp)),
    "java.lang.UnsupportedOperationException" ->
      ((m, c, ec, ss, mp) => SparkUnsupportedOperationException(m, c, ec, ss, mp)),
    "java.lang.ArrayIndexOutOfBoundsException" ->
      ((m, c, ec, ss, mp) => SparkArrayIndexOutOfBoundsException(m, c, ec, ss, mp)),
    "java.time.DateTimeException" ->
      ((m, c, ec, ss, mp) => SparkDateTimeException(m, c, ec, ss, mp)),
    "org.apache.spark.SparkRuntimeException" ->
      ((m, c, ec, ss, mp) => SparkRuntimeException(m, c, ec, ss, mp)),
    "org.apache.spark.SparkUpgradeException" ->
      ((m, c, ec, ss, mp) => SparkUpgradeException(m, c, ec, ss, mp)),
    "org.apache.spark.SparkException" ->
      ((m, c, ec, ss, mp) => SparkException(m, c, ec, ss, mp))
  )

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

  /** Recursively build a SparkException from the error list, using error_type_hierarchy to select
    * the most specific exception type.
    */
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

    // Walk error_type_hierarchy (most-specific first), find first match in errorFactory
    val hierarchy = error.getErrorTypeHierarchyList.asScala
    val factoryOpt = hierarchy.collectFirst {
      case cls if errorFactory.contains(cls) => errorFactory(cls)
    }

    val ex = factoryOpt match
      case Some(factory) =>
        factory(message, cause, errorClass, sqlState, msgParams)
      case None if hierarchy.nonEmpty =>
        // No factory found — prepend class name to message for diagnostics
        val className = hierarchy.headOption.getOrElse("")
        val prefixed =
          if className.nonEmpty && !message.startsWith(s"[$className]")
          then s"[$className] $message"
          else message
        SparkException(prefixed, cause, errorClass, sqlState, msgParams)
      case None =>
        SparkException(message, cause, errorClass, sqlState, msgParams)

    ex.setServerStackTrace(serverTrace)
    ex
