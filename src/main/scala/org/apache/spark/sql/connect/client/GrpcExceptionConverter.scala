package org.apache.spark.sql.connect.client

import com.google.rpc.ErrorInfo
import io.grpc.StatusRuntimeException
import io.grpc.protobuf.StatusProto
import org.apache.spark.sql.SparkException

import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Converts gRPC `StatusRuntimeException` into [[SparkException]].
  *
  * Extracts `ErrorInfo` from the gRPC status details (via `StatusProto`) to populate `errorClass`
  * and `sqlState`. If no `ErrorInfo` is present, wraps the exception with the status description.
  *
  * This is a simplified version of the upstream `GrpcExceptionConverter` — it does '''not''' call
  * the `FetchErrorDetails` RPC and does not reconstruct the full exception type hierarchy.
  */
object GrpcExceptionConverter:

  private val SparkErrorInfoReason = "org.apache.spark.SparkException"

  /** Wrap `fn` so that any `StatusRuntimeException` is converted to a `SparkException`. */
  def convert[T](fn: => T): T =
    try fn
    catch case e: StatusRuntimeException => throw toSparkException(e)

  /** Convert a `StatusRuntimeException` into a `SparkException`. */
  def toSparkException(e: StatusRuntimeException): SparkException =
    val errorInfoOpt = extractErrorInfo(e)
    errorInfoOpt match
      case Some(info) =>
        val metadata = info.getMetadataMap.asScala
        val errorClass = metadata.get("errorClass")
        val sqlState = metadata.get("sqlState")
        val message = metadata.getOrElse("message", e.getStatus.getDescription match
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
