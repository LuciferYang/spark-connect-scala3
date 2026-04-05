package org.apache.spark.sql.connect.client

import io.grpc.{Status, StatusRuntimeException}

/** Configurable retry policy with exponential backoff and jitter.
  *
  * Modeled after the upstream Spark Connect `RetryPolicy` but simplified: no server-side
  * `RetryInfo` parsing (which requires `grpc-protobuf-services` / `com.google.rpc.RetryInfo`).
  */
case class RetryPolicy(
    maxRetries: Int = 15,
    initialBackoffMs: Long = 50,
    maxBackoffMs: Long = 60000,
    backoffMultiplier: Double = 4.0,
    jitterMs: Long = 500,
    canRetry: Throwable => Boolean = RetryPolicy.defaultCanRetry
)

object RetryPolicy:

  /** Default predicate: retry on UNAVAILABLE and on INTERNAL with INVALID_CURSOR.DISCONNECTED. */
  def defaultCanRetry(e: Throwable): Boolean =
    e match
      case e: StatusRuntimeException =>
        e.getStatus.getCode == Status.Code.UNAVAILABLE ||
        (e.getStatus.getCode == Status.Code.INTERNAL &&
          Option(e.getMessage).exists(_.contains("INVALID_CURSOR.DISCONNECTED")))
      case _ => false

  def defaultPolicy(): RetryPolicy = RetryPolicy()
