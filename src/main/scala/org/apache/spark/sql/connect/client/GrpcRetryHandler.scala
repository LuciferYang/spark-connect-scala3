package org.apache.spark.sql.connect.client

import java.util.concurrent.ThreadLocalRandom

/** Executes a block with retry logic according to a [[RetryPolicy]].
  *
  * On each retryable failure the handler sleeps for an exponentially increasing duration (with
  * random jitter) before re-attempting. Non-retryable exceptions are propagated immediately.
  *
  * A [[GrpcRetryHandler.RetryException]] is always retryable (no policy match needed) and triggers
  * an immediate retry without backoff.
  */
class GrpcRetryHandler(
    policy: RetryPolicy,
    sleep: Long => Unit = Thread.sleep,
    nowNanos: () => Long = () => System.nanoTime()
):

  /** Execute `fn`, retrying up to `policy.maxRetries` times on retryable errors.
    *
    * Also enforces `policy.maxTotalDurationMs` — if the cumulative time spent retrying exceeds this
    * budget, the last exception is rethrown even if maxRetries has not been exhausted.
    */
  def retry[T](fn: => T): T =
    var lastException: Throwable = null
    var attempt = 0
    val deadline = nowNanos() + policy.maxTotalDurationMs * 1_000_000L
    while attempt <= policy.maxRetries do
      try return fn
      catch
        case re: GrpcRetryHandler.RetryException if attempt < policy.maxRetries =>
          if re.getCause != null then lastException = re.getCause
          attempt += 1 // immediate retry, no backoff
        case e: Throwable if policy.canRetry(e) && attempt < policy.maxRetries =>
          lastException = e
          if nowNanos() >= deadline then throw e // total duration budget exhausted
          val backoff = math.min(
            policy.initialBackoffMs * math.pow(policy.backoffMultiplier, attempt.toDouble).toLong,
            policy.maxBackoffMs
          )
          val jitter =
            if policy.jitterMs > 0 then ThreadLocalRandom.current().nextLong(policy.jitterMs)
            else 0L
          try sleep(backoff + jitter)
          catch
            case ie: InterruptedException =>
              // `Thread.sleep` clears the interrupt flag when it throws. Restore it so
              // cooperative-cancellation callers (gRPC executor, structured concurrency,
              // Future cancel) further up the stack still observe the interrupt.
              Thread.currentThread.interrupt()
              throw ie
          attempt += 1
        case e: Throwable =>
          throw e
    // Should not reach here, but just in case:
    if lastException == null then
      throw RuntimeException("Retry loop exhausted without capturing an exception")
    else throw lastException

object GrpcRetryHandler:
  /** Always-retryable exception that triggers immediate retry without backoff. */
  class RetryException(cause: Throwable = null) extends Throwable(cause)
