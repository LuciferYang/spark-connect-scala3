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
    // Guard against non-positive budget (would make deadline fire immediately) and
    // overflow (2^63 ns ≈ 292 years). Use Math.addExact so overflow surfaces as
    // ArithmeticException rather than producing a silent negative deadline.
    val budgetNs =
      if policy.maxTotalDurationMs <= 0 then Long.MaxValue
      else
        try Math.multiplyExact(policy.maxTotalDurationMs, 1_000_000L)
        catch case _: ArithmeticException => Long.MaxValue
    val deadline =
      try Math.addExact(nowNanos(), budgetNs)
      catch case _: ArithmeticException => Long.MaxValue
    while attempt <= policy.maxRetries do
      try return fn
      catch
        case re: GrpcRetryHandler.RetryException if attempt < policy.maxRetries =>
          // Always record a usable last exception so the "exhausted" fallback has something to
          // rethrow. Prefer the wrapped cause when present; fall back to re itself.
          lastException = if re.getCause != null then re.getCause else re
          // RetryException also respects the deadline so repeated immediate retries cannot
          // spin inside the budget indefinitely — matches the canRetry branch below.
          if nowNanos() >= deadline then throw lastException
          attempt += 1 // immediate retry, no backoff
        case e: Throwable if policy.canRetry(e) && attempt < policy.maxRetries =>
          lastException = e
          if nowNanos() >= deadline then throw e // total duration budget exhausted
          // Guard backoff arithmetic against overflow: pow(...).toLong can overflow Long.MaxValue
          // when backoffMultiplier is large and attempt is high; clamp to maxBackoffMs before
          // multiplying to prevent the resulting negative value from being passed to Thread.sleep.
          val rawBackoff =
            val scaled = math.pow(policy.backoffMultiplier, attempt.toDouble)
            if scaled.isInfinite ||
              scaled >= (Long.MaxValue / math.max(1L, policy.initialBackoffMs)).toDouble
            then policy.maxBackoffMs
            else math.min(policy.initialBackoffMs * scaled.toLong, policy.maxBackoffMs)
          val backoff = math.max(0L, rawBackoff)
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
