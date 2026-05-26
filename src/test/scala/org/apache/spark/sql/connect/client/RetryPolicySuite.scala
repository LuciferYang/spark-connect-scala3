package org.apache.spark.sql.connect.client

import io.grpc.{Status, StatusRuntimeException}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RetryPolicySuite extends AnyFunSuite with Matchers:

  test("default policy has expected values") {
    val p = RetryPolicy.defaultPolicy()
    p.maxRetries shouldBe 15
    p.initialBackoffMs shouldBe 50
    p.maxBackoffMs shouldBe 60000
    p.backoffMultiplier shouldBe 4.0
    p.jitterMs shouldBe 500
  }

  test("defaultCanRetry returns true for UNAVAILABLE") {
    val e = new StatusRuntimeException(Status.UNAVAILABLE)
    RetryPolicy.defaultCanRetry(e) shouldBe true
  }

  test("defaultCanRetry returns true for INTERNAL with INVALID_CURSOR.DISCONNECTED") {
    val e = new StatusRuntimeException(
      Status.INTERNAL.withDescription("INVALID_CURSOR.DISCONNECTED: cursor lost")
    )
    RetryPolicy.defaultCanRetry(e) shouldBe true
  }

  test("defaultCanRetry returns false for NOT_FOUND") {
    val e = new StatusRuntimeException(Status.NOT_FOUND)
    RetryPolicy.defaultCanRetry(e) shouldBe false
  }

  test("defaultCanRetry returns false for INTERNAL without INVALID_CURSOR") {
    val e = new StatusRuntimeException(Status.INTERNAL.withDescription("some other error"))
    RetryPolicy.defaultCanRetry(e) shouldBe false
  }

  test("defaultCanRetry returns false for non-StatusRuntimeException") {
    RetryPolicy.defaultCanRetry(RuntimeException("boom")) shouldBe false
  }

  test("GrpcRetryHandler retries on UNAVAILABLE and succeeds") {
    var attempts = 0
    val handler = GrpcRetryHandler(
      RetryPolicy(maxRetries = 3, initialBackoffMs = 1, jitterMs = 0),
      sleep = _ => () // no-op sleep for tests
    )
    val result = handler.retry {
      attempts += 1
      if attempts < 3 then throw StatusRuntimeException(Status.UNAVAILABLE)
      "ok"
    }
    result shouldBe "ok"
    attempts shouldBe 3
  }

  test("GrpcRetryHandler throws after max retries exceeded") {
    val handler = GrpcRetryHandler(
      RetryPolicy(maxRetries = 2, initialBackoffMs = 1, jitterMs = 0),
      sleep = _ => ()
    )
    assertThrows[StatusRuntimeException] {
      handler.retry {
        throw StatusRuntimeException(Status.UNAVAILABLE)
      }
    }
  }

  test("GrpcRetryHandler does not retry non-retryable exceptions") {
    var attempts = 0
    val handler = GrpcRetryHandler(
      RetryPolicy(maxRetries = 5, initialBackoffMs = 1, jitterMs = 0),
      sleep = _ => ()
    )
    assertThrows[StatusRuntimeException] {
      handler.retry {
        attempts += 1
        throw StatusRuntimeException(Status.NOT_FOUND)
      }
    }
    attempts shouldBe 1
  }

  test("GrpcRetryHandler calls sleep with increasing backoff") {
    val sleepTimes = scala.collection.mutable.ArrayBuffer.empty[Long]
    val handler = GrpcRetryHandler(
      RetryPolicy(
        maxRetries = 3,
        initialBackoffMs = 10,
        backoffMultiplier = 2.0,
        maxBackoffMs = 1000,
        jitterMs = 0
      ),
      sleep = t => sleepTimes += t
    )
    var attempts = 0
    handler.retry {
      attempts += 1
      if attempts <= 3 then throw StatusRuntimeException(Status.UNAVAILABLE)
      "done"
    }
    sleepTimes should have size 3
    // backoff: 10, 20, 40
    sleepTimes(0) shouldBe 10
    sleepTimes(1) shouldBe 20
    sleepTimes(2) shouldBe 40
  }

  test("GrpcRetryHandler respects maxBackoffMs") {
    val sleepTimes = scala.collection.mutable.ArrayBuffer.empty[Long]
    val handler = GrpcRetryHandler(
      RetryPolicy(
        maxRetries = 3,
        initialBackoffMs = 100,
        backoffMultiplier = 10.0,
        maxBackoffMs = 500,
        jitterMs = 0
      ),
      sleep = t => sleepTimes += t
    )
    var attempts = 0
    handler.retry {
      attempts += 1
      if attempts <= 3 then throw StatusRuntimeException(Status.UNAVAILABLE)
      "done"
    }
    // backoff: min(100, 500)=100, min(1000, 500)=500, min(10000, 500)=500
    sleepTimes(0) shouldBe 100
    sleepTimes(1) shouldBe 500
    sleepTimes(2) shouldBe 500
  }

  test("GrpcRetryHandler rethrows when maxTotalDurationMs is exhausted") {
    // Drive both clock and sleep counter under test control — no real wall time involved, so
    // the deadline trips deterministically regardless of CI jitter / GC pauses.
    var attempts = 0
    val virtualClock = new java.util.concurrent.atomic.AtomicLong(0L)
    val handler = GrpcRetryHandler(
      RetryPolicy(
        maxRetries = 100, // generous — deadline must trip first
        initialBackoffMs = 1,
        jitterMs = 0,
        maxTotalDurationMs = 30
      ),
      // Each "sleep" advances the virtual clock by 50ms (50ms > 30ms budget → first deadline
      // check after one retry already trips the bound).
      sleep = _ => virtualClock.addAndGet(50L * 1_000_000L),
      nowNanos = () => virtualClock.get()
    )
    val thrown = intercept[StatusRuntimeException] {
      handler.retry {
        attempts += 1
        throw StatusRuntimeException(Status.UNAVAILABLE)
      }
    }
    thrown.getStatus.getCode shouldBe Status.Code.UNAVAILABLE
    // 1st fn call fails → sleep(advances clock by 50ms) → 2nd fn call fails → deadline
    // check sees `now >= deadline` → rethrow. Upper bound guards against accidentally
    // taking the maxRetries path if the deadline is silently ignored.
    attempts shouldBe 2
  }

  test("GrpcRetryHandler restores interrupt flag when sleep throws InterruptedException (R18)") {
    val handler = GrpcRetryHandler(
      RetryPolicy(maxRetries = 5, initialBackoffMs = 1, jitterMs = 0),
      sleep = _ => throw java.lang.InterruptedException("simulated cancellation")
    )
    // Make sure we start with a clean interrupt flag.
    Thread.interrupted()
    val thrown = intercept[InterruptedException] {
      handler.retry {
        throw StatusRuntimeException(Status.UNAVAILABLE)
      }
    }
    thrown.getMessage shouldBe "simulated cancellation"
    // The handler must have re-asserted the interrupt status before rethrowing — read the flag
    // and clear it so test isolation is preserved.
    Thread.interrupted() shouldBe true
  }
