package org.apache.spark.sql.connect.client

import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.apache.spark.connect.proto.{
  AnalyzePlanRequest,
  AnalyzePlanResponse,
  SparkConnectServiceGrpc
}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicInteger

/** Verifies that a RetryPolicy set via SparkSession.Builder controls the client's retry count. */
class RetryPolicyBuilderSuite extends AnyFunSuite with Matchers:

  test("Builder.retryPolicy controls how many times a retryable RPC is attempted") {
    val attempts = new AtomicInteger(0)
    val service = new SparkConnectServiceGrpc.SparkConnectServiceImplBase:
      override def analyzePlan(
          request: AnalyzePlanRequest,
          obs: StreamObserver[AnalyzePlanResponse]
      ): Unit =
        attempts.incrementAndGet()
        val err = Status.UNAVAILABLE.withDescription("simulated transient").asRuntimeException()
        obs.onError(err)

    val server = FakeSparkConnectServer(service)
    try
      // maxRetries = 2 → 3 total attempts (the default policy would attempt 16).
      val policy = RetryPolicy(maxRetries = 2, initialBackoffMs = 1, maxBackoffMs = 1, jitterMs = 0)
      val spark = SparkSession.builder().remote(server.url).retryPolicy(policy).build()
      try
        intercept[Exception](spark.range(1).toDF().schema)
        attempts.get() shouldBe 3
      finally spark.stop()
    finally server.stop()
  }
