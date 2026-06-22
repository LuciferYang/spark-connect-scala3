package org.apache.spark.sql.connect.client

import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.apache.spark.connect.proto.{
  ConfigRequest,
  ConfigResponse,
  ExecutePlanRequest,
  ExecutePlanResponse,
  ReattachExecuteRequest,
  ReleaseExecuteRequest,
  ReleaseExecuteResponse,
  SparkConnectServiceGrpc
}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, TimeUnit}

/** Verifies that when a reattach fails with OPERATION_NOT_FOUND, the client falls back to a full
  * re-execute instead of surfacing the raw error.
  */
class ReattachOperationNotFoundSuite extends AnyFunSuite with Matchers:

  test("reattach OPERATION_NOT_FOUND triggers a full re-execute") {
    val executeCount = new AtomicInteger(0)
    val reattached = new CountDownLatch(1)
    val reExecuted = new CountDownLatch(1)

    val service = new SparkConnectServiceGrpc.SparkConnectServiceImplBase:
      override def config(request: ConfigRequest, obs: StreamObserver[ConfigResponse]): Unit =
        obs.onNext(ConfigResponse.getDefaultInstance)
        obs.onCompleted()

      override def releaseExecute(
          request: ReleaseExecuteRequest,
          obs: StreamObserver[ReleaseExecuteResponse]
      ): Unit =
        obs.onNext(ReleaseExecuteResponse.getDefaultInstance)
        obs.onCompleted()

      override def reattachExecute(
          request: ReattachExecuteRequest,
          obs: StreamObserver[ExecutePlanResponse]
      ): Unit =
        reattached.countDown()
        obs.onError(
          Status.INTERNAL.withDescription("INVALID_HANDLE.OPERATION_NOT_FOUND").asRuntimeException()
        )

      override def executePlan(
          request: ExecutePlanRequest,
          obs: StreamObserver[ExecutePlanResponse]
      ): Unit =
        if executeCount.incrementAndGet() == 1 then
          // First execution: end the stream without ResultComplete, forcing a reattach.
          obs.onCompleted()
        else
          // Re-execute after the OPERATION_NOT_FOUND fallback: complete the result.
          reExecuted.countDown()
          obs.onNext(
            ExecutePlanResponse
              .newBuilder()
              .setResultComplete(ExecutePlanResponse.ResultComplete.getDefaultInstance)
              .build()
          )
          obs.onCompleted()

    val server = FakeSparkConnectServer(service)
    try
      val spark = SparkSession.builder().remote(server.url).build()
      try
        val rows = spark.range(5).toDF().collect()
        assert(reattached.await(10, TimeUnit.SECONDS), "client never attempted a reattach")
        assert(
          reExecuted.await(10, TimeUnit.SECONDS),
          "client did not re-execute after OPERATION_NOT_FOUND on reattach"
        )
        rows shouldBe empty
      finally spark.stop()
    finally server.stop()
  }
