package org.apache.spark.sql.connect.client

import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import org.apache.spark.connect.proto.{
  ConfigRequest,
  ConfigResponse,
  ExecutePlanRequest,
  ExecutePlanResponse,
  ReleaseExecuteRequest,
  ReleaseExecuteResponse,
  SparkConnectServiceGrpc
}
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{CountDownLatch, TimeUnit}

/** Verifies that closing the execute-result iterator cancels the in-flight gRPC stream. */
class ExecuteStreamCancellationSuite extends AnyFunSuite with Matchers:

  test("closing the execute iterator cancels the in-flight server stream") {
    val started = new CountDownLatch(1)
    val cancelled = new CountDownLatch(1)
    val service = new SparkConnectServiceGrpc.SparkConnectServiceImplBase:
      override def config(request: ConfigRequest, obs: StreamObserver[ConfigResponse]): Unit =
        obs.onNext(ConfigResponse.getDefaultInstance)
        obs.onCompleted()

      override def executePlan(
          request: ExecutePlanRequest,
          obs: StreamObserver[ExecutePlanResponse]
      ): Unit =
        // Register a cancel handler and keep the stream open (never onCompleted), so the only way
        // the call ends is client-side cancellation.
        val sobs = obs.asInstanceOf[ServerCallStreamObserver[ExecutePlanResponse]]
        sobs.setOnCancelHandler(() => cancelled.countDown())
        started.countDown()

      override def releaseExecute(
          request: ReleaseExecuteRequest,
          obs: StreamObserver[ReleaseExecuteResponse]
      ): Unit =
        obs.onNext(ReleaseExecuteResponse.getDefaultInstance)
        obs.onCompleted()

    val server = FakeSparkConnectServer(service)
    try
      val spark = SparkSession.builder().remote(server.url).build()
      try
        val it = spark.range(10).toDF().toLocalIterator()
        assert(started.await(10, TimeUnit.SECONDS), "server never received executePlan")
        it.close()
        assert(
          cancelled.await(10, TimeUnit.SECONDS),
          "close() did not cancel the in-flight execute stream"
        )
      finally spark.stop()
    finally server.stop()
  }
