package org.apache.spark.sql.connect.client

import io.grpc.stub.StreamObserver
import org.apache.spark.connect.proto.{
  ConfigRequest,
  ConfigResponse,
  ExecutePlanRequest,
  ExecutePlanResponse,
  ReleaseExecuteRequest,
  ReleaseExecuteResponse,
  SparkConnectServiceGrpc
}
import org.apache.spark.sql.{SparkRuntimeException, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Verifies that a streaming-manager command with a missing result surfaces a typed
  * SparkRuntimeException (not a bare RuntimeException).
  */
class StreamingManagerErrorSuite extends AnyFunSuite with Matchers:

  test("manager command with no result throws SparkRuntimeException") {
    val service = new SparkConnectServiceGrpc.SparkConnectServiceImplBase:
      override def config(request: ConfigRequest, obs: StreamObserver[ConfigResponse]): Unit =
        obs.onNext(ConfigResponse.getDefaultInstance)
        obs.onCompleted()

      override def executePlan(
          request: ExecutePlanRequest,
          obs: StreamObserver[ExecutePlanResponse]
      ): Unit =
        // Complete the stream without a StreamingQueryManagerCommandResult.
        obs.onCompleted()

      override def releaseExecute(
          request: ReleaseExecuteRequest,
          obs: StreamObserver[ReleaseExecuteResponse]
      ): Unit =
        obs.onNext(ReleaseExecuteResponse.getDefaultInstance)
        obs.onCompleted()

    val server = FakeSparkConnectServer(service)
    try
      val spark = SparkSession.builder().remote(server.url).build()
      try a[SparkRuntimeException] should be thrownBy spark.streams.active
      finally spark.stop()
    finally server.stop()
  }
