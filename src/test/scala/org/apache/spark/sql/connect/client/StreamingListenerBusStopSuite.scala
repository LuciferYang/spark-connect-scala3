package org.apache.spark.sql.connect.client

import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import org.apache.spark.connect.proto.{
  ConfigRequest,
  ConfigResponse,
  ExecutePlanRequest,
  ExecutePlanResponse,
  ReleaseExecuteRequest,
  ReleaseExecuteResponse,
  SparkConnectServiceGrpc,
  StreamingQueryListenerEventsResult
}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.jdk.CollectionConverters.*

/** Verifies that removing the last streaming listener stops the background handler thread by
  * cancelling the long-running event stream (rather than relying solely on Thread.interrupt()).
  */
class StreamingListenerBusStopSuite extends AnyFunSuite with Matchers:

  private val handlerThreadName = "StreamingQueryListenerBus-handler"

  private def handlerThreadAlive(): Boolean =
    Thread.getAllStackTraces.keySet.asScala
      .exists(t => t.getName == handlerThreadName && t.isAlive)

  private def awaitUntil(timeoutMs: Long)(cond: => Boolean): Boolean =
    val deadline = System.currentTimeMillis() + timeoutMs
    while !cond && System.currentTimeMillis() < deadline do Thread.sleep(50)
    cond

  test("removeListener cancels the event stream and stops the handler thread") {
    val eventStreamCancelled = new CountDownLatch(1)
    val service = new SparkConnectServiceGrpc.SparkConnectServiceImplBase:
      override def config(request: ConfigRequest, obs: StreamObserver[ConfigResponse]): Unit =
        obs.onNext(ConfigResponse.getDefaultInstance)
        obs.onCompleted()

      override def executePlan(
          request: ExecutePlanRequest,
          obs: StreamObserver[ExecutePlanResponse]
      ): Unit =
        val cmd = request.getPlan.getCommand.getStreamingQueryListenerBusCommand
        if cmd.getAddListenerBusListener then
          // Long-running event stream: confirm registration, then keep it open until cancelled.
          val sobs = obs.asInstanceOf[ServerCallStreamObserver[ExecutePlanResponse]]
          sobs.setOnCancelHandler(() => eventStreamCancelled.countDown())
          val added = StreamingQueryListenerEventsResult
            .newBuilder()
            .setListenerBusListenerAdded(true)
            .build()
          val confirmation = ExecutePlanResponse
            .newBuilder()
            .setStreamingQueryListenerEventsResult(added)
            .build()
          sobs.onNext(confirmation)
        else
          // RemoveListenerBusListener (or any other) command: complete immediately.
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
      try
        val listener = new StreamingQueryListener:
          def onQueryStarted(e: StreamingQueryListener.QueryStartedEvent): Unit = ()
          def onQueryProgress(e: StreamingQueryListener.QueryProgressEvent): Unit = ()
          def onQueryTerminated(e: StreamingQueryListener.QueryTerminatedEvent): Unit = ()

        spark.streams.addListener(listener)
        assert(
          awaitUntil(5000)(handlerThreadAlive()),
          "handler thread should start after addListener"
        )

        spark.streams.removeListener(listener)
        assert(
          eventStreamCancelled.await(10, TimeUnit.SECONDS),
          "removeListener did not cancel the event stream"
        )
        assert(
          awaitUntil(10000)(!handlerThreadAlive()),
          "handler thread did not terminate after removeListener"
        )
      finally spark.stop()
    finally server.stop()
  }
