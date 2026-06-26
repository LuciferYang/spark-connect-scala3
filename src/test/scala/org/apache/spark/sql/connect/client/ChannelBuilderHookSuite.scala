package org.apache.spark.sql.connect.client

import org.apache.spark.connect.proto.SparkConnectServiceGrpc
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/** Verifies the `SparkSession.Builder.channelBuilder` customization hook. */
class ChannelBuilderHookSuite extends AnyFunSuite with Matchers:

  private def emptyService = new SparkConnectServiceGrpc.SparkConnectServiceImplBase() {}

  test("Builder.channelBuilder customizer runs during channel creation") {
    val applied = new AtomicBoolean(false)
    val server = FakeSparkConnectServer(emptyService)
    try
      val spark = SparkSession.builder()
        .remote(server.url)
        .channelBuilder { cb =>
          applied.set(true)
          // A real, harmless channel option — proves the live builder is handed to the callback.
          cb.keepAliveTime(30, TimeUnit.SECONDS)
        }
        .build()
      try applied.get() shouldBe true
      finally spark.stop()
    finally server.stop()
  }

  test("multiple channelBuilder customizers compose in registration order") {
    // Customizers run sequentially on the building thread during create(), so a plain buffer is safe.
    val order = scala.collection.mutable.ArrayBuffer.empty[Int]
    val server = FakeSparkConnectServer(emptyService)
    try
      val spark = SparkSession.builder()
        .remote(server.url)
        .channelBuilder(_ => order += 1)
        .channelBuilder(_ => order += 2)
        .build()
      try order.toSeq shouldBe Seq(1, 2)
      finally spark.stop()
    finally server.stop()
  }
