package org.apache.spark.sql.connect.client

import io.grpc.stub.StreamObserver
import org.apache.spark.connect.proto.*
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicInteger

/** End-to-end plan-compression behavior against a fake server that advertises ZSTD compression with
  * a low threshold. Exercises `SparkConnectClient.tryCompressPlan` through a real client.
  */
class PlanCompressionClientSuite extends AnyFunSuite with Matchers:

  /** A large, highly compressible plan: a SQL relation whose query is a long repetitive string. */
  private def largePlan(): Plan =
    val rel = Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(0).build())
      .setSql(SQL.newBuilder().setQuery("SELECT '" + ("a" * 5000) + "'").build())
      .build()
    Plan.newBuilder().setRoot(rel).build()

  /** Fake service advertising ZSTD plan compression with a 100-byte threshold. `thresholdRequests`
    * counts how many times the client asks the server for the compression threshold config.
    */
  private def compressionService(thresholdRequests: AtomicInteger) =
    new SparkConnectServiceGrpc.SparkConnectServiceImplBase:
      override def config(request: ConfigRequest, obs: StreamObserver[ConfigResponse]): Unit =
        val builder = ConfigResponse.newBuilder()
        if request.getOperation.hasGet then
          request.getOperation.getGet.getKeysList.forEach { key =>
            if key.contains("threshold") then thresholdRequests.incrementAndGet()
            val value =
              if key.contains("defaultAlgorithm") then "ZSTD"
              else if key.contains("threshold") then "100"
              else ""
            builder.addPairs(KeyValue.newBuilder().setKey(key).setValue(value).build())
          }
        obs.onNext(builder.build())
        obs.onCompleted()

  test("tryCompressPlan compresses a large plan when compression is enabled (default)") {
    val thresholdRequests = new AtomicInteger(0)
    val server = FakeSparkConnectServer(compressionService(thresholdRequests))
    try
      val spark = SparkSession.builder().remote(server.url).build()
      try
        val result = spark.client.tryCompressPlan(largePlan())
        result.getOpTypeCase shouldBe Plan.OpTypeCase.COMPRESSED_OPERATION
        result.getCompressedOperation.getCompressionCodec shouldBe
          CompressionCodec.COMPRESSION_CODEC_ZSTD
      finally spark.stop()
    finally server.stop()
  }
