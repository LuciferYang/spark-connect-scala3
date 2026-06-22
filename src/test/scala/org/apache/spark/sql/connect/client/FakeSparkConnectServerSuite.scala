package org.apache.spark.sql.connect.client

import io.grpc.stub.StreamObserver
import org.apache.spark.connect.proto.{
  AnalyzePlanRequest,
  AnalyzePlanResponse,
  SparkConnectServiceGrpc
}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Smoke test proving the production client can talk to [[FakeSparkConnectServer]]. */
class FakeSparkConnectServerSuite extends AnyFunSuite with Matchers:

  test("client resolves schema from an in-process fake server") {
    // A distinctive schema (not range's real "id: long") proves the response came from the fake.
    val expected = StructType(Seq(StructField("smoke", StringType)))
    val service = new SparkConnectServiceGrpc.SparkConnectServiceImplBase:
      override def analyzePlan(
          request: AnalyzePlanRequest,
          obs: StreamObserver[AnalyzePlanResponse]
      ): Unit =
        obs.onNext(
          AnalyzePlanResponse
            .newBuilder()
            .setSchema(
              AnalyzePlanResponse.Schema
                .newBuilder()
                .setSchema(DataTypeProtoConverter.toProto(expected))
                .build()
            )
            .build()
        )
        obs.onCompleted()

    val server = FakeSparkConnectServer(service)
    try
      val spark = SparkSession.builder().remote(server.url).build()
      try spark.range(10).toDF().schema shouldBe expected
      finally spark.stop()
    finally server.stop()
  }
