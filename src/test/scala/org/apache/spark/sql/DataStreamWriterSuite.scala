package org.apache.spark.sql

import org.apache.spark.connect.proto.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataStreamWriterSuite extends AnyFunSuite with Matchers:

  private def session: SparkSession =
    SparkSession.builder().remote("sc://localhost:15002").build()

  /** Create a dummy streaming DataFrame for testing proto construction. */
  private def dummyStreamDf: DataFrame =
    val spark = session
    spark.readStream.format("rate").load()

  test("buildWriteStreamOp sets format") {
    val writer = DataStreamWriter(dummyStreamDf)
    val proto = writer.format("console").buildWriteStreamOp().build()
    proto.getFormat shouldBe "console"
  }

  test("buildWriteStreamOp sets outputMode") {
    val writer = DataStreamWriter(dummyStreamDf)
    val proto = writer.outputMode("append").buildWriteStreamOp().build()
    proto.getOutputMode shouldBe "append"
  }

  test("buildWriteStreamOp sets queryName") {
    val writer = DataStreamWriter(dummyStreamDf)
    val proto = writer.queryName("my_query").buildWriteStreamOp().build()
    proto.getQueryName shouldBe "my_query"
  }

  test("buildWriteStreamOp sets options") {
    val writer = DataStreamWriter(dummyStreamDf)
    val proto = writer
      .option("checkpointLocation", "/tmp/cp")
      .options(Map("key" -> "value"))
      .buildWriteStreamOp().build()
    proto.getOptionsMap.get("checkpointLocation") shouldBe "/tmp/cp"
    proto.getOptionsMap.get("key") shouldBe "value"
  }

  test("buildWriteStreamOp sets partitioning columns") {
    val writer = DataStreamWriter(dummyStreamDf)
    val proto = writer.partitionBy("col1", "col2").buildWriteStreamOp().build()
    import scala.jdk.CollectionConverters.*
    proto.getPartitioningColumnNamesList.asScala shouldBe Seq("col1", "col2")
  }

  test("trigger ProcessingTime maps correctly") {
    val writer = DataStreamWriter(dummyStreamDf)
    val proto = writer.trigger(Trigger.ProcessingTime(5000)).buildWriteStreamOp().build()
    proto.hasProcessingTimeInterval shouldBe true
    proto.getProcessingTimeInterval shouldBe "5000 milliseconds"
  }

  test("trigger AvailableNow maps correctly") {
    val writer = DataStreamWriter(dummyStreamDf)
    val proto = writer.trigger(Trigger.AvailableNow).buildWriteStreamOp().build()
    proto.hasAvailableNow shouldBe true
    proto.getAvailableNow shouldBe true
  }

  test("trigger Once maps correctly") {
    val writer = DataStreamWriter(dummyStreamDf)
    val proto = writer.trigger(Trigger.Once).buildWriteStreamOp().build()
    proto.hasOnce shouldBe true
    proto.getOnce shouldBe true
  }

  test("trigger Continuous maps correctly") {
    val writer = DataStreamWriter(dummyStreamDf)
    val proto = writer.trigger(Trigger.Continuous(1000)).buildWriteStreamOp().build()
    proto.hasContinuousCheckpointInterval shouldBe true
    proto.getContinuousCheckpointInterval shouldBe "1000 milliseconds"
  }

  test("buildWriteStreamOp sets input relation") {
    val df = dummyStreamDf
    val writer = DataStreamWriter(df)
    val proto = writer.buildWriteStreamOp().build()
    proto.hasInput shouldBe true
    proto.getInput shouldBe df.relation
  }

  test("full builder chain") {
    val writer = DataStreamWriter(dummyStreamDf)
    val proto = writer
      .format("kafka")
      .outputMode("append")
      .queryName("kafka_sink")
      .trigger(Trigger.ProcessingTime(2000))
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "output")
      .partitionBy("key")
      .buildWriteStreamOp().build()

    proto.getFormat shouldBe "kafka"
    proto.getOutputMode shouldBe "append"
    proto.getQueryName shouldBe "kafka_sink"
    proto.getProcessingTimeInterval shouldBe "2000 milliseconds"
    proto.getOptionsMap.get("kafka.bootstrap.servers") shouldBe "localhost:9092"
    proto.getOptionsMap.get("topic") shouldBe "output"

    import scala.jdk.CollectionConverters.*
    proto.getPartitioningColumnNamesList.asScala shouldBe Seq("key")
  }
