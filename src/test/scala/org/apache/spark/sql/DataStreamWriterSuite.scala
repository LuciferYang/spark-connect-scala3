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

  // ---------------------------------------------------------------------------
  // Stub-based tests (no gRPC required)
  // ---------------------------------------------------------------------------

  private def stubSession: SparkSession = SparkSession(null)

  private def stubStreamDf: DataFrame =
    val spark = stubSession
    val rel = Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(spark.nextPlanId()).build())
      .setLocalRelation(LocalRelation.getDefaultInstance)
      .build()
    DataFrame(spark, rel)

  test("format returns the same DataStreamWriter instance (fluent API)") {
    val writer = DataStreamWriter(stubStreamDf)
    val result = writer.format("parquet")
    result should be theSameInstanceAs writer
  }

  test("outputMode returns the same DataStreamWriter instance") {
    val writer = DataStreamWriter(stubStreamDf)
    val result = writer.outputMode("complete")
    result should be theSameInstanceAs writer
  }

  test("trigger returns the same DataStreamWriter instance") {
    val writer = DataStreamWriter(stubStreamDf)
    val result = writer.trigger(Trigger.Once)
    result should be theSameInstanceAs writer
  }

  test("queryName returns the same DataStreamWriter instance") {
    val writer = DataStreamWriter(stubStreamDf)
    val result = writer.queryName("q1")
    result should be theSameInstanceAs writer
  }

  test("option returns the same DataStreamWriter instance") {
    val writer = DataStreamWriter(stubStreamDf)
    val result = writer.option("k", "v")
    result should be theSameInstanceAs writer
  }

  test("options returns the same DataStreamWriter instance") {
    val writer = DataStreamWriter(stubStreamDf)
    val result = writer.options(Map("k" -> "v"))
    result should be theSameInstanceAs writer
  }

  test("partitionBy returns the same DataStreamWriter instance") {
    val writer = DataStreamWriter(stubStreamDf)
    val result = writer.partitionBy("col1")
    result should be theSameInstanceAs writer
  }

  test("buildWriteStreamOp without any settings produces minimal proto") {
    val writer = DataStreamWriter(stubStreamDf)
    val proto = writer.buildWriteStreamOp().build()
    proto.hasInput shouldBe true
    proto.getFormat shouldBe ""
    proto.getOutputMode shouldBe ""
    proto.getQueryName shouldBe ""
    proto.getOptionsMap should be(empty)
  }

  test("multiple options merge correctly") {
    val writer = DataStreamWriter(stubStreamDf)
    val proto = writer
      .option("a", "1")
      .option("b", "2")
      .options(Map("c" -> "3", "d" -> "4"))
      .buildWriteStreamOp().build()
    proto.getOptionsMap.get("a") shouldBe "1"
    proto.getOptionsMap.get("b") shouldBe "2"
    proto.getOptionsMap.get("c") shouldBe "3"
    proto.getOptionsMap.get("d") shouldBe "4"
  }

  test("later option overrides earlier one") {
    val writer = DataStreamWriter(stubStreamDf)
    val proto = writer
      .option("key", "old")
      .option("key", "new")
      .buildWriteStreamOp().build()
    proto.getOptionsMap.get("key") shouldBe "new"
  }

  test("partitionBy with multiple columns") {
    val writer = DataStreamWriter(stubStreamDf)
    val proto = writer.partitionBy("a", "b", "c").buildWriteStreamOp().build()
    import scala.jdk.CollectionConverters.*
    proto.getPartitioningColumnNamesList.asScala shouldBe Seq("a", "b", "c")
  }

  test("partitionBy with no args produces empty list") {
    val writer = DataStreamWriter(stubStreamDf)
    val proto = writer.partitionBy().buildWriteStreamOp().build()
    import scala.jdk.CollectionConverters.*
    proto.getPartitioningColumnNamesList.asScala shouldBe empty
  }

  test("foreachBatch sets the foreach batch payload") {
    val writer = DataStreamWriter(stubStreamDf)
    val result = writer.foreachBatch((df: DataFrame, batchId: Long) => ())
    result should be theSameInstanceAs writer
    // Verify the payload is set by checking the proto
    val proto = result.buildWriteStreamOp().build()
    proto.hasForeachBatch shouldBe true
    proto.getForeachBatch.hasScalaFunction shouldBe true
    proto.getForeachBatch.getScalaFunction.getPayload.size() should be > 0
  }

  test("foreach sets the foreach writer payload") {
    val testWriter = new ForeachWriter[Row]:
      def open(partitionId: Long, epochId: Long): Boolean = true
      def process(value: Row): Unit = ()
      def close(errorOrNull: Throwable): Unit = ()

    val writer = DataStreamWriter(stubStreamDf)
    val result = writer.foreach(testWriter)
    result should be theSameInstanceAs writer
    val proto = result.buildWriteStreamOp().build()
    proto.hasForeachWriter shouldBe true
    proto.getForeachWriter.hasScalaFunction shouldBe true
    proto.getForeachWriter.getScalaFunction.getPayload.size() should be > 0
  }

  // ---------------------------------------------------------------------------
  // Trigger types
  // ---------------------------------------------------------------------------

  test("Trigger.ProcessingTime stores interval") {
    val t = Trigger.ProcessingTime(5000)
    t.intervalMs shouldBe 5000
  }

  test("Trigger.Continuous stores interval") {
    val t = Trigger.Continuous(2000)
    t.intervalMs shouldBe 2000
  }

  test("Trigger.AvailableNow is a singleton") {
    Trigger.AvailableNow should be theSameInstanceAs Trigger.AvailableNow
  }

  test("Trigger.Once is a singleton") {
    Trigger.Once should be theSameInstanceAs Trigger.Once
  }

  // ---------------------------------------------------------------------------
  // P1: Typed option overloads
  // ---------------------------------------------------------------------------

  test("option(key, Boolean) converts to string") {
    val writer = DataStreamWriter(stubStreamDf)
    val proto = writer.option("header", true).buildWriteStreamOp().build()
    proto.getOptionsMap.get("header") shouldBe "true"
  }

  test("option(key, Long) converts to string") {
    val writer = DataStreamWriter(stubStreamDf)
    val proto = writer.option("maxRecords", 100L).buildWriteStreamOp().build()
    proto.getOptionsMap.get("maxRecords") shouldBe "100"
  }

  test("option(key, Double) converts to string") {
    val writer = DataStreamWriter(stubStreamDf)
    val proto = writer.option("threshold", 0.5).buildWriteStreamOp().build()
    proto.getOptionsMap.get("threshold") shouldBe "0.5"
  }

  test("option(key, Boolean false) converts to string") {
    val writer = DataStreamWriter(stubStreamDf)
    val proto = writer.option("header", false).buildWriteStreamOp().build()
    proto.getOptionsMap.get("header") shouldBe "false"
  }

  // ---------------------------------------------------------------------------
  // P1: outputMode(OutputMode) overload
  // ---------------------------------------------------------------------------

  test("outputMode(OutputMode.Append) sets 'Append'") {
    val writer = DataStreamWriter(stubStreamDf)
    val proto = writer.outputMode(streaming.OutputMode.Append).buildWriteStreamOp().build()
    proto.getOutputMode shouldBe "Append"
  }

  test("outputMode(OutputMode.Update) sets 'Update'") {
    val writer = DataStreamWriter(stubStreamDf)
    val proto = writer.outputMode(streaming.OutputMode.Update).buildWriteStreamOp().build()
    proto.getOutputMode shouldBe "Update"
  }

  test("outputMode(OutputMode.Complete) sets 'Complete'") {
    val writer = DataStreamWriter(stubStreamDf)
    val proto = writer.outputMode(streaming.OutputMode.Complete).buildWriteStreamOp().build()
    proto.getOutputMode shouldBe "Complete"
  }
