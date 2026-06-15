package org.apache.spark.sql

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.*

import org.apache.spark.connect.proto.*
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders
import org.apache.spark.sql.execution.streaming.{ProcessingTimeTrigger, RealTimeTrigger}
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

  test("streaming.Trigger AvailableNow paren call maps correctly") {
    val writer = DataStreamWriter(dummyStreamDf)
    val proto = writer
      .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
      .buildWriteStreamOp().build()
    proto.hasAvailableNow shouldBe true
    proto.getAvailableNow shouldBe true
  }

  test("streaming.Trigger Once paren call maps correctly") {
    val writer = DataStreamWriter(dummyStreamDf)
    val proto = writer
      .trigger(org.apache.spark.sql.streaming.Trigger.Once())
      .buildWriteStreamOp().build()
    proto.hasOnce shouldBe true
    proto.getOnce shouldBe true
  }

  test("trigger Continuous maps correctly") {
    val writer = DataStreamWriter(dummyStreamDf)
    val proto = writer.trigger(Trigger.Continuous(1000)).buildWriteStreamOp().build()
    proto.hasContinuousCheckpointInterval shouldBe true
    proto.getContinuousCheckpointInterval shouldBe "1000 milliseconds"
  }

  test("streaming.Trigger ProcessingTime string maps correctly") {
    val writer = DataStreamWriter(dummyStreamDf)
    val proto = writer
      .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds"))
      .buildWriteStreamOp().build()
    proto.hasProcessingTimeInterval shouldBe true
    proto.getProcessingTimeInterval shouldBe "10000 milliseconds"
  }

  test("streaming.Trigger ProcessingTime TimeUnit maps correctly") {
    val writer = DataStreamWriter(dummyStreamDf)
    val proto = writer
      .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime(
        2,
        java.util.concurrent.TimeUnit.SECONDS
      ))
      .buildWriteStreamOp().build()
    proto.getProcessingTimeInterval shouldBe "2000 milliseconds"
  }

  test("streaming.Trigger ProcessingTime Duration maps correctly") {
    val writer = DataStreamWriter(dummyStreamDf)
    val proto = writer
      .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime(
        scala.concurrent.duration.Duration(3, "seconds")
      ))
      .buildWriteStreamOp().build()
    proto.getProcessingTimeInterval shouldBe "3000 milliseconds"
  }

  test("streaming.Trigger Continuous string maps correctly") {
    val writer = DataStreamWriter(dummyStreamDf)
    val proto = writer
      .trigger(org.apache.spark.sql.streaming.Trigger.Continuous("1 minute"))
      .buildWriteStreamOp().build()
    proto.hasContinuousCheckpointInterval shouldBe true
    proto.getContinuousCheckpointInterval shouldBe "60000 milliseconds"
  }

  test("streaming.Trigger RealTime is exposed but not supported by Connect proto") {
    val writer = DataStreamWriter(dummyStreamDf)
    an[MatchError] shouldBe thrownBy {
      writer.trigger(org.apache.spark.sql.streaming.Trigger.RealTime())
    }
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

  test("streaming.Trigger.ProcessingTime stores string interval and exposes millis") {
    val t = org.apache.spark.sql.streaming.Trigger
      .ProcessingTime("2 seconds")
      .asInstanceOf[ProcessingTimeTrigger]
    t.intervalMs shouldBe 2000
  }

  test("streaming.Trigger parses upstream-style interval strings") {
    org.apache.spark.sql.streaming.Trigger
      .ProcessingTime("interval 1 second")
      .asInstanceOf[ProcessingTimeTrigger].intervalMs shouldBe 1000
    org.apache.spark.sql.streaming.Trigger
      .ProcessingTime("1 second 500 milliseconds")
      .asInstanceOf[ProcessingTimeTrigger].intervalMs shouldBe 1500
    org.apache.spark.sql.streaming.Trigger
      .ProcessingTime("1.5 seconds")
      .asInstanceOf[ProcessingTimeTrigger].intervalMs shouldBe 1500
    org.apache.spark.sql.streaming.Trigger
      .ProcessingTime("1 week")
      .asInstanceOf[ProcessingTimeTrigger].intervalMs shouldBe 604800000
    org.apache.spark.sql.streaming.Trigger
      .ProcessingTime("1000 microseconds")
      .asInstanceOf[ProcessingTimeTrigger].intervalMs shouldBe 1
    org.apache.spark.sql.streaming.Trigger
      .ProcessingTime("interval - 1 second + 2 seconds")
      .asInstanceOf[ProcessingTimeTrigger].intervalMs shouldBe 1000
    org.apache.spark.sql.streaming.Trigger
      .ProcessingTime("1 month -1 month 5 seconds")
      .asInstanceOf[ProcessingTimeTrigger].intervalMs shouldBe 5000
  }

  test("streaming.Trigger validates interval bounds") {
    an[IllegalArgumentException] shouldBe thrownBy {
      org.apache.spark.sql.streaming.Trigger.ProcessingTime(-1)
    }
    an[IllegalArgumentException] shouldBe thrownBy {
      org.apache.spark.sql.streaming.Trigger.Continuous(-1)
    }
    an[IllegalArgumentException] shouldBe thrownBy {
      org.apache.spark.sql.streaming.Trigger.RealTime(0)
    }
    an[IllegalArgumentException] shouldBe thrownBy {
      org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 s")
    }
    an[IllegalArgumentException] shouldBe thrownBy {
      org.apache.spark.sql.streaming.Trigger.ProcessingTime("1.5 minutes")
    }
    an[IllegalArgumentException] shouldBe thrownBy {
      org.apache.spark.sql.streaming.Trigger.ProcessingTime("1e3 seconds")
    }
    an[IllegalArgumentException] shouldBe thrownBy {
      org.apache.spark.sql.streaming.Trigger.ProcessingTime("0.123456789123 seconds")
    }
    an[IllegalArgumentException] shouldBe thrownBy {
      org.apache.spark.sql.streaming.Trigger.ProcessingTime("1 month")
    }
    an[IllegalArgumentException] shouldBe thrownBy {
      org.apache.spark.sql.streaming.Trigger.ProcessingTime(s"${Long.MaxValue} days")
    }
  }

  test("streaming.Trigger RealTime exposes batchDurationMs") {
    val default = org.apache.spark.sql.streaming.Trigger.RealTime().asInstanceOf[RealTimeTrigger]
    val fromString = org.apache.spark.sql.streaming.Trigger
      .RealTime("2 seconds")
      .asInstanceOf[RealTimeTrigger]

    default.batchDurationMs shouldBe 300000
    fromString.batchDurationMs shouldBe 2000
  }

  test("root Trigger facade exposes compatibility helpers") {
    Trigger.processingTime("2 seconds").intervalMs shouldBe 2000
    Trigger.continuous("1 second").intervalMs shouldBe 1000
    Trigger.realTime("3 seconds").batchDurationMs shouldBe 3000
  }

  test("root Trigger facade covers overloads and extractors") {
    Trigger.ProcessingTime(250L).intervalMs shouldBe 250
    Trigger.ProcessingTime(2L, TimeUnit.SECONDS).intervalMs shouldBe 2000
    Trigger.ProcessingTime(3.seconds).intervalMs shouldBe 3000
    Trigger.processingTime(4L, TimeUnit.SECONDS).intervalMs shouldBe 4000
    Trigger.processingTime(5.seconds).intervalMs shouldBe 5000

    Trigger.Continuous(125L).intervalMs shouldBe 125
    Trigger.Continuous(2L, TimeUnit.SECONDS).intervalMs shouldBe 2000
    Trigger.Continuous(3.seconds).intervalMs shouldBe 3000
    Trigger.continuous(4L, TimeUnit.SECONDS).intervalMs shouldBe 4000
    Trigger.continuous(5.seconds).intervalMs shouldBe 5000

    Trigger.RealTime().batchDurationMs shouldBe 300000
    Trigger.RealTime(250L).batchDurationMs shouldBe 250
    Trigger.RealTime(2L, TimeUnit.SECONDS).batchDurationMs shouldBe 2000
    Trigger.RealTime(3.seconds).batchDurationMs shouldBe 3000
    Trigger.realTime().batchDurationMs shouldBe 300000
    Trigger.realTime(4L, TimeUnit.SECONDS).batchDurationMs shouldBe 4000
    Trigger.realTime(5.seconds).batchDurationMs shouldBe 5000

    Trigger.ProcessingTime.unapply(Trigger.ProcessingTime("6 seconds")) shouldBe Some(6000)
    Trigger.Continuous.unapply(Trigger.Continuous("7 seconds")) shouldBe Some(7000)
    Trigger.RealTime.unapply(Trigger.RealTime("8 seconds")) shouldBe Some(8000)
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
  //
  // Must lowercase (Locale.ROOT) at the client to match upstream Spark Connect
  // and the server's `InternalOutputModes.apply`, which strict-matches lower-cased
  // input. R74 regression: typed overload previously sent PascalCase verbatim.
  // ---------------------------------------------------------------------------

  test("outputMode(OutputMode.Append) sets 'append' (lowercased)") {
    val writer = DataStreamWriter(stubStreamDf)
    val proto = writer.outputMode(streaming.OutputMode.Append).buildWriteStreamOp().build()
    proto.getOutputMode shouldBe "append"
  }

  test("outputMode(OutputMode.Update) sets 'update' (lowercased)") {
    val writer = DataStreamWriter(stubStreamDf)
    val proto = writer.outputMode(streaming.OutputMode.Update).buildWriteStreamOp().build()
    proto.getOutputMode shouldBe "update"
  }

  test("outputMode(OutputMode.Complete) sets 'complete' (lowercased)") {
    val writer = DataStreamWriter(stubStreamDf)
    val proto = writer.outputMode(streaming.OutputMode.Complete).buildWriteStreamOp().build()
    proto.getOutputMode shouldBe "complete"
  }

  test("outputMode(String) preserves case (only typed overload lowercases)") {
    val writer = DataStreamWriter(stubStreamDf)
    val proto = writer.outputMode("Append").buildWriteStreamOp().build()
    proto.getOutputMode shouldBe "Append"
  }

  // ---------------------------------------------------------------------------
  // R75: typed encoder propagation (Dataset[T].writeStream.foreach)
  //
  // When called from Dataset[T], the writer's encoder field must be the
  // dataset's encoder, NOT UnboundRowEncoder. Otherwise the server-side
  // foreach handler feeds Row instances into ForeachWriter[T] and crashes
  // with ClassCastException. (We can't round-trip-deserialize the payload
  // because EncoderSerializationProxy.readResolve targets server-side Scala
  // 2.13 classes that aren't present in unit tests.)
  // ---------------------------------------------------------------------------

  test("typed DataStreamWriter[T] retains the supplied encoder") {
    val writer = new DataStreamWriter[Long](stubStreamDf, AgnosticEncoders.PrimitiveLongEncoder)
    writer.encoder shouldBe AgnosticEncoders.PrimitiveLongEncoder
    writer.encoder should not be AgnosticEncoders.UnboundRowEncoder
  }

  test("foreach payload is built (typed writer carries the right encoder)") {
    val writer = new DataStreamWriter[Long](stubStreamDf, AgnosticEncoders.PrimitiveLongEncoder)
    val foreachWriter = new ForeachWriter[Long]:
      def open(partitionId: Long, epochId: Long): Boolean = true
      def process(value: Long): Unit = ()
      def close(errorOrNull: Throwable): Unit = ()

    val proto = writer.foreach(foreachWriter).buildWriteStreamOp().build()
    proto.hasForeachWriter shouldBe true
    proto.getForeachWriter.getScalaFunction.getPayload.size() should be > 0
  }

  test("DataFrame.writeStream defaults to UnboundRowEncoder (backward-compat)") {
    val writer: DataStreamWriter[Row] = stubStreamDf.writeStream
    writer.encoder shouldBe AgnosticEncoders.UnboundRowEncoder
  }

  test("DataStreamWriter(df) companion apply produces Row writer with Row encoder") {
    val writer = DataStreamWriter(stubStreamDf)
    val typed: DataStreamWriter[Row] = writer
    typed.encoder shouldBe AgnosticEncoders.UnboundRowEncoder
  }

  test("Dataset[Long].writeStream propagates the dataset's encoder") {
    val ds: Dataset[Long] = Dataset(stubStreamDf, summon[Encoder[Long]])
    val writer: DataStreamWriter[Long] = ds.writeStream
    writer.encoder shouldBe AgnosticEncoders.PrimitiveLongEncoder
    writer.encoder should not be AgnosticEncoders.UnboundRowEncoder
  }

  test("Dataset[String].writeStream propagates the dataset's encoder") {
    val ds: Dataset[String] = Dataset(stubStreamDf, summon[Encoder[String]])
    val writer: DataStreamWriter[String] = ds.writeStream
    writer.encoder shouldBe AgnosticEncoders.StringEncoder
  }
