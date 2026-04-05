package org.apache.spark.sql

import org.apache.spark.connect.proto.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.*

class StreamingQueryManagerSuite extends AnyFunSuite with Matchers:

  test("StreamingQueryManagerCommand proto for active") {
    val cmd = StreamingQueryManagerCommand.newBuilder()
      .setActive(true)
      .build()

    cmd.hasActive shouldBe true
    cmd.getActive shouldBe true
  }

  test("StreamingQueryManagerCommand proto for getQuery") {
    val cmd = StreamingQueryManagerCommand.newBuilder()
      .setGetQuery("query-uuid-123")
      .build()

    cmd.hasGetQuery shouldBe true
    cmd.getGetQuery shouldBe "query-uuid-123"
  }

  test("StreamingQueryManagerCommand proto for awaitAnyTermination without timeout") {
    val cmd = StreamingQueryManagerCommand.newBuilder()
      .setAwaitAnyTermination(
        StreamingQueryManagerCommand.AwaitAnyTerminationCommand.getDefaultInstance
      ).build()

    cmd.hasAwaitAnyTermination shouldBe true
    cmd.getAwaitAnyTermination.hasTimeoutMs shouldBe false
  }

  test("StreamingQueryManagerCommand proto for awaitAnyTermination with timeout") {
    val cmd = StreamingQueryManagerCommand.newBuilder()
      .setAwaitAnyTermination(
        StreamingQueryManagerCommand.AwaitAnyTerminationCommand.newBuilder()
          .setTimeoutMs(10000).build()
      ).build()

    cmd.hasAwaitAnyTermination shouldBe true
    cmd.getAwaitAnyTermination.getTimeoutMs shouldBe 10000
  }

  test("StreamingQueryManagerCommand proto for resetTerminated") {
    val cmd = StreamingQueryManagerCommand.newBuilder()
      .setResetTerminated(true)
      .build()

    cmd.hasResetTerminated shouldBe true
    cmd.getResetTerminated shouldBe true
  }

  test("StreamingQueryManagerCommandResult ActiveResult proto") {
    val qi1 = StreamingQueryManagerCommandResult.StreamingQueryInstance.newBuilder()
      .setId(StreamingQueryInstanceId.newBuilder().setId("id1").setRunId("run1").build())
      .setName("query1")
      .build()
    val qi2 = StreamingQueryManagerCommandResult.StreamingQueryInstance.newBuilder()
      .setId(StreamingQueryInstanceId.newBuilder().setId("id2").setRunId("run2").build())
      .build()

    val result = StreamingQueryManagerCommandResult.ActiveResult.newBuilder()
      .addActiveQueries(qi1)
      .addActiveQueries(qi2)
      .build()

    result.getActiveQueriesList should have size 2
    result.getActiveQueries(0).getId.getId shouldBe "id1"
    result.getActiveQueries(0).getName shouldBe "query1"
    result.getActiveQueries(0).hasName shouldBe true
    result.getActiveQueries(1).getId.getId shouldBe "id2"
    result.getActiveQueries(1).hasName shouldBe false
  }

  test("StreamingQueryManagerCommandResult AwaitAnyTerminationResult proto") {
    val result = StreamingQueryManagerCommandResult.AwaitAnyTerminationResult.newBuilder()
      .setTerminated(true)
      .build()

    result.getTerminated shouldBe true
  }

  test("WriteStreamOperationStart full proto construction") {
    val spark = SparkSession.builder().remote("sc://localhost:15002").build()
    val df = spark.readStream.format("rate").load()

    val builder = WriteStreamOperationStart.newBuilder()
      .setInput(df.relation)
      .setFormat("console")
      .setOutputMode("append")
      .setQueryName("test_query")
      .setProcessingTimeInterval("5000 milliseconds")
      .putOptions("checkpointLocation", "/tmp/cp")
      .addPartitioningColumnNames("key")

    val proto = builder.build()
    proto.getFormat shouldBe "console"
    proto.getOutputMode shouldBe "append"
    proto.getQueryName shouldBe "test_query"
    proto.getProcessingTimeInterval shouldBe "5000 milliseconds"
    proto.getOptionsMap.get("checkpointLocation") shouldBe "/tmp/cp"
    proto.getPartitioningColumnNamesList.asScala shouldBe Seq("key")
    proto.hasInput shouldBe true
  }

  test("WriteStreamOperationStartResult proto") {
    val result = WriteStreamOperationStartResult.newBuilder()
      .setQueryId(StreamingQueryInstanceId.newBuilder()
        .setId("qid-1").setRunId("rid-1").build())
      .setName("my_stream")
      .build()

    result.getQueryId.getId shouldBe "qid-1"
    result.getQueryId.getRunId shouldBe "rid-1"
    result.getName shouldBe "my_stream"
  }

  test("StreamingQueryManager provides listener methods") {
    val spark = SparkSession.builder().remote("sc://localhost:15002").build()
    val manager = spark.streams

    // listListeners should return empty array initially
    manager.streamingQueryListenerBus.list() shouldBe empty
  }

  test("SparkSession.streams returns same instance (lazy val)") {
    val spark = SparkSession.builder().remote("sc://localhost:15002").build()
    val s1 = spark.streams
    val s2 = spark.streams
    s1 should be theSameInstanceAs s2
  }
