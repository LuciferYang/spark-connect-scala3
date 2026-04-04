package org.apache.spark.sql

import org.apache.spark.connect.proto.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class StreamingQuerySuite extends AnyFunSuite with Matchers:

  test("StreamingQuery stores id, runId, and name") {
    val spark = SparkSession.builder().remote("sc://localhost:15002").build()
    val sq = StreamingQuery(spark, "test-id", "test-run-id", Some("my_query"))
    sq.id shouldBe "test-id"
    sq.runId shouldBe "test-run-id"
    sq.name shouldBe Some("my_query")
  }

  test("StreamingQuery name can be None") {
    val spark = SparkSession.builder().remote("sc://localhost:15002").build()
    val sq = StreamingQuery(spark, "id", "rid", None)
    sq.name shouldBe None
  }

  test("StreamingQueryCommand proto structure for status") {
    val cmdBuilder = StreamingQueryCommand.newBuilder()
      .setQueryId(StreamingQueryInstanceId.newBuilder()
        .setId("qid").setRunId("rid").build())
      .setStatus(true)
    val cmd = cmdBuilder.build()

    cmd.getQueryId.getId shouldBe "qid"
    cmd.getQueryId.getRunId shouldBe "rid"
    cmd.hasStatus shouldBe true
    cmd.getStatus shouldBe true
  }

  test("StreamingQueryCommand proto structure for stop") {
    val cmd = StreamingQueryCommand.newBuilder()
      .setQueryId(StreamingQueryInstanceId.newBuilder()
        .setId("qid").setRunId("rid").build())
      .setStop(true)
      .build()

    cmd.hasStop shouldBe true
    cmd.getStop shouldBe true
  }

  test("StreamingQueryCommand proto structure for processAllAvailable") {
    val cmd = StreamingQueryCommand.newBuilder()
      .setQueryId(StreamingQueryInstanceId.newBuilder()
        .setId("qid").setRunId("rid").build())
      .setProcessAllAvailable(true)
      .build()

    cmd.hasProcessAllAvailable shouldBe true
    cmd.getProcessAllAvailable shouldBe true
  }

  test("StreamingQueryCommand proto structure for awaitTermination") {
    val cmd = StreamingQueryCommand.newBuilder()
      .setQueryId(StreamingQueryInstanceId.newBuilder()
        .setId("qid").setRunId("rid").build())
      .setAwaitTermination(
        StreamingQueryCommand.AwaitTerminationCommand.newBuilder()
          .setTimeoutMs(5000).build()
      ).build()

    cmd.hasAwaitTermination shouldBe true
    cmd.getAwaitTermination.getTimeoutMs shouldBe 5000
  }

  test("StreamingQueryCommand proto structure for explain") {
    val cmd = StreamingQueryCommand.newBuilder()
      .setQueryId(StreamingQueryInstanceId.newBuilder()
        .setId("qid").setRunId("rid").build())
      .setExplain(
        StreamingQueryCommand.ExplainCommand.newBuilder()
          .setExtended(true).build()
      ).build()

    cmd.hasExplain shouldBe true
    cmd.getExplain.getExtended shouldBe true
  }

  test("StreamingQueryCommand proto structure for exception") {
    val cmd = StreamingQueryCommand.newBuilder()
      .setQueryId(StreamingQueryInstanceId.newBuilder()
        .setId("qid").setRunId("rid").build())
      .setException(true)
      .build()

    cmd.hasException shouldBe true
    cmd.getException shouldBe true
  }

  test("StreamingQueryCommand proto structure for recentProgress") {
    val cmd = StreamingQueryCommand.newBuilder()
      .setQueryId(StreamingQueryInstanceId.newBuilder()
        .setId("qid").setRunId("rid").build())
      .setRecentProgress(true)
      .build()

    cmd.hasRecentProgress shouldBe true
    cmd.getRecentProgress shouldBe true
  }

  test("StreamingQueryCommand proto structure for lastProgress") {
    val cmd = StreamingQueryCommand.newBuilder()
      .setQueryId(StreamingQueryInstanceId.newBuilder()
        .setId("qid").setRunId("rid").build())
      .setLastProgress(true)
      .build()

    cmd.hasLastProgress shouldBe true
    cmd.getLastProgress shouldBe true
  }

  test("StreamingQueryCommandResult StatusResult proto") {
    val status = StreamingQueryCommandResult.StatusResult.newBuilder()
      .setStatusMessage("Processing new data")
      .setIsDataAvailable(true)
      .setIsTriggerActive(true)
      .setIsActive(true)
      .build()

    status.getStatusMessage shouldBe "Processing new data"
    status.getIsDataAvailable shouldBe true
    status.getIsTriggerActive shouldBe true
    status.getIsActive shouldBe true
  }

  test("StreamingQueryCommandResult ExceptionResult proto") {
    val ex = StreamingQueryCommandResult.ExceptionResult.newBuilder()
      .setExceptionMessage("Something went wrong")
      .setErrorClass("STREAM_FAILED")
      .build()

    ex.getExceptionMessage shouldBe "Something went wrong"
    ex.getErrorClass shouldBe "STREAM_FAILED"
    ex.hasExceptionMessage shouldBe true
  }
