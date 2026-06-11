package org.apache.spark.sql.streaming

import java.util.UUID

import scala.jdk.CollectionConverters.*

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.spark.sql.types.{DoubleType, LongType, StringType}

class StreamingQueryProgressSuite extends AnyFunSuite with Matchers:

  // A representative progress JSON shape, mirroring what the Connect server emits.
  private val sampleJson: String =
    """{
      |  "id": "11111111-1111-1111-1111-111111111111",
      |  "runId": "22222222-2222-2222-2222-222222222222",
      |  "name": "myQuery",
      |  "timestamp": "2025-01-01T00:00:00.000Z",
      |  "batchId": 7,
      |  "batchDuration": 1234,
      |  "numInputRows": 0,
      |  "inputRowsPerSecond": 0.0,
      |  "processedRowsPerSecond": 0.0,
      |  "durationMs": { "addBatch": 50, "getBatch": 10 },
      |  "eventTime": { "max": "2025-01-01T00:00:00.500Z" },
      |  "stateOperators": [
      |    {
      |      "operatorName": "stateStoreSave",
      |      "numRowsTotal": 100,
      |      "numRowsUpdated": 5,
      |      "allUpdatesTimeMs": 12,
      |      "numRowsRemoved": 1,
      |      "allRemovalsTimeMs": 3,
      |      "commitTimeMs": 4,
      |      "memoryUsedBytes": 1024,
      |      "numRowsDroppedByWatermark": 0,
      |      "numShufflePartitions": 4,
      |      "numStateStoreInstances": 2,
      |      "customMetrics": { "metricA": 10, "metricB": 20 }
      |    }
      |  ],
      |  "sources": [
      |    {
      |      "description": "RateStreamV2[rowsPerSecond=10]",
      |      "startOffset": "0",
      |      "endOffset": "10",
      |      "latestOffset": "10",
      |      "numInputRows": 100,
      |      "inputRowsPerSecond": 10.0,
      |      "processedRowsPerSecond": 8.0,
      |      "metrics": { "lag": "5" }
      |    }
      |  ],
      |  "sink": {
      |    "description": "ConsoleSink",
      |    "numOutputRows": 100,
      |    "metrics": { "writeMs": "5" }
      |  },
      |  "observedMetrics": {
      |    "event1": {
      |      "values": [1, 3.0],
      |      "schema": {
      |        "type": "struct",
      |        "fields": [
      |          { "name": "c1", "type": "long", "nullable": true, "metadata": {} },
      |          { "name": "c2", "type": "double", "nullable": true, "metadata": {} }
      |        ]
      |      }
      |    },
      |    "event2": {
      |      "values": [1, "hello", "world"],
      |      "schema": {
      |        "type": "struct",
      |        "fields": [
      |          { "name": "rc", "type": "long", "nullable": true, "metadata": {} },
      |          { "name": "min_q", "type": "string", "nullable": true, "metadata": {} },
      |          { "name": "max_q", "type": "string", "nullable": true, "metadata": {} }
      |        ]
      |      }
      |    }
      |  }
      |}""".stripMargin

  // ---------------------------------------------------------------------------
  // fromJson — full happy path
  // ---------------------------------------------------------------------------

  test("fromJson parses top-level fields") {
    val p = StreamingQueryProgress.fromJson(sampleJson)
    p.id shouldBe UUID.fromString("11111111-1111-1111-1111-111111111111")
    p.runId shouldBe UUID.fromString("22222222-2222-2222-2222-222222222222")
    p.name shouldBe "myQuery"
    p.timestamp shouldBe "2025-01-01T00:00:00.000Z"
    p.batchId shouldBe 7L
    p.batchDuration shouldBe 1234L
  }

  test("fromJson aggregates source-derived metrics") {
    val p = StreamingQueryProgress.fromJson(sampleJson)
    p.numInputRows shouldBe 100L
    p.inputRowsPerSecond shouldBe 10.0
    p.processedRowsPerSecond shouldBe 8.0
  }

  test("fromJson parses durationMs and eventTime maps") {
    val p = StreamingQueryProgress.fromJson(sampleJson)
    p.durationMs.asScala.toMap.view.mapValues(_.toLong).toMap shouldBe
      Map("addBatch" -> 50L, "getBatch" -> 10L)
    p.eventTime.asScala.toMap shouldBe Map("max" -> "2025-01-01T00:00:00.500Z")
  }

  test("fromJson parses stateOperators with custom metrics") {
    val p = StreamingQueryProgress.fromJson(sampleJson)
    p.stateOperators should have length 1
    val op = p.stateOperators(0)
    op.operatorName shouldBe "stateStoreSave"
    op.numRowsTotal shouldBe 100L
    op.numRowsUpdated shouldBe 5L
    op.numShufflePartitions shouldBe 4L
    op.numStateStoreInstances shouldBe 2L
    op.customMetrics.asScala.toMap.view.mapValues(_.toLong).toMap shouldBe
      Map("metricA" -> 10L, "metricB" -> 20L)
  }

  test("fromJson parses sources with rate metrics") {
    val p = StreamingQueryProgress.fromJson(sampleJson)
    p.sources should have length 1
    val src = p.sources(0)
    src.description shouldBe "RateStreamV2[rowsPerSecond=10]"
    src.numInputRows shouldBe 100L
    src.inputRowsPerSecond shouldBe 10.0
    src.processedRowsPerSecond shouldBe 8.0
    src.metrics.get("lag") shouldBe "5"
  }

  test("fromJson parses sink") {
    val p = StreamingQueryProgress.fromJson(sampleJson)
    p.sink should not be null
    p.sink.description shouldBe "ConsoleSink"
    p.sink.numOutputRows shouldBe 100L
    p.sink.metrics.get("writeMs") shouldBe "5"
  }

  test("fromJson parses observedMetrics as schema-aware rows") {
    val p = StreamingQueryProgress.fromJson(sampleJson)
    p.observedMetrics.keySet().asScala.toSet shouldBe Set("event1", "event2")

    val event1 = p.observedMetrics.get("event1")
    event1.getAs[Long]("c1") shouldBe 1L
    event1.getAs[Double]("c2") shouldBe 3.0d
    event1.schema.get.fields.map(_.dataType) shouldBe Seq(LongType, DoubleType)

    val event2 = p.observedMetrics.get("event2")
    event2.getAs[String]("min_q") shouldBe "hello"
    event2.getAs[String]("max_q") shouldBe "world"
    event2.schema.get.fields.map(_.dataType) shouldBe Seq(LongType, StringType, StringType)
  }

  test("fromJson parses observedMetrics field-object shape") {
    val json =
      """{"observedMetrics":{"event":{"c1":2,"c2":4.5,
        |"schema":{"type":"struct","fields":[
        |{"name":"c1","type":"long","nullable":true,"metadata":{}},
        |{"name":"c2","type":"double","nullable":true,"metadata":{}}
        |]}}}}""".stripMargin
    val row = StreamingQueryProgress.fromJson(json).observedMetrics.get("event")
    row.getAs[Long]("c1") shouldBe 2L
    row.getAs[Double]("c2") shouldBe 4.5d
  }

  test("fromJson parses observedMetrics primitive value types") {
    val json =
      """{"observedMetrics":{"event":{
        |"values":[true,1,2,3,4.5,"AQI=",12.34,"2026-01-02","2026-01-02T03:04:05Z",
        |"2026-01-02T03:04:05"],
        |"schema":{"type":"struct","fields":[
        |{"name":"bool","type":"boolean","nullable":true,"metadata":{}},
        |{"name":"byte","type":"byte","nullable":true,"metadata":{}},
        |{"name":"short","type":"short","nullable":true,"metadata":{}},
        |{"name":"int","type":"int","nullable":true,"metadata":{}},
        |{"name":"float","type":"float","nullable":true,"metadata":{}},
        |{"name":"binary","type":"binary","nullable":true,"metadata":{}},
        |{"name":"decimal","type":"decimal(10,2)","nullable":true,"metadata":{}},
        |{"name":"date","type":"date","nullable":true,"metadata":{}},
        |{"name":"timestamp","type":"timestamp","nullable":true,"metadata":{}},
        |{"name":"timestampNtz","type":"timestamp_ntz","nullable":true,"metadata":{}}
        |]}}}}""".stripMargin
    val row = StreamingQueryProgress.fromJson(json).observedMetrics.get("event")
    row.getAs[Boolean]("bool") shouldBe true
    row.getAs[Byte]("byte") shouldBe 1.toByte
    row.getAs[Short]("short") shouldBe 2.toShort
    row.getAs[Int]("int") shouldBe 3
    row.getAs[Float]("float") shouldBe 4.5f
    row.getAs[Array[Byte]]("binary").toSeq shouldBe Seq(1.toByte, 2.toByte)
    row.getAs[java.math.BigDecimal]("decimal") shouldBe new java.math.BigDecimal("12.34")
    row.getAs[java.sql.Date]("date") shouldBe java.sql.Date.valueOf("2026-01-02")
    row.getAs[java.sql.Timestamp]("timestamp") shouldBe
      java.sql.Timestamp.from(java.time.Instant.parse("2026-01-02T03:04:05Z"))
    row.getAs[java.time.LocalDateTime]("timestampNtz") shouldBe
      java.time.LocalDateTime.parse("2026-01-02T03:04:05")
  }

  // ---------------------------------------------------------------------------
  // fromJson — null/missing-field tolerance
  // ---------------------------------------------------------------------------

  test("fromJson tolerates missing optional fields") {
    val minimal = """{"id":"00000000-0000-0000-0000-000000000000",
                    |"runId":"00000000-0000-0000-0000-000000000000",
                    |"timestamp":"2025-01-01T00:00:00.000Z",
                    |"batchId":0,"batchDuration":0}""".stripMargin
    val p = StreamingQueryProgress.fromJson(minimal)
    p.name shouldBe null
    p.stateOperators shouldBe empty
    p.sources shouldBe empty
    p.sink shouldBe null
    p.durationMs shouldBe empty
    p.eventTime shouldBe empty
    p.observedMetrics shouldBe empty
  }

  test("fromJson treats null id/runId as zero UUID") {
    val nullIds = """{"id":null,"runId":null,"timestamp":"t","batchId":0,"batchDuration":0}"""
    val p = StreamingQueryProgress.fromJson(nullIds)
    p.id shouldBe new UUID(0L, 0L)
    p.runId shouldBe new UUID(0L, 0L)
  }

  test("fromJson preserves complex offsets verbatim") {
    val complex = """{"id":"00000000-0000-0000-0000-000000000001",
                    |"runId":"00000000-0000-0000-0000-000000000002",
                    |"timestamp":"t","batchId":1,"batchDuration":1,
                    |"sources":[{"description":"k","startOffset":{"a":1},"endOffset":{"a":2},
                    |"numInputRows":0,"inputRowsPerSecond":0.0,"processedRowsPerSecond":0.0}]}"""
      .stripMargin
    val p = StreamingQueryProgress.fromJson(complex)
    val src = p.sources(0)
    new ObjectMapper().readTree(src.startOffset).get("a").asInt shouldBe 1
    new ObjectMapper().readTree(src.endOffset).get("a").asInt shouldBe 2
  }

  // ---------------------------------------------------------------------------
  // json emission round-trip
  // ---------------------------------------------------------------------------

  test("json emits a parseable JSON string with the same id/name") {
    val p = StreamingQueryProgress.fromJson(sampleJson)
    val parsed = new ObjectMapper().readTree(p.json)
    parsed.get("id").asText shouldBe "11111111-1111-1111-1111-111111111111"
    parsed.get("name").asText shouldBe "myQuery"
    parsed.get("batchId").asLong shouldBe 7L
    parsed.get("stateOperators").size() shouldBe 1
    parsed.get("sources").size() shouldBe 1
    parsed.get("observedMetrics").get("event1").get("c1").asLong shouldBe 1L
  }

  test("prettyJson is parseable and indented") {
    val p = StreamingQueryProgress.fromJson(sampleJson)
    val pj = p.prettyJson
    pj should include("\n")
    new ObjectMapper().readTree(pj).get("name").asText shouldBe "myQuery"
  }

  test("toString equals prettyJson") {
    val p = StreamingQueryProgress.fromJson(sampleJson)
    p.toString shouldBe p.prettyJson
  }

  // ---------------------------------------------------------------------------
  // SourceProgress / SinkProgress / StateOperatorProgress — direct JSON output
  // ---------------------------------------------------------------------------

  test("StateOperatorProgress.json emits all known fields") {
    val op = new StateOperatorProgress(
      operatorName = "op",
      numRowsTotal = 1L,
      numRowsUpdated = 2L,
      allUpdatesTimeMs = 3L,
      numRowsRemoved = 4L,
      allRemovalsTimeMs = 5L,
      commitTimeMs = 6L,
      memoryUsedBytes = 7L,
      numRowsDroppedByWatermark = 8L,
      numShufflePartitions = 9L,
      numStateStoreInstances = 10L
    )
    val parsed = new ObjectMapper().readTree(op.json)
    parsed.get("operatorName").asText shouldBe "op"
    parsed.get("numRowsUpdated").asLong shouldBe 2L
    parsed.get("numStateStoreInstances").asLong shouldBe 10L
    parsed.has("customMetrics") shouldBe false
    op.prettyJson should include("\n")
    op.toString shouldBe op.prettyJson
  }

  test("SinkProgress.json includes description and rows") {
    val sp = new SinkProgress("MySink", 42L)
    val parsed = new ObjectMapper().readTree(sp.json)
    parsed.get("description").asText shouldBe "MySink"
    parsed.get("numOutputRows").asLong shouldBe 42L
    sp.prettyJson should include("\n")
    sp.toString shouldBe sp.prettyJson
  }

  test("SinkProgress single-arg constructor uses sentinel -1") {
    val sp = new SinkProgress("X")
    sp.numOutputRows shouldBe SinkProgress.DEFAULT_NUM_OUTPUT_ROWS
    sp.numOutputRows shouldBe -1L
  }

  test("SinkProgress.apply factory accepts Some/None numOutputRows") {
    val s1 = SinkProgress("A", Some(5L))
    s1.numOutputRows shouldBe 5L
    val s2 = SinkProgress("B", None)
    s2.numOutputRows shouldBe SinkProgress.DEFAULT_NUM_OUTPUT_ROWS
  }

  test("SourceProgress.json round-trips simple fields") {
    val src = new SourceProgress(
      description = "S",
      startOffset = "0",
      endOffset = "10",
      latestOffset = "10",
      numInputRows = 1L,
      inputRowsPerSecond = 0.5,
      processedRowsPerSecond = 0.25
    )
    val parsed = new ObjectMapper().readTree(src.json)
    parsed.get("description").asText shouldBe "S"
    parsed.get("numInputRows").asLong shouldBe 1L
    parsed.get("inputRowsPerSecond").asDouble shouldBe 0.5
    src.prettyJson should include("\n")
    src.toString shouldBe src.prettyJson
  }

  test("non-finite rate metrics are omitted from emitted JSON") {
    val src = new SourceProgress(
      description = "S",
      startOffset = null,
      endOffset = null,
      latestOffset = null,
      numInputRows = 0L,
      inputRowsPerSecond = Double.NaN,
      processedRowsPerSecond = Double.PositiveInfinity
    )
    val parsed = new ObjectMapper().readTree(src.json)
    parsed.has("inputRowsPerSecond") shouldBe false
    parsed.has("processedRowsPerSecond") shouldBe false
  }
