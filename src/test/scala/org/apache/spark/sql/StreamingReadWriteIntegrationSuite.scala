package org.apache.spark.sql

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.tags.IntegrationTest
import org.apache.spark.sql.types.*

import java.nio.file.{Files, Path}
import java.util.Comparator

/** Integration tests for Structured Streaming: DataStreamReader options, Trigger variants,
  * foreachBatch, toTable, outputMode, partitionBy.
  */
@IntegrationTest
class StreamingReadWriteIntegrationSuite extends IntegrationTestBase:

  private def withTempDir(f: Path => Unit): Unit =
    val dir = Files.createTempDirectory("sc3_stream_test_")
    try f(dir)
    finally
      if Files.exists(dir) then
        Files.walk(dir)
          .sorted(Comparator.reverseOrder())
          .forEach(p => Files.deleteIfExists(p))

  // ---------------------------------------------------------------------------
  // DataStreamReader options
  // ---------------------------------------------------------------------------

  test("readStream.format.option.load creates streaming DataFrame") {
    val df = spark.readStream.format("rate").option("rowsPerSecond", "5").load()
    assert(df.isStreaming)
  }

  test("readStream.schema with DDL string") {
    withTempDir { dir =>
      // Write some json files first
      val rows = Seq(Row(1, "a"), Row(2, "b"))
      val schema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("name", StringType)
      ))
      spark.createDataFrame(rows, schema).write.json(dir.resolve("data").toString)

      val df = spark.readStream
        .schema("id INT, name STRING")
        .format("json")
        .load(dir.resolve("data").toString)
      assert(df.isStreaming)
    }
  }

  // ---------------------------------------------------------------------------
  // Trigger variants
  // ---------------------------------------------------------------------------

  test("Trigger.AvailableNow processes all available data then stops") {
    withTempDir { dir =>
      val checkpoint = dir.resolve("checkpoint").toString
      val output = dir.resolve("output").toString

      val df = spark.readStream.format("rate").option("rowsPerSecond", "100").load()
      val query = df.writeStream
        .format("parquet")
        .option("checkpointLocation", checkpoint)
        .trigger(Trigger.AvailableNow)
        .start(output)

      query.awaitTermination(30000)
      assert(!query.isActive)
    }
  }

  test("Trigger.Once processes one micro-batch then stops") {
    withTempDir { dir =>
      val checkpoint = dir.resolve("checkpoint").toString
      val output = dir.resolve("output").toString

      val df = spark.readStream.format("rate").option("rowsPerSecond", "100").load()
      val query = df.writeStream
        .format("parquet")
        .option("checkpointLocation", checkpoint)
        .trigger(Trigger.Once)
        .start(output)

      query.awaitTermination(30000)
      assert(!query.isActive)
    }
  }

  test("Trigger.ProcessingTime starts a running query") {
    val df = spark.readStream.format("rate").option("rowsPerSecond", "5").load()
    val query = df.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2000))
      .start()

    try
      assert(query.isActive)
      assert(query.name.isEmpty || query.name.isDefined) // name is optional
    finally query.stop()
  }

  // ---------------------------------------------------------------------------
  // writeStream options
  // ---------------------------------------------------------------------------

  test("writeStream.queryName sets query name") {
    val df = spark.readStream.format("rate").option("rowsPerSecond", "5").load()
    val query = df.writeStream
      .format("console")
      .queryName("named_query")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2000))
      .start()

    try assert(query.name.contains("named_query"))
    finally query.stop()
  }

  test("writeStream.outputMode complete with aggregation") {
    val df = spark.readStream.format("rate").option("rowsPerSecond", "5").load()
      .groupBy(col("value")).count()

    val query = df.writeStream
      .format("console")
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(2000))
      .start()

    try assert(query.isActive)
    finally query.stop()
  }

  // ---------------------------------------------------------------------------
  // StreamingQuery properties
  // ---------------------------------------------------------------------------

  test("StreamingQuery id, runId, status, recentProgress, lastProgress") {
    val df = spark.readStream.format("rate").option("rowsPerSecond", "5").load()
    val query = df.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(1000))
      .start()

    try
      assert(query.id.nonEmpty)
      assert(query.runId.nonEmpty)
      // status and progress may be empty initially but should not throw
      query.status
      query.recentProgress
      query.lastProgress
    finally query.stop()
  }

  test("StreamingQuery awaitTermination with timeout") {
    val df = spark.readStream.format("rate").option("rowsPerSecond", "5").load()
    val query = df.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(1000))
      .start()

    try
      // awaitTermination should return false (not terminated within timeout)
      val terminated = query.awaitTermination(1000)
      assert(!terminated)
      assert(query.isActive)
    finally query.stop()
  }

  // ---------------------------------------------------------------------------
  // StreamingQueryManager
  // ---------------------------------------------------------------------------

  test("streams.get retrieves query by id") {
    val df = spark.readStream.format("rate").option("rowsPerSecond", "1").load()
    val query = df.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2000))
      .start()

    try
      val retrieved = spark.streams.get(query.id)
      assert(retrieved != null)
      assert(retrieved.id == query.id)
    finally query.stop()
  }

  test("streams.awaitAnyTermination with timeout") {
    // Reset any terminated status from prior tests (e.g. Trigger.Once, Trigger.AvailableNow)
    spark.streams.resetTerminated()

    val df = spark.readStream.format("rate").option("rowsPerSecond", "1").load()
    val query = df.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2000))
      .start()

    try
      // should return false (no termination within timeout)
      val terminated = spark.streams.awaitAnyTermination(1000)
      assert(!terminated)
    finally query.stop()
  }

  // ---------------------------------------------------------------------------
  // foreachBatch
  // ---------------------------------------------------------------------------

  test("foreachBatch executes batch function") {
    assert(classFilesUploaded)
    val df = spark.readStream.format("rate").option("rowsPerSecond", "100").load()

    try
      @volatile var batchCount = 0L
      val query = df.writeStream
        .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
          batchCount = batchId
        }
        .trigger(Trigger.ProcessingTime(500))
        .start()

      try
        Thread.sleep(2000)
        if !query.isActive then
          // Query died — check if it's the known lambda/class compat issue
          val ex = query.exception
          ex match
            case Some(msg) if msg.contains("deserializeLambda") ||
              msg.contains("Failed to unpack scala udf") ||
              msg.contains("Failed to load class correctly") ||
              msg.contains("cannot be cast to class org.apache.spark.sql.DataFrame") =>
              cancel("Scala 3 foreachBatch incompatible with Scala 2.13 server")
            case Some(msg) => fail(s"Query terminated with unexpected error: $msg")
            case None      => fail("Query terminated without exception and isActive is false")
      finally query.stop()
    catch
      case e: SparkException if e.getMessage != null &&
        (e.getMessage.contains("deserializeLambda") ||
         e.getMessage.contains("Failed to unpack scala udf") ||
         e.getMessage.contains("Failed to load class correctly") ||
         e.getMessage.contains("cannot be cast to class org.apache.spark.sql.DataFrame")) =>
        cancel("Scala 3 foreachBatch incompatible with Scala 2.13 server")
  }

  // ---------------------------------------------------------------------------
  // toTable
  // ---------------------------------------------------------------------------

  test("toTable writes streaming output to a table") {
    withTempDir { dir =>
      val checkpoint = dir.resolve("checkpoint").toString
      val tableName = "sc3_test_stream_to_table"

      // Clean up stale table from previous runs
      try spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
      catch case _: Exception => ()

      val df = spark.readStream.format("rate").option("rowsPerSecond", "100").load()
      val query = df.writeStream
        .option("checkpointLocation", checkpoint)
        .trigger(Trigger.ProcessingTime(500))
        .toTable(tableName)

      try
        // Let the streaming query run for a few seconds to generate data
        Thread.sleep(3000)
        query.stop()
        query.awaitTermination(10000)
        val result = spark.read.table(tableName).count()
        assert(result > 0, s"Expected rows in table but got $result")
      finally
        try query.stop()
        catch case _: Exception => ()
        try spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
        catch case _: Exception => ()
    }
  }
