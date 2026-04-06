package org.apache.spark.sql

import org.apache.spark.connect.proto.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.*

/** Proto-only tests for DataFrameWriter — no live Spark Connect server needed. */
class DataFrameWriterSuite extends AnyFunSuite with Matchers:

  // ---------------------------------------------------------------------------
  // Stub helpers
  // ---------------------------------------------------------------------------

  private def stubSession: SparkSession = SparkSession(null)

  private def stubDf: DataFrame =
    val session = stubSession
    val rel = Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(session.nextPlanId()).build())
      .setLocalRelation(LocalRelation.getDefaultInstance)
      .build()
    DataFrame(session, rel)

  /** Access private buildWriteOp via reflection so we can inspect the proto. */
  private def buildWriteOp(writer: DataFrameWriter): WriteOperation =
    val method = classOf[DataFrameWriter].getDeclaredMethod("buildWriteOp")
    method.setAccessible(true)
    method.invoke(writer).asInstanceOf[WriteOperation.Builder].build()

  // ---------------------------------------------------------------------------
  // format()
  // ---------------------------------------------------------------------------

  test("default format is parquet") {
    val writer = DataFrameWriter(stubDf)
    val op = buildWriteOp(writer)
    op.getSource shouldBe "parquet"
  }

  test("format sets the source") {
    val writer = DataFrameWriter(stubDf).format("json")
    val op = buildWriteOp(writer)
    op.getSource shouldBe "json"
  }

  test("format can be changed multiple times") {
    val writer = DataFrameWriter(stubDf).format("json").format("csv")
    val op = buildWriteOp(writer)
    op.getSource shouldBe "csv"
  }

  // ---------------------------------------------------------------------------
  // mode()
  // ---------------------------------------------------------------------------

  test("default mode is error") {
    val writer = DataFrameWriter(stubDf)
    val op = buildWriteOp(writer)
    op.getMode shouldBe WriteOperation.SaveMode.SAVE_MODE_ERROR_IF_EXISTS
  }

  test("mode(String) sets overwrite") {
    val writer = DataFrameWriter(stubDf).mode("overwrite")
    val op = buildWriteOp(writer)
    op.getMode shouldBe WriteOperation.SaveMode.SAVE_MODE_OVERWRITE
  }

  test("mode(String) sets append") {
    val writer = DataFrameWriter(stubDf).mode("append")
    val op = buildWriteOp(writer)
    op.getMode shouldBe WriteOperation.SaveMode.SAVE_MODE_APPEND
  }

  test("mode(String) sets ignore") {
    val writer = DataFrameWriter(stubDf).mode("ignore")
    val op = buildWriteOp(writer)
    op.getMode shouldBe WriteOperation.SaveMode.SAVE_MODE_IGNORE
  }

  test("mode(String) sets error for unknown mode") {
    val writer = DataFrameWriter(stubDf).mode("unknown")
    val op = buildWriteOp(writer)
    op.getMode shouldBe WriteOperation.SaveMode.SAVE_MODE_ERROR_IF_EXISTS
  }

  test("mode(String) errorifexists alias") {
    val writer = DataFrameWriter(stubDf).mode("errorifexists")
    val op = buildWriteOp(writer)
    op.getMode shouldBe WriteOperation.SaveMode.SAVE_MODE_ERROR_IF_EXISTS
  }

  test("mode(SaveMode.Overwrite) sets overwrite") {
    val writer = DataFrameWriter(stubDf).mode(SaveMode.Overwrite)
    val op = buildWriteOp(writer)
    op.getMode shouldBe WriteOperation.SaveMode.SAVE_MODE_OVERWRITE
  }

  test("mode(SaveMode.Append) sets append") {
    val writer = DataFrameWriter(stubDf).mode(SaveMode.Append)
    val op = buildWriteOp(writer)
    op.getMode shouldBe WriteOperation.SaveMode.SAVE_MODE_APPEND
  }

  test("mode(SaveMode.Ignore) sets ignore") {
    val writer = DataFrameWriter(stubDf).mode(SaveMode.Ignore)
    val op = buildWriteOp(writer)
    op.getMode shouldBe WriteOperation.SaveMode.SAVE_MODE_IGNORE
  }

  test("mode(SaveMode.ErrorIfExists) sets error") {
    val writer = DataFrameWriter(stubDf).mode(SaveMode.ErrorIfExists)
    val op = buildWriteOp(writer)
    op.getMode shouldBe WriteOperation.SaveMode.SAVE_MODE_ERROR_IF_EXISTS
  }

  // ---------------------------------------------------------------------------
  // option() / options()
  // ---------------------------------------------------------------------------

  test("option(key, String) adds to options map") {
    val writer = DataFrameWriter(stubDf).option("key1", "value1")
    val op = buildWriteOp(writer)
    op.getOptionsMap.asScala shouldBe Map("key1" -> "value1")
  }

  test("option(key, Boolean) converts to string") {
    val writer = DataFrameWriter(stubDf).option("header", true)
    val op = buildWriteOp(writer)
    op.getOptionsMap.get("header") shouldBe "true"
  }

  test("option(key, Long) converts to string") {
    val writer = DataFrameWriter(stubDf).option("maxRecords", 100L)
    val op = buildWriteOp(writer)
    op.getOptionsMap.get("maxRecords") shouldBe "100"
  }

  test("option(key, Double) converts to string") {
    val writer = DataFrameWriter(stubDf).option("threshold", 0.5)
    val op = buildWriteOp(writer)
    op.getOptionsMap.get("threshold") shouldBe "0.5"
  }

  test("options(Map) merges options") {
    val writer = DataFrameWriter(stubDf)
      .option("a", "1")
      .options(Map("b" -> "2", "c" -> "3"))
    val op = buildWriteOp(writer)
    op.getOptionsMap.asScala shouldBe Map("a" -> "1", "b" -> "2", "c" -> "3")
  }

  test("later option overrides earlier") {
    val writer = DataFrameWriter(stubDf)
      .option("key", "old")
      .option("key", "new")
    val op = buildWriteOp(writer)
    op.getOptionsMap.get("key") shouldBe "new"
  }

  // ---------------------------------------------------------------------------
  // partitionBy()
  // ---------------------------------------------------------------------------

  test("partitionBy sets partition columns") {
    val writer = DataFrameWriter(stubDf).partitionBy("year", "month")
    val op = buildWriteOp(writer)
    op.getPartitioningColumnsList.asScala shouldBe Seq("year", "month")
  }

  test("partitionBy with no args sets empty list") {
    val writer = DataFrameWriter(stubDf).partitionBy()
    val op = buildWriteOp(writer)
    op.getPartitioningColumnsList.asScala shouldBe empty
  }

  // ---------------------------------------------------------------------------
  // bucketBy()
  // ---------------------------------------------------------------------------

  test("bucketBy sets bucket info") {
    val writer = DataFrameWriter(stubDf).bucketBy(10, "id")
    val op = buildWriteOp(writer)
    op.hasBucketBy shouldBe true
    op.getBucketBy.getNumBuckets shouldBe 10
    op.getBucketBy.getBucketColumnNamesList.asScala shouldBe Seq("id")
  }

  test("bucketBy with multiple columns") {
    val writer = DataFrameWriter(stubDf).bucketBy(5, "col1", "col2", "col3")
    val op = buildWriteOp(writer)
    op.getBucketBy.getNumBuckets shouldBe 5
    op.getBucketBy.getBucketColumnNamesList.asScala shouldBe Seq("col1", "col2", "col3")
  }

  // ---------------------------------------------------------------------------
  // sortBy()
  // ---------------------------------------------------------------------------

  test("sortBy sets sort column names") {
    val writer = DataFrameWriter(stubDf).sortBy("name", "age")
    val op = buildWriteOp(writer)
    op.getSortColumnNamesList.asScala shouldBe Seq("name", "age")
  }

  // ---------------------------------------------------------------------------
  // Chaining
  // ---------------------------------------------------------------------------

  test("chaining format, mode, option, partitionBy, bucketBy, sortBy") {
    val writer = DataFrameWriter(stubDf)
      .format("orc")
      .mode("append")
      .option("compression", "snappy")
      .partitionBy("year")
      .bucketBy(8, "id")
      .sortBy("name")
    val op = buildWriteOp(writer)
    op.getSource shouldBe "orc"
    op.getMode shouldBe WriteOperation.SaveMode.SAVE_MODE_APPEND
    op.getOptionsMap.get("compression") shouldBe "snappy"
    op.getPartitioningColumnsList.asScala shouldBe Seq("year")
    op.hasBucketBy shouldBe true
    op.getBucketBy.getNumBuckets shouldBe 8
    op.getSortColumnNamesList.asScala shouldBe Seq("name")
  }

  // ---------------------------------------------------------------------------
  // buildWriteOp includes input relation
  // ---------------------------------------------------------------------------

  test("buildWriteOp includes input relation from the DataFrame") {
    val writer = DataFrameWriter(stubDf)
    val op = buildWriteOp(writer)
    op.hasInput shouldBe true
    op.getInput.hasLocalRelation shouldBe true
  }

  // ---------------------------------------------------------------------------
  // toProtoMode edge cases
  // ---------------------------------------------------------------------------

  test("mode is case-insensitive") {
    val writer = DataFrameWriter(stubDf).mode("OVERWRITE")
    val op = buildWriteOp(writer)
    op.getMode shouldBe WriteOperation.SaveMode.SAVE_MODE_OVERWRITE
  }

  test("mode(String) APPEND case-insensitive") {
    val writer = DataFrameWriter(stubDf).mode("Append")
    val op = buildWriteOp(writer)
    op.getMode shouldBe WriteOperation.SaveMode.SAVE_MODE_APPEND
  }

  test("mode(String) IGNORE case-insensitive") {
    val writer = DataFrameWriter(stubDf).mode("IGNORE")
    val op = buildWriteOp(writer)
    op.getMode shouldBe WriteOperation.SaveMode.SAVE_MODE_IGNORE
  }

  test("mode(String) ErrorIfExists case-insensitive") {
    val writer = DataFrameWriter(stubDf).mode("ErrorIfExists")
    val op = buildWriteOp(writer)
    op.getMode shouldBe WriteOperation.SaveMode.SAVE_MODE_ERROR_IF_EXISTS
  }

  // ---------------------------------------------------------------------------
  // Fluent API (returns same instance)
  // ---------------------------------------------------------------------------

  test("format returns the same DataFrameWriter instance") {
    val writer = DataFrameWriter(stubDf)
    val result = writer.format("json")
    result should be theSameInstanceAs writer
  }

  test("mode(String) returns the same DataFrameWriter instance") {
    val writer = DataFrameWriter(stubDf)
    val result = writer.mode("append")
    result should be theSameInstanceAs writer
  }

  test("mode(SaveMode) returns the same DataFrameWriter instance") {
    val writer = DataFrameWriter(stubDf)
    val result = writer.mode(SaveMode.Overwrite)
    result should be theSameInstanceAs writer
  }

  test("option(key, String) returns the same DataFrameWriter instance") {
    val writer = DataFrameWriter(stubDf)
    val result = writer.option("k", "v")
    result should be theSameInstanceAs writer
  }

  test("option(key, Boolean) returns the same DataFrameWriter instance") {
    val writer = DataFrameWriter(stubDf)
    val result = writer.option("k", true)
    result should be theSameInstanceAs writer
  }

  test("option(key, Long) returns the same DataFrameWriter instance") {
    val writer = DataFrameWriter(stubDf)
    val result = writer.option("k", 100L)
    result should be theSameInstanceAs writer
  }

  test("option(key, Double) returns the same DataFrameWriter instance") {
    val writer = DataFrameWriter(stubDf)
    val result = writer.option("k", 1.5)
    result should be theSameInstanceAs writer
  }

  test("options(Map) returns the same DataFrameWriter instance") {
    val writer = DataFrameWriter(stubDf)
    val result = writer.options(Map("k" -> "v"))
    result should be theSameInstanceAs writer
  }

  test("partitionBy returns the same DataFrameWriter instance") {
    val writer = DataFrameWriter(stubDf)
    val result = writer.partitionBy("col1")
    result should be theSameInstanceAs writer
  }

  test("bucketBy returns the same DataFrameWriter instance") {
    val writer = DataFrameWriter(stubDf)
    val result = writer.bucketBy(10, "col1")
    result should be theSameInstanceAs writer
  }

  test("sortBy returns the same DataFrameWriter instance") {
    val writer = DataFrameWriter(stubDf)
    val result = writer.sortBy("col1")
    result should be theSameInstanceAs writer
  }

  // ---------------------------------------------------------------------------
  // sortBy with multiple columns
  // ---------------------------------------------------------------------------

  test("sortBy with multiple columns") {
    val writer = DataFrameWriter(stubDf).sortBy("a", "b", "c")
    val op = buildWriteOp(writer)
    op.getSortColumnNamesList.asScala shouldBe Seq("a", "b", "c")
  }

  // ---------------------------------------------------------------------------
  // bucketBy combined with sortBy
  // ---------------------------------------------------------------------------

  test("bucketBy and sortBy combined") {
    val writer = DataFrameWriter(stubDf)
      .bucketBy(16, "id", "category")
      .sortBy("timestamp")
    val op = buildWriteOp(writer)
    op.hasBucketBy shouldBe true
    op.getBucketBy.getNumBuckets shouldBe 16
    op.getBucketBy.getBucketColumnNamesList.asScala shouldBe Seq("id", "category")
    op.getSortColumnNamesList.asScala shouldBe Seq("timestamp")
  }

  // ---------------------------------------------------------------------------
  // No bucketBy by default
  // ---------------------------------------------------------------------------

  test("no bucketBy by default") {
    val writer = DataFrameWriter(stubDf)
    val op = buildWriteOp(writer)
    op.hasBucketBy shouldBe false
  }

  // ---------------------------------------------------------------------------
  // option with Boolean false
  // ---------------------------------------------------------------------------

  test("option(key, Boolean false) converts to string") {
    val writer = DataFrameWriter(stubDf).option("header", false)
    val op = buildWriteOp(writer)
    op.getOptionsMap.get("header") shouldBe "false"
  }

  // ---------------------------------------------------------------------------
  // Multiple partitionBy overrides
  // ---------------------------------------------------------------------------

  test("partitionBy overrides previous partitionBy") {
    val writer = DataFrameWriter(stubDf)
      .partitionBy("a", "b")
      .partitionBy("c")
    val op = buildWriteOp(writer)
    op.getPartitioningColumnsList.asScala shouldBe Seq("c")
  }
