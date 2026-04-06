package org.apache.spark.sql

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.tags.IntegrationTest

/** Integration tests for DataFrameWriterV2 and MergeIntoWriter. */
@IntegrationTest
class WriterIntegrationSuite extends IntegrationTestBase:

  test("writeTo returns DataFrameWriterV2 and fluent API works") {
    val df = spark.range(5)
    val writer = df.writeTo("any_table")
      .using("parquet")
      .option("key", "value")
      .tableProperty("prop", "val")
    assert(writer != null)
  }

  test("mergeInto returns MergeIntoWriter with full fluent chain") {
    val source = spark.range(5).select(col("id"), lit("name").as("name"))
    val writer = source.mergeInto("some_table", col("id") === col("id"))
      .whenMatched().updateAll()
      .whenNotMatched().insertAll()
      .whenNotMatchedBySource().delete()
      .withSchemaEvolution()
    assert(writer != null)
  }
