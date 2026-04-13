package org.apache.spark.sql

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.tags.IntegrationTest
import org.apache.spark.sql.types.*

/** Integration tests for functions string overloads (P0 API gap).
  *
  * These tests require a live Spark Connect server.
  */
@IntegrationTest
class FunctionsIntegrationSuite extends IntegrationTestBase:

  private def sampleDf: DataFrame =
    val rows = Seq(Row("Alice", 30), Row("Bob", 25), Row("Charlie", 35))
    val schema = StructType(Seq(StructField("name", StringType), StructField("age", IntegerType)))
    spark.createDataFrame(rows, schema)

  // ---------------------------------------------------------------------------
  // Aggregate string overloads
  // ---------------------------------------------------------------------------

  test("mean(String) computes average") {
    val result = sampleDf.select(mean("age")).collect()
    assert(result.length == 1)
    assert(result.head.getDouble(0) == 30.0)
  }

  test("first(String) returns first value") {
    val result = sampleDf.select(first("name")).collect()
    assert(result.length == 1)
    assert(result.head.getString(0).nonEmpty)
  }

  test("last(String) returns last value") {
    val result = sampleDf.select(last("name")).collect()
    assert(result.length == 1)
    assert(result.head.getString(0).nonEmpty)
  }

  test("countDistinct(String, String*) counts distinct") {
    val result = sampleDf.select(countDistinct("name")).collect()
    assert(result.head.getLong(0) == 3)
  }

  test("collect_list(String) collects values") {
    val result = sampleDf.select(collect_list("name")).collect()
    assert(result.length == 1)
  }

  test("collect_set(String) collects distinct values") {
    val result = sampleDf.select(collect_set("name")).collect()
    assert(result.length == 1)
  }

  test("stddev(String) computes standard deviation") {
    val result = sampleDf.select(stddev("age")).collect()
    assert(result.length == 1)
    assert(result.head.getDouble(0) > 0)
  }

  test("variance(String) computes variance") {
    val result = sampleDf.select(variance("age")).collect()
    assert(result.length == 1)
    assert(result.head.getDouble(0) > 0)
  }

  // ---------------------------------------------------------------------------
  // Math string overloads
  // ---------------------------------------------------------------------------

  test("sqrt(String) computes square root") {
    val df = spark.range(1).toDF("id").withColumn("v", lit(4.0))
    val result = df.select(sqrt("v")).collect()
    assert(result.head.getDouble(0) == 2.0)
  }

  test("sin(String) computes sine") {
    val df = spark.range(1).toDF("id").withColumn("v", lit(0.0))
    val result = df.select(sin("v")).collect()
    assert(result.head.getDouble(0) == 0.0)
  }

  test("cos(String) computes cosine") {
    val df = spark.range(1).toDF("id").withColumn("v", lit(0.0))
    val result = df.select(cos("v")).collect()
    assert(result.head.getDouble(0) == 1.0)
  }

  test("floor(String) computes floor") {
    val df = spark.range(1).toDF("id").withColumn("v", lit(3.7))
    val result = df.select(floor("v")).collect()
    assert(result.head.getLong(0) == 3L)
  }

  test("ceil(String) computes ceiling") {
    val df = spark.range(1).toDF("id").withColumn("v", lit(3.2))
    val result = df.select(ceil("v")).collect()
    assert(result.head.getLong(0) == 4L)
  }

  // ---------------------------------------------------------------------------
  // Collection string overloads
  // ---------------------------------------------------------------------------

  test("array(String, String*) creates array column") {
    val result = sampleDf.select(array("name", "name").as("arr")).collect()
    assert(result.length == 3)
  }

  test("struct(String, String*) creates struct column") {
    val result = sampleDf.select(struct("name", "age").as("s")).collect()
    assert(result.length == 3)
  }

  test("greatest(String, String*) computes greatest") {
    val rows = Seq(Row(1, 3), Row(5, 2))
    val schema = StructType(Seq(StructField("a", IntegerType), StructField("b", IntegerType)))
    val df = spark.createDataFrame(rows, schema)
    val result = df.select(greatest("a", "b")).collect()
    assert(result(0).getInt(0) == 3)
    assert(result(1).getInt(0) == 5)
  }

  test("least(String, String*) computes least") {
    val rows = Seq(Row(1, 3), Row(5, 2))
    val schema = StructType(Seq(StructField("a", IntegerType), StructField("b", IntegerType)))
    val df = spark.createDataFrame(rows, schema)
    val result = df.select(least("a", "b")).collect()
    assert(result(0).getInt(0) == 1)
    assert(result(1).getInt(0) == 2)
  }
