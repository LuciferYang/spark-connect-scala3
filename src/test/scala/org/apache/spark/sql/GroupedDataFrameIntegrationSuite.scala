package org.apache.spark.sql

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.tags.IntegrationTest
import org.apache.spark.sql.types.*

/** Integration tests for GroupedDataFrame: count, mean/avg, max, min, sum, agg(Map), pivot.
  */
@IntegrationTest
class GroupedDataFrameIntegrationSuite extends IntegrationTestBase:

  private def groupTestDf: DataFrame =
    val rows = Seq(
      Row("A", 10, 1.0),
      Row("A", 20, 2.0),
      Row("B", 30, 3.0),
      Row("B", 40, 4.0),
      Row("B", 50, 5.0)
    )
    val schema = StructType(Seq(
      StructField("group", StringType),
      StructField("value", IntegerType),
      StructField("score", DoubleType)
    ))
    spark.createDataFrame(rows, schema)

  // ---------------------------------------------------------------------------
  // count
  // ---------------------------------------------------------------------------

  test("groupBy.count") {
    val result = groupTestDf.groupBy(col("group")).count()
      .orderBy(col("group")).collect()
    assert(result(0).getString(0) == "A")
    assert(result(0).getLong(1) == 2)
    assert(result(1).getString(0) == "B")
    assert(result(1).getLong(1) == 3)
  }

  // ---------------------------------------------------------------------------
  // mean / avg
  // ---------------------------------------------------------------------------

  test("groupBy.mean") {
    val result = groupTestDf.groupBy(col("group")).mean("value")
      .orderBy(col("group")).collect()
    assert(result(0).get(1).toString.toDouble == 15.0) // A: avg(10,20)
    assert(result(1).get(1).toString.toDouble == 40.0) // B: avg(30,40,50)
  }

  test("groupBy.avg is alias for mean") {
    val result = groupTestDf.groupBy(col("group")).avg("value")
      .orderBy(col("group")).collect()
    assert(result(0).get(1).toString.toDouble == 15.0)
  }

  test("mean with multiple columns") {
    val result = groupTestDf.groupBy(col("group")).mean("value", "score")
      .orderBy(col("group")).collect()
    // A: avg(value)=15, avg(score)=1.5
    assert(result(0).get(1).toString.toDouble == 15.0)
    assert(result(0).get(2).toString.toDouble == 1.5)
  }

  // ---------------------------------------------------------------------------
  // max / min / sum
  // ---------------------------------------------------------------------------

  test("groupBy.max") {
    val result = groupTestDf.groupBy(col("group")).max("value")
      .orderBy(col("group")).collect()
    assert(result(0).get(1).toString.toInt == 20) // A max
    assert(result(1).get(1).toString.toInt == 50) // B max
  }

  test("groupBy.min") {
    val result = groupTestDf.groupBy(col("group")).min("value")
      .orderBy(col("group")).collect()
    assert(result(0).get(1).toString.toInt == 10)
    assert(result(1).get(1).toString.toInt == 30)
  }

  test("groupBy.sum") {
    val result = groupTestDf.groupBy(col("group")).sum("value")
      .orderBy(col("group")).collect()
    assert(result(0).get(1).toString.toLong == 30L) // A: 10+20
    assert(result(1).get(1).toString.toLong == 120L) // B: 30+40+50
  }

  test("sum with multiple columns") {
    val result = groupTestDf.groupBy(col("group")).sum("value", "score")
      .orderBy(col("group")).collect()
    assert(result(0).get(1).toString.toLong == 30L)
    assert(result(0).get(2).toString.toDouble == 3.0) // A: 1.0+2.0
  }

  // ---------------------------------------------------------------------------
  // agg with Column expressions
  // ---------------------------------------------------------------------------

  test("groupBy.agg with Column expressions") {
    val result = groupTestDf.groupBy(col("group")).agg(
      sum(col("value")).as("total"),
      max(col("score")).as("max_score")
    ).orderBy(col("group")).collect()
    assert(result(0).get(1).toString.toLong == 30L)
    assert(result(0).get(2).toString.toDouble == 2.0)
    assert(result(1).get(1).toString.toLong == 120L)
    assert(result(1).get(2).toString.toDouble == 5.0)
  }

  // ---------------------------------------------------------------------------
  // agg with Map[String, String]
  // ---------------------------------------------------------------------------

  test("groupBy.agg with Map[String, String]") {
    val result = groupTestDf.groupBy(col("group"))
      .agg(Map("value" -> "sum", "score" -> "max"))
      .orderBy(col("group")).collect()
    assert(result.length == 2)
    // verify columns exist (order may vary with Map)
    assert(result(0).size >= 3)
  }

  // ---------------------------------------------------------------------------
  // pivot
  // ---------------------------------------------------------------------------

  test("groupBy.pivot with aggregation") {
    val rows = Seq(
      Row("Alice", "Math", 90),
      Row("Alice", "Science", 85),
      Row("Bob", "Math", 80),
      Row("Bob", "Science", 95)
    )
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("subject", StringType),
      StructField("score", IntegerType)
    ))
    val df = spark.createDataFrame(rows, schema)
    val result = df.groupBy(col("name"))
      .pivot(col("subject"))
      .agg(sum(col("score")))
      .orderBy(col("name")).collect()
    assert(result.length == 2)
    // Alice row should have Math=90, Science=85
    assert(result(0).getString(0) == "Alice")
  }

  // ---------------------------------------------------------------------------
  // rollup / cube (convenience methods on GroupedDataFrame)
  // ---------------------------------------------------------------------------

  test("rollup.sum") {
    val result = groupTestDf.rollup(col("group")).sum("value")
      .orderBy(col("group")).collect()
    // rollup produces: (A, 30), (B, 120), (null, 150)
    assert(result.length == 3)
    // null group is the grand total
    val grandTotal = result.find(_.isNullAt(0)).get
    assert(grandTotal.get(1).toString.toLong == 150L)
  }

  test("cube.sum") {
    val result = groupTestDf.cube(col("group")).sum("value")
      .orderBy(col("group")).collect()
    assert(result.length == 3) // same as rollup for single group column
    val grandTotal = result.find(_.isNullAt(0)).get
    assert(grandTotal.get(1).toString.toLong == 150L)
  }

  // ---------------------------------------------------------------------------
  // P1: pivot(String) overloads
  // ---------------------------------------------------------------------------

  test("pivot(String) with aggregation") {
    val rows = Seq(
      Row("Alice", "Math", 90),
      Row("Alice", "Science", 85),
      Row("Bob", "Math", 80),
      Row("Bob", "Science", 95)
    )
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("subject", StringType),
      StructField("score", IntegerType)
    ))
    val df = spark.createDataFrame(rows, schema)
    val result = df.groupBy(col("name"))
      .pivot("subject")
      .agg(sum(col("score")))
      .orderBy(col("name")).collect()
    assert(result.length == 2)
    assert(result(0).getString(0) == "Alice")
  }

  test("pivot(String, Seq) with explicit values") {
    val rows = Seq(
      Row("Alice", "Math", 90),
      Row("Alice", "Science", 85),
      Row("Bob", "Math", 80),
      Row("Bob", "Science", 95)
    )
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("subject", StringType),
      StructField("score", IntegerType)
    ))
    val df = spark.createDataFrame(rows, schema)
    val result = df.groupBy(col("name"))
      .pivot("subject", Seq("Math", "Science"))
      .agg(sum(col("score")))
      .orderBy(col("name")).collect()
    assert(result.length == 2)
    // With explicit pivot values, columns should be predictable
    assert(result(0).getString(0) == "Alice")
  }

  // ---------------------------------------------------------------------------
  // P2: GroupedDataFrame Java interop
  // ---------------------------------------------------------------------------

  test("agg(java.util.Map) on GroupedDataFrame") {
    val jMap = new java.util.HashMap[String, String]()
    jMap.put("value", "sum")
    val result = groupTestDf.groupBy(col("group")).agg(jMap).orderBy(col("group")).collect()
    assert(result.length == 2)
    assert(result(0).getString(0) == "A")
  }
