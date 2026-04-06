package org.apache.spark.sql

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.tags.IntegrationTest

/** Integration tests for scalar, exists, and IN subqueries. */
@IntegrationTest
class SubqueryIntegrationSuite extends IntegrationTestBase:

  test("scalar subquery in filter") {
    val df = spark.range(10).select(col("id").as("value"))
    val maxVal = df.select(max(col("value"))).scalar()
    val result = df.filter(col("value") === maxVal).collect()
    assert(result.length == 1)
    assert(result(0).getLong(0) == 9L)
  }

  test("scalar subquery in select") {
    val df = spark.range(5).select(col("id").as("value"))
    val total = df.select(sum(col("value"))).scalar()
    val result = df.select(col("value"), total.as("total")).collect()
    assert(result.length == 5)
    // Every row should have total = 0+1+2+3+4 = 10
    result.foreach(r => assert(r.getLong(1) == 10L))
  }

  test("exists subquery in filter") {
    // EXISTS subquery: keep all rows when the subquery returns non-empty result
    val df = spark.range(10).select(col("id").as("value"))
    // Subquery that always returns a result (non-empty)
    val alwaysExists = spark.range(1).exists()
    val result = df.filter(alwaysExists).collect()
    assert(result.length == 10)

    // Subquery that returns empty result
    val neverExists = spark.range(0).exists()
    val emptyResult = df.filter(neverExists).collect()
    assert(emptyResult.length == 0)
  }

  test("IN subquery in filter") {
    val df = spark.range(10).select(col("id").as("value"))
    val subset = spark.range(3, 6).select(col("id"))
    val result = df.filter(col("value").isin(subset))
      .orderBy(col("value")).collect()
    assert(result.length == 3)
    assert(result(0).getLong(0) == 3L)
    assert(result(1).getLong(0) == 4L)
    assert(result(2).getLong(0) == 5L)
  }

  test("scalar subquery combined with arithmetic") {
    val df = spark.range(5).select(col("id").as("value"))
    val total = df.select(sum(col("value"))).scalar()
    // Use scalar subquery in arithmetic: value * 100 / total
    val result = df.select(
      col("value"),
      (col("value") * lit(100) / total).as("pct")
    ).orderBy(col("value")).collect()
    assert(result.length == 5)
    // value=0 → 0*100/10=0, value=1 → 100/10=10, value=4 → 400/10=40
    assert(result(0).get(1).toString.toDouble == 0.0)
    assert(result(4).get(1).toString.toDouble == 40.0)
  }
