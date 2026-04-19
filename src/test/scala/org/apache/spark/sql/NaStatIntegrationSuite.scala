package org.apache.spark.sql

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.tags.IntegrationTest
import org.apache.spark.sql.types.*

/** Integration tests for DataFrameNaFunctions and DataFrameStatFunctions. */
@IntegrationTest
class NaStatIntegrationSuite extends IntegrationTestBase:

  // ---------------------------------------------------------------------------
  // DataFrameNaFunctions
  // ---------------------------------------------------------------------------

  private def naTestDf: DataFrame =
    val rows = Seq(
      Row("Alice", 30, 1.0),
      Row(null, 25, 2.0),
      Row("Charlie", null, 3.0),
      Row(null, null, null)
    )
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("score", DoubleType)
    ))
    spark.createDataFrame(rows, schema)

  test("na.drop() removes rows with any null") {
    val result = naTestDf.na.drop().collect()
    assert(result.length == 1)
    assert(result(0).getString(0) == "Alice")
  }

  test("na.drop(how=all) removes rows where all are null") {
    val result = naTestDf.na.drop("all").collect()
    assert(result.length == 3) // only the all-null row is dropped
  }

  test("na.drop(how, cols) drops based on specific columns") {
    val result = naTestDf.na.drop("any", Seq("name")).collect()
    assert(result.length == 2) // rows where name is not null
    result.foreach(r => assert(!r.isNullAt(0)))
  }

  test("na.drop(minNonNulls) keeps rows with at least N non-nulls") {
    val result = naTestDf.na.drop(2).collect()
    // Alice has 3 non-nulls, row2 has 2, row3 has 2, row4 has 0
    assert(result.length == 3)
  }

  test("na.fill(double) replaces nulls in numeric columns") {
    val result = naTestDf.na.fill(0.0).collect()
    // row4: age=null -> 0, score=null -> 0.0
    val row4 = result(3)
    assert(row4.get(1) != null && row4.get(1).toString.toInt == 0)
    assert(row4.get(2) != null && row4.get(2).toString.toDouble == 0.0)
  }

  test("na.fill(string) replaces nulls in string columns") {
    val result = naTestDf.na.fill("unknown").collect()
    assert(result(1).getString(0) == "unknown")
    assert(result(3).getString(0) == "unknown")
  }

  test("na.fill(double, cols) fills specific columns") {
    val result = naTestDf.na.fill(99.0, Seq("score")).collect()
    // row4 score should be 99.0, but name remains null
    assert(result(3).isNullAt(0)) // name still null
    assert(result(3).getDouble(2) == 99.0)
  }

  test("na.fill(string, cols) fills specific string columns") {
    val result = naTestDf.na.fill("N/A", Seq("name")).collect()
    assert(result(1).getString(0) == "N/A")
    assert(result(3).getString(0) == "N/A")
  }

  test("na.fill(valueMap) fills different values per column") {
    val result = naTestDf.na.fill(Map("name" -> "unknown", "age" -> 0)).collect()
    assert(result(1).getString(0) == "unknown")
    assert(result(2).get(1).toString.toInt == 0)
  }

  test("na.replace replaces values") {
    val rows = Seq(Row("a", 1), Row("b", 2), Row("c", 3))
    val schema = StructType(Seq(
      StructField("key", StringType),
      StructField("value", IntegerType)
    ))
    val df = spark.createDataFrame(rows, schema)
    val result = df.na.replace("key", Map("a" -> "x", "c" -> "z"))
      .orderBy(col("value")).collect()
    assert(result(0).getString(0) == "x")
    assert(result(1).getString(0) == "b")
    assert(result(2).getString(0) == "z")
  }

  // ---------------------------------------------------------------------------
  // DataFrameStatFunctions
  // ---------------------------------------------------------------------------

  private def statTestDf: DataFrame =
    val rows = Seq(
      Row(1.0, 2.0, "A"),
      Row(2.0, 4.0, "B"),
      Row(3.0, 6.0, "A"),
      Row(4.0, 8.0, "B"),
      Row(5.0, 10.0, "A")
    )
    val schema = StructType(Seq(
      StructField("x", DoubleType),
      StructField("y", DoubleType),
      StructField("group", StringType)
    ))
    spark.createDataFrame(rows, schema)

  test("stat.corr with default method") {
    val r = statTestDf.stat.corr("x", "y")
    assert(math.abs(r - 1.0) < 0.001)
  }

  test("stat.corr with explicit method") {
    val r = statTestDf.stat.corr("x", "y", "pearson")
    assert(math.abs(r - 1.0) < 0.001)
  }

  test("stat.cov") {
    val c = statTestDf.stat.cov("x", "y")
    assert(c > 0) // positive covariance for perfectly correlated data
  }

  test("stat.crosstab") {
    val ct = statTestDf.stat.crosstab("group", "x")
    assert(ct.collect().nonEmpty)
    assert(ct.columns.contains("group_x"))
  }

  test("stat.approxQuantile single column") {
    val quantiles = statTestDf.stat.approxQuantile("x", Array(0.0, 0.5, 1.0), 0.01)
    assert(quantiles.length == 3)
    assert(quantiles(0) <= quantiles(1))
    assert(quantiles(1) <= quantiles(2))
  }

  test("stat.approxQuantile multiple columns") {
    val quantiles = statTestDf.stat.approxQuantile(
      Array("x", "y"),
      Array(0.0, 1.0),
      0.01
    )
    assert(quantiles.length == 2) // one array per column
    assert(quantiles(0).length == 2) // two quantiles per column
  }

  test("stat.freqItems with support") {
    val rows = (1 to 100).map(i => Row(if i % 2 == 0 then "even" else "odd"))
    val schema = StructType(Seq(StructField("label", StringType)))
    val df = spark.createDataFrame(rows, schema)
    val result = df.stat.freqItems(Seq("label"), 0.4).collect()
    assert(result.nonEmpty)
    // Both "even" and "odd" appear 50% of the time, should be frequent at 40% support
  }

  test("stat.freqItems with default support") {
    val rows = (1 to 100).map(i => Row(i % 3))
    val schema = StructType(Seq(StructField("x", IntegerType)))
    val df = spark.createDataFrame(rows, schema)
    val result = df.stat.freqItems(Seq("x")).collect()
    assert(result.nonEmpty)
  }

  test("stat.sampleBy with Column") {
    val rows = (1 to 100).map(i => Row(if i <= 50 then "A" else "B"))
    val schema = StructType(Seq(StructField("group", StringType)))
    val df = spark.createDataFrame(rows, schema)
    val sampled = df.stat.sampleBy(
      col("group"),
      Map("A" -> 0.5, "B" -> 0.1),
      seed = 42L
    )
    val counts = sampled.groupBy(col("group")).count().collect()
      .map(r => r.getString(0) -> r.getLong(1)).toMap
    // A should have roughly 25, B roughly 5
    assert(counts.getOrElse("A", 0L) > 0)
    assert(counts.getOrElse("B", 0L) >= 0)
    assert(counts.getOrElse("A", 0L) > counts.getOrElse("B", 0L))
  }

  test("stat.sampleBy with string column name") {
    val rows = (1 to 100).map(i => Row(if i <= 50 then "X" else "Y"))
    val schema = StructType(Seq(StructField("label", StringType)))
    val df = spark.createDataFrame(rows, schema)
    val sampled = df.stat.sampleBy("label", Map("X" -> 0.8, "Y" -> 0.2), seed = 42L)
    assert(sampled.count() > 0)
  }

  // ---------------------------------------------------------------------------
  // P1: NaFunctions additional overloads
  // ---------------------------------------------------------------------------

  test("na.drop(minNonNulls, cols) filters on specified columns") {
    val result = naTestDf.na.drop(1, Seq("name", "age")).collect()
    // Rows with at least 1 non-null among (name, age):
    // Alice(name=Alice, age=30) -> 2 non-null ✓
    // row2(name=null, age=25) -> 1 non-null ✓
    // row3(name=Charlie, age=null) -> 1 non-null ✓
    // row4(name=null, age=null) -> 0 non-null ✗
    assert(result.length == 3)
  }

  test("na.fill(Boolean, cols) fills boolean columns") {
    val rows = Seq(
      Row("Alice", null),
      Row("Bob", true)
    )
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("active", types.BooleanType)
    ))
    val df = spark.createDataFrame(rows, schema)
    val result = df.na.fill(false, Seq("active")).orderBy(col("name")).collect()
    // null booleans should be filled with false
    assert(result(0).getBoolean(1) == false) // Alice's null -> false
    assert(result(1).getBoolean(1) == true) // Bob stays true
  }

  test("na.replace(Seq, Map) replaces across multiple columns") {
    val rows = Seq(Row("a", "a"), Row("b", "b"), Row("c", "c"))
    val schema = StructType(Seq(
      StructField("col1", StringType),
      StructField("col2", StringType)
    ))
    val df = spark.createDataFrame(rows, schema)
    val result = df.na.replace(Seq("col1", "col2"), Map("a" -> "x")).collect()
    // Both col1 and col2 should have "a" replaced with "x"
    val row0 = result.find(r => r.getString(0) == "x" || r.getString(1) == "x")
    assert(row0.isDefined)
  }

  // ---------------------------------------------------------------------------
  // BloomFilter & CountMinSketch
  // ---------------------------------------------------------------------------

  private def sketchTestDf: DataFrame =
    val rows = (1 to 1000).map(i => Row(i.toLong))
    val schema = StructType(Seq(StructField("id", types.LongType)))
    spark.createDataFrame(rows, schema)

  test("stat.bloomFilter(colName, expectedNumItems, fpp)") {
    val bf = sketchTestDf.stat.bloomFilter("id", 1000L, 0.01)
    assert(bf.mightContain(1L))
    assert(bf.mightContain(500L))
    assert(bf.mightContain(1000L))
    assert(bf.expectedFpp() <= 0.05)
  }

  test("stat.bloomFilter(col, expectedNumItems, fpp)") {
    val bf = sketchTestDf.stat.bloomFilter(col("id"), 1000L, 0.01)
    assert(bf.mightContain(1L))
    assert(bf.mightContain(1000L))
  }

  test("stat.bloomFilter(colName, expectedNumItems, numBits)") {
    val bf = sketchTestDf.stat.bloomFilter("id", 1000L, 8192L)
    assert(bf.mightContain(1L))
    assert(bf.mightContain(500L))
  }

  test("stat.bloomFilter(col, expectedNumItems, numBits)") {
    val bf = sketchTestDf.stat.bloomFilter(col("id"), 1000L, 8192L)
    assert(bf.mightContain(1L))
  }

  test("stat.countMinSketch(colName, eps, confidence, seed)") {
    val cms = sketchTestDf.stat.countMinSketch("id", 0.01, 0.99, 42)
    // Each value appears exactly once
    assert(cms.estimateCount(1L) >= 1)
    assert(cms.estimateCount(500L) >= 1)
    assert(cms.totalCount() == 1000L)
  }

  test("stat.countMinSketch(col, eps, confidence, seed)") {
    val cms = sketchTestDf.stat.countMinSketch(col("id"), 0.01, 0.99, 42)
    assert(cms.estimateCount(1L) >= 1)
    assert(cms.totalCount() == 1000L)
  }

  test("stat.countMinSketch(colName, depth, width, seed)") {
    val cms = sketchTestDf.stat.countMinSketch("id", 5, 100, 42)
    assert(cms.estimateCount(1L) >= 1)
    assert(cms.totalCount() == 1000L)
  }

  test("stat.countMinSketch(col, depth, width, seed)") {
    val cms = sketchTestDf.stat.countMinSketch(col("id"), 5, 100, 42)
    assert(cms.estimateCount(1L) >= 1)
    assert(cms.totalCount() == 1000L)
  }
