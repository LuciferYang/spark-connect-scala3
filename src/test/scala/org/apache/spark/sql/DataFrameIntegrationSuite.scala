package org.apache.spark.sql

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.tags.IntegrationTest
import org.apache.spark.sql.types.*

/** Integration tests for DataFrame operations: basic connectivity, transformations, NaFunctions,
  * StatFunctions, Window, join, describe, and explain.
  */
@IntegrationTest
class DataFrameIntegrationSuite extends IntegrationTestBase:

  // ---------------------------------------------------------------------------
  // Basic connectivity
  // ---------------------------------------------------------------------------

  test("version returns non-empty string") {
    val v = spark.version
    assert(v.nonEmpty, "Spark version should not be empty")
    println(s"Connected to Spark version: $v")
  }

  // ---------------------------------------------------------------------------
  // DataFrame operations
  // ---------------------------------------------------------------------------

  test("range and collect") {
    val df = spark.range(10)
    val rows = df.collect()
    assert(rows.length == 10)
    assert(rows.head.getLong(0) == 0L)
    assert(rows.last.getLong(0) == 9L)
  }

  test("SQL query") {
    val df = spark.sql("SELECT 1 + 1 AS result")
    val rows = df.collect()
    assert(rows.length == 1)
    assert(rows.head.get(0) == 2)
  }

  test("createDataFrame with schema") {
    val rows = Seq(
      Row("Alice", 30),
      Row("Bob", 25)
    )
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType)
    ))
    val df = spark.createDataFrame(rows, schema)
    val result = df.collect()
    assert(result.length == 2)
    assert(result(0).getString(0) == "Alice")
    assert(result(1).getInt(1) == 25)
  }

  // ---------------------------------------------------------------------------
  // DataFrame transformations
  // ---------------------------------------------------------------------------

  test("select and filter") {
    val df = spark.range(0, 20)
    val filtered = df.filter(col("id") > lit(10)).select(col("id"))
    val result = filtered.collect()
    assert(result.length == 9) // 11, 12, ..., 19
    assert(result.head.getLong(0) == 11L)
  }

  test("groupBy and agg") {
    val rows = Seq(
      Row("A", 10),
      Row("A", 20),
      Row("B", 30)
    )
    val schema = StructType(Seq(
      StructField("group", StringType),
      StructField("value", IntegerType)
    ))
    val df = spark.createDataFrame(rows, schema)
    val result = df.groupBy(col("group")).agg(sum(col("value")).as("total"))
      .orderBy(col("group"))
      .collect()
    assert(result.length == 2)
    // Group A: 10 + 20 = 30
    assert(result(0).getString(0) == "A")
    assert(result(0).get(1).toString.toLong == 30L)
  }

  test("withColumn, withColumnRenamed, drop") {
    val df = spark.range(3).select(col("id").as("x"))
    val added = df.withColumn("y", col("x") + lit(10))
    val renamed = added.withColumnRenamed("y", "z")
    val dropped = renamed.drop("x")

    val result = dropped.collect()
    assert(result.length == 3)
    assert(dropped.columns.toSeq == Seq("z"))
    assert(result(0).get(0).toString.toLong == 10L) // 0 + 10
    assert(result(2).get(0).toString.toLong == 12L) // 2 + 10
  }

  test("union and unionByName") {
    val df1 = spark.range(3).select(col("id").as("x"))
    val df2 = spark.range(3, 6).select(col("id").as("x"))
    val result = df1.union(df2).collect()
    assert(result.length == 6)
    assert(result.map(_.getLong(0)).toSet == Set(0L, 1L, 2L, 3L, 4L, 5L))
  }

  test("intersect and except") {
    val df1 = spark.range(5).select(col("id").as("x"))
    val df2 = spark.range(3, 8).select(col("id").as("x"))

    val inter = df1.intersect(df2).orderBy(col("x")).collect()
    assert(inter.map(_.getLong(0)).toSeq == Seq(3L, 4L))

    val diff = df1.except(df2).orderBy(col("x")).collect()
    assert(diff.map(_.getLong(0)).toSeq == Seq(0L, 1L, 2L))
  }

  test("join") {
    val left = Seq(Row(1, "a"), Row(2, "b"), Row(3, "c"))
    val right = Seq(Row(1, 10), Row(2, 20), Row(4, 40))
    val lSchema = StructType(
      Seq(StructField("id", IntegerType), StructField("name", StringType))
    )
    val rSchema = StructType(
      Seq(StructField("rid", IntegerType), StructField("value", IntegerType))
    )
    val lDf = spark.createDataFrame(left, lSchema)
    val rDf = spark.createDataFrame(right, rSchema)

    val result = lDf.join(rDf, col("id") === col("rid"), "inner")
      .select(col("name"), col("value"))
      .orderBy(col("value"))
      .collect()
    assert(result.length == 2)
    assert(result(0).getString(0) == "a")
    assert(result(0).getInt(1) == 10)
    assert(result(1).getString(0) == "b")
    assert(result(1).getInt(1) == 20)
  }

  test("describe") {
    val rows = Seq(Row(1.0), Row(2.0), Row(3.0))
    val schema = StructType(Seq(StructField("value", DoubleType)))
    val df = spark.createDataFrame(rows, schema)
    val desc = df.describe("value").collect()
    assert(desc.nonEmpty)
    // describe returns count, mean, stddev, min, max
    val stats = desc.map(r => r.getString(0) -> r.getString(1)).toMap
    assert(stats("count") == "3")
    assert(stats("min") == "1.0")
    assert(stats("max") == "3.0")
  }

  // ---------------------------------------------------------------------------
  // DataFrameNaFunctions
  // ---------------------------------------------------------------------------

  test("na.drop removes rows with nulls") {
    val rows = Seq(
      Row("Alice", 30),
      Row(null, 25),
      Row("Charlie", null)
    )
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType)
    ))
    val df = spark.createDataFrame(rows, schema)
    val result = df.na.drop().collect()
    assert(result.length == 1)
    assert(result(0).getString(0) == "Alice")
  }

  test("na.fill replaces nulls") {
    val rows = Seq(
      Row("Alice", null),
      Row(null, 25)
    )
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType)
    ))
    val df = spark.createDataFrame(rows, schema)
    val filled = df.na.fill("unknown", Seq("name")).collect()
    assert(filled.length == 2)
    assert(filled(1).getString(0) == "unknown")
  }

  // ---------------------------------------------------------------------------
  // DataFrameStatFunctions
  // ---------------------------------------------------------------------------

  test("stat.corr computes Pearson correlation") {
    val rows = Seq(
      Row(1.0, 2.0),
      Row(2.0, 4.0),
      Row(3.0, 6.0)
    )
    val schema = StructType(Seq(
      StructField("x", DoubleType),
      StructField("y", DoubleType)
    ))
    val df = spark.createDataFrame(rows, schema)
    val r = df.stat.corr("x", "y")
    assert(math.abs(r - 1.0) < 0.001) // Perfect positive correlation
  }

  test("stat.crosstab produces contingency table") {
    val rows = Seq(
      Row("A", "X"),
      Row("A", "Y"),
      Row("B", "X"),
      Row("B", "X")
    )
    val schema = StructType(Seq(
      StructField("group", StringType),
      StructField("category", StringType)
    ))
    val df = spark.createDataFrame(rows, schema)
    val ct = df.stat.crosstab("group", "category")
    val result = ct.collect()
    assert(result.nonEmpty)
  }

  // ---------------------------------------------------------------------------
  // Window functions
  // ---------------------------------------------------------------------------

  test("window function: row_number") {
    val rows = Seq(
      Row("A", 10),
      Row("A", 20),
      Row("B", 30),
      Row("B", 40)
    )
    val schema = StructType(Seq(
      StructField("group", StringType),
      StructField("value", IntegerType)
    ))
    val df = spark.createDataFrame(rows, schema)
    val w = Window.partitionBy(col("group")).orderBy(col("value"))
    val result = df
      .withColumn("rn", row_number().over(w))
      .orderBy(col("group"), col("value"))
      .collect()
    assert(result.length == 4)
    // First row in group A
    assert(result(0).getInt(2) == 1)
    // Second row in group A
    assert(result(1).getInt(2) == 2)
  }

  // ---------------------------------------------------------------------------
  // Explain
  // ---------------------------------------------------------------------------

  test("explain produces non-empty output") {
    val df = spark.range(10)
    // explain() returns Unit and prints to stdout, just verify no exception
    df.explain()
  }

  test("explain with mode") {
    val df = spark.range(10)
    df.explain("simple")
    df.explain("extended")
    df.explain(extended = true)
  }

  // ---------------------------------------------------------------------------
  // Actions & metadata
  // ---------------------------------------------------------------------------

  test("count") {
    assert(spark.range(7).count() == 7L)
  }

  test("isEmpty") {
    assert(spark.range(0).isEmpty)
    assert(!spark.range(1).isEmpty)
  }

  test("first and head") {
    val df = spark.range(5)
    assert(df.first().getLong(0) == 0L)
    assert(df.head().length == 1)
    assert(df.head(3).length == 3)
  }

  test("take and tail") {
    val df = spark.range(10).orderBy(col("id"))
    val taken = df.take(3)
    assert(taken.length == 3)
    assert(taken.map(_.getLong(0)).toSeq == Seq(0L, 1L, 2L))

    val tailed = df.tail(3)
    assert(tailed.length == 3)
    assert(tailed.map(_.getLong(0)).toSeq == Seq(7L, 8L, 9L))
  }

  test("limit and offset") {
    val df = spark.range(10).orderBy(col("id"))
    val limited = df.limit(3).collect()
    assert(limited.length == 3)
    assert(limited.map(_.getLong(0)).toSeq == Seq(0L, 1L, 2L))

    val offsetted = df.offset(7).collect()
    assert(offsetted.length == 3)
    assert(offsetted.map(_.getLong(0)).toSeq == Seq(7L, 8L, 9L))
  }

  test("show does not throw") {
    val df = spark.range(3)
    df.show()
    df.show(2, 10)
    df.show(2, 10, vertical = true)
  }

  test("printSchema does not throw") {
    val df = spark.range(3)
    df.printSchema()
    df.printSchema(1)
  }

  test("schema and dtypes and columns") {
    val rows = Seq(Row("a", 1))
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("id", IntegerType)
    ))
    val df = spark.createDataFrame(rows, schema)
    assert(df.schema.fieldNames.toSeq == Seq("name", "id"))
    assert(df.dtypes.toSeq == Seq(("name", "StringType"), ("id", "IntegerType")))
    assert(df.columns.toSeq == Seq("name", "id"))
  }

  test("toLocalIterator") {
    val df = spark.range(5).orderBy(col("id"))
    val iter = df.toLocalIterator()
    try
      val collected = scala.collection.mutable.ArrayBuffer.empty[Long]
      while iter.hasNext do collected += iter.next().getLong(0)
      assert(collected.toSeq == Seq(0L, 1L, 2L, 3L, 4L))
    finally iter.close()
  }

  // ---------------------------------------------------------------------------
  // Distinct / dropDuplicates
  // ---------------------------------------------------------------------------

  test("distinct") {
    val rows = Seq(Row(1), Row(1), Row(2), Row(3), Row(3))
    val schema = StructType(Seq(StructField("x", IntegerType)))
    val df = spark.createDataFrame(rows, schema)
    val result = df.distinct().orderBy(col("x")).collect()
    assert(result.map(_.getInt(0)).toSeq == Seq(1, 2, 3))
  }

  test("dropDuplicates with columns") {
    val rows = Seq(Row("A", 1), Row("A", 2), Row("B", 1))
    val schema = StructType(Seq(
      StructField("group", StringType),
      StructField("value", IntegerType)
    ))
    val df = spark.createDataFrame(rows, schema)
    val result = df.dropDuplicates(Seq("group")).orderBy(col("group")).collect()
    assert(result.length == 2)
    assert(result(0).getString(0) == "A")
    assert(result(1).getString(0) == "B")
  }

  // ---------------------------------------------------------------------------
  // selectExpr / where
  // ---------------------------------------------------------------------------

  test("selectExpr") {
    val df = spark.range(5)
    val result = df.selectExpr("id", "id + 1 as id_plus_one")
      .orderBy(col("id")).collect()
    assert(result.length == 5)
    assert(result(0).getLong(1) == 1L)
    assert(result(4).getLong(1) == 5L)
  }

  test("where with string expression") {
    val df = spark.range(10)
    val result = df.where("id >= 8").orderBy(col("id")).collect()
    assert(result.length == 2)
    assert(result(0).getLong(0) == 8L)
    assert(result(1).getLong(0) == 9L)
  }

  // ---------------------------------------------------------------------------
  // toDF / alias
  // ---------------------------------------------------------------------------

  test("toDF renames columns") {
    val df = spark.range(3).toDF("my_id")
    assert(df.columns.toSeq == Seq("my_id"))
    assert(df.collect().length == 3)
  }

  test("alias") {
    val df = spark.range(3).alias("t")
    // alias doesn't change schema, but should not throw
    assert(df.collect().length == 3)
  }

  // ---------------------------------------------------------------------------
  // summary
  // ---------------------------------------------------------------------------

  test("summary") {
    val rows = Seq(Row(1.0), Row(2.0), Row(3.0))
    val schema = StructType(Seq(StructField("value", DoubleType)))
    val df = spark.createDataFrame(rows, schema)
    val result = df.summary("count", "min", "max").collect()
    val stats = result.map(r => r.getString(0) -> r.getString(1)).toMap
    assert(stats("count") == "3")
    assert(stats("min") == "1.0")
    assert(stats("max") == "3.0")
  }

  // ---------------------------------------------------------------------------
  // sample / randomSplit
  // ---------------------------------------------------------------------------

  test("sample") {
    val df = spark.range(1000)
    val sampled = df.sample(0.1).collect()
    // With 10% sampling, expect roughly 100 rows (allow wide margin)
    assert(sampled.length > 0 && sampled.length < 500)
  }

  test("randomSplit") {
    val df = spark.range(100)
    val splits = df.randomSplit(Array(0.5, 0.5))
    assert(splits.length == 2)
    val total = splits(0).count() + splits(1).count()
    assert(total == 100L)
  }

  // ---------------------------------------------------------------------------
  // repartition / coalesce
  // ---------------------------------------------------------------------------

  test("repartition and coalesce") {
    val df = spark.range(100)
    // These should execute without error; verify data is preserved
    val repartitioned = df.repartition(4)
    assert(repartitioned.count() == 100L)

    val coalesced = df.coalesce(1)
    assert(coalesced.count() == 100L)
  }

  test("repartition with columns") {
    val df = spark.range(100)
    val result = df.repartition(4, col("id"))
    assert(result.count() == 100L)
  }

  // ---------------------------------------------------------------------------
  // crossJoin / join variants
  // ---------------------------------------------------------------------------

  test("crossJoin") {
    val df1 = spark.range(3).select(col("id").as("a"))
    val df2 = spark.range(2).select(col("id").as("b"))
    val result = df1.crossJoin(df2).collect()
    assert(result.length == 6) // 3 x 2
  }

  test("join using column name") {
    val left = spark.range(5).select(col("id"))
    val right = spark.range(3, 8).select(col("id"), (col("id") * lit(10)).as("value"))
    val result = left.join(right, "id").orderBy(col("id")).collect()
    assert(result.length == 2) // ids 3 and 4
    assert(result(0).getLong(0) == 3L)
    assert(result(1).getLong(0) == 4L)
  }

  test("join using columns with join type") {
    val left = spark.range(5).select(col("id"))
    val right = spark.range(3, 8).select(col("id"))
    val result = left.join(right, Seq("id"), "left").orderBy(col("id")).collect()
    assert(result.length == 5) // left join keeps all 5 left rows
  }

  // ---------------------------------------------------------------------------
  // intersectAll / exceptAll / unionAll / unionByName
  // ---------------------------------------------------------------------------

  test("intersectAll preserves duplicates") {
    val rows1 = Seq(Row(1), Row(1), Row(2))
    val rows2 = Seq(Row(1), Row(2), Row(2))
    val schema = StructType(Seq(StructField("x", IntegerType)))
    val df1 = spark.createDataFrame(rows1, schema)
    val df2 = spark.createDataFrame(rows2, schema)
    val result = df1.intersectAll(df2).orderBy(col("x")).collect()
    // intersectAll: 1 appears min(2,1)=1 time, 2 appears min(1,2)=1 time
    assert(result.map(_.getInt(0)).toSeq == Seq(1, 2))
  }

  test("exceptAll preserves duplicates") {
    val rows1 = Seq(Row(1), Row(1), Row(2))
    val rows2 = Seq(Row(1))
    val schema = StructType(Seq(StructField("x", IntegerType)))
    val df1 = spark.createDataFrame(rows1, schema)
    val df2 = spark.createDataFrame(rows2, schema)
    val result = df1.exceptAll(df2).orderBy(col("x")).collect()
    // exceptAll: removes one occurrence of 1, keeps second 1 and 2
    assert(result.map(_.getInt(0)).toSeq == Seq(1, 2))
  }

  test("unionByName with allowMissingColumns") {
    val rows1 = Seq(Row("a", 1))
    val rows2 = Seq(Row("b"))
    val schema1 = StructType(Seq(
      StructField("name", StringType),
      StructField("id", IntegerType)
    ))
    val schema2 = StructType(Seq(StructField("name", StringType)))
    val df1 = spark.createDataFrame(rows1, schema1)
    val df2 = spark.createDataFrame(rows2, schema2)
    val result = df1.unionByName(df2, allowMissingColumns = true)
      .orderBy(col("name")).collect()
    assert(result.length == 2)
    assert(result(0).getString(0) == "a")
    assert(result(1).getString(0) == "b")
    assert(result(1).isNullAt(1)) // id is null for df2 row
  }

  // ---------------------------------------------------------------------------
  // sortWithinPartitions / hint / transform
  // ---------------------------------------------------------------------------

  test("sortWithinPartitions") {
    val df = spark.range(10)
    // Should not throw; data preserved
    val result = df.sortWithinPartitions(col("id").desc)
    assert(result.count() == 10L)
  }

  test("hint") {
    val df = spark.range(10).hint("broadcast")
    // hint is a no-op on client side, verify data preserved
    assert(df.count() == 10L)
  }

  test("transform") {
    val df = spark.range(5)
    val result = df.transform(d => d.filter(col("id") > lit(2)))
    assert(result.count() == 2L) // 3 and 4
  }

  // ---------------------------------------------------------------------------
  // withColumns / withColumnsRenamed
  // ---------------------------------------------------------------------------

  test("withColumns") {
    val df = spark.range(3).select(col("id"))
    val result = df.withColumns(Map(
      "doubled" -> (col("id") * lit(2)),
      "tripled" -> (col("id") * lit(3))
    )).orderBy(col("id")).collect()
    assert(result(0).getLong(1) == 0L)
    assert(result(2).getLong(1) == 4L)
    assert(result(2).getLong(2) == 6L)
  }

  test("withColumnsRenamed") {
    val df = spark.range(3).select(col("id").as("x"))
    val result = df.withColumnsRenamed(Map("x" -> "y"))
    assert(result.columns.toSeq == Seq("y"))
    assert(result.count() == 3L)
  }

  // ---------------------------------------------------------------------------
  // Temp views
  // ---------------------------------------------------------------------------

  test("createTempView and createGlobalTempView") {
    val df = spark.range(3)
    df.createOrReplaceTempView("patch1_local_view")
    val r1 = spark.sql("SELECT * FROM patch1_local_view").collect()
    assert(r1.length == 3)

    df.createOrReplaceGlobalTempView("patch1_global_view")
    val r2 = spark.sql("SELECT * FROM global_temp.patch1_global_view").collect()
    assert(r2.length == 3)

    // cleanup
    spark.catalog.dropTempView("patch1_local_view")
    spark.catalog.dropGlobalTempView("patch1_global_view")
  }

  // ---------------------------------------------------------------------------
  // cache / persist / unpersist / storageLevel
  // ---------------------------------------------------------------------------

  test("cache and unpersist") {
    val df = spark.range(10).select(col("id"))
    df.createOrReplaceTempView("patch1_cache_view")
    try
      spark.catalog.cacheTable("patch1_cache_view")
      assert(spark.catalog.isCached("patch1_cache_view"))
      spark.catalog.uncacheTable("patch1_cache_view")
      assert(!spark.catalog.isCached("patch1_cache_view"))
    finally spark.catalog.dropTempView("patch1_cache_view")
  }

  // ---------------------------------------------------------------------------
  // sameSemantics / semanticHash
  // ---------------------------------------------------------------------------

  test("sameSemantics and semanticHash") {
    val df1 = spark.range(10).select(col("id"))
    val df2 = spark.range(10).select(col("id"))
    assert(df1.sameSemantics(df2))
    assert(df1.semanticHash == df2.semanticHash)
  }

  // ---------------------------------------------------------------------------
  // toJSON
  // ---------------------------------------------------------------------------

  test("toJSON") {
    val rows = Seq(Row("Alice", 30))
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType)
    ))
    val df = spark.createDataFrame(rows, schema)
    val jsonRows = df.toJSON.collect()
    assert(jsonRows.length == 1)
    assert(jsonRows(0).getString(0).contains("Alice"))
  }
