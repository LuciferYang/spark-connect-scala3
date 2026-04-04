package org.apache.spark.sql

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.*

/** Integration tests that require a running Spark Connect server at sc://localhost:15002.
  *
  * Start a Spark Connect server with: $SPARK_HOME/sbin/start-connect-server.sh
  *
  * Run with: build/sbt 'testOnly *IntegrationSuite'
  */
class IntegrationSuite extends org.scalatest.funsuite.AnyFunSuite:

  lazy val spark: SparkSession =
    SparkSession.builder().remote("sc://localhost:15002").build()

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
  // Dataset[T] with Encoder
  // ---------------------------------------------------------------------------

  case class Person(name: String, age: Int) derives Encoder

  test("createDataset and collect typed") {
    val ds = spark.createDataset(Seq(
      Person("Alice", 30),
      Person("Bob", 25)
    ))
    val result = ds.collect()
    assert(result.length == 2)
    assert(result(0) == Person("Alice", 30))
    assert(result(1) == Person("Bob", 25))
  }

  test("Dataset map and filter") {
    val ds = spark.createDataset(Seq(
      Person("Alice", 30),
      Person("Bob", 25),
      Person("Charlie", 35)
    ))
    val names = ds.filter(_.age > 28).map(_.name)
    val result = names.collect()
    assert(result.toSet == Set("Alice", "Charlie"))
  }

  // ---------------------------------------------------------------------------
  // UDF (User-Defined Functions)
  // Note: UDFs require ArtifactManager to upload class files to the server.
  // These tests will work only after ArtifactManager is implemented.
  // ---------------------------------------------------------------------------

  test("UDF register and use in SQL") {
    val addOne = udf((x: Int) => x + 1)
    spark.udf.register("add_one", addOne)

    val df = spark.range(5)
    df.createOrReplaceTempView("numbers")

    val result = spark.sql("SELECT add_one(CAST(id AS INT)) AS result FROM numbers")
      .collect()
    assert(result.length == 5)
    assert(result(0).get(0).toString.toInt == 1) // 0 + 1
    assert(result(4).get(0).toString.toInt == 5) // 4 + 1
  }

  test("UDF applied to columns directly") {
    val doubler = udf((x: Int) => x * 2)
    val rows = Seq(Row(1), Row(2), Row(3))
    val schema = StructType(Seq(StructField("value", IntegerType)))
    val df = spark.createDataFrame(rows, schema)

    val result = df.select(doubler(col("value")).as("doubled")).collect()
    assert(result.length == 3)
    assert(result(0).get(0).toString.toInt == 2)
    assert(result(1).get(0).toString.toInt == 4)
    assert(result(2).get(0).toString.toInt == 6)
  }

  test("UDF with multiple arguments") {
    val concat = udf((a: String, b: String) => s"$a-$b")
    val rows = Seq(
      Row("hello", "world"),
      Row("foo", "bar")
    )
    val schema = StructType(Seq(
      StructField("a", StringType),
      StructField("b", StringType)
    ))
    val df = spark.createDataFrame(rows, schema)

    val result = df.select(concat(col("a"), col("b")).as("merged")).collect()
    assert(result.length == 2)
    assert(result(0).getString(0) == "hello-world")
    assert(result(1).getString(0) == "foo-bar")
  }

  // ---------------------------------------------------------------------------
  // Explain
  // ---------------------------------------------------------------------------

  test("explain produces non-empty output") {
    val df = spark.range(10)
    // explain() returns Unit and prints to stdout, just verify no exception
    df.explain()
  }

  // ---------------------------------------------------------------------------
  // Catalog
  // ---------------------------------------------------------------------------

  test("catalog listDatabases") {
    val dbs = spark.catalog.listDatabases()
    val rows = dbs.collect()
    assert(rows.nonEmpty)
    println(s"Found ${rows.length} database(s)")
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
  // Structured Streaming
  // ---------------------------------------------------------------------------

  test("structured streaming: rate source lifecycle") {
    val df = spark.readStream.format("rate").option("rowsPerSecond", "10").load()
    val query = df.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(1000))
      .start()

    try
      assert(query.id.nonEmpty)
      assert(query.runId.nonEmpty)
      assert(query.isActive)
    finally query.stop()
  }

  test("streams.active lists running queries") {
    val df = spark.readStream.format("rate").option("rowsPerSecond", "1").load()
    val query = df.writeStream
      .format("console")
      .queryName("active_test")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2000))
      .start()

    try
      val active = spark.streams.active
      assert(active.exists(_.id == query.id))
    finally query.stop()
  }
