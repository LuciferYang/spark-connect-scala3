package org.apache.spark.sql

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.*

/**
 * Integration tests that require a running Spark Connect server at sc://localhost:15002.
 *
 * Start a Spark Connect server with:
 *   ./sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.13:4.1.1
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
      Row("A", 10), Row("A", 20), Row("B", 30)
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
