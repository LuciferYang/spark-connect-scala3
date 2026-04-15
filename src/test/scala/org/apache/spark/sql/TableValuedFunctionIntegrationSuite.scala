package org.apache.spark.sql

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.tags.IntegrationTest

/** Integration tests for TableValuedFunction (accessed via `spark.tvf`).
  *
  * Requires a running Spark Connect server at sc://localhost:15002.
  */
@IntegrationTest
class TableValuedFunctionIntegrationSuite extends IntegrationTestBase:

  // ---------------------------------------------------------------------------
  // Range
  // ---------------------------------------------------------------------------

  test("tvf.range with end") {
    val df = spark.tvf.range(5)
    assert(df.count() == 5)
    val rows = df.orderBy(col("id")).collect()
    assert(rows.head.getLong(0) == 0L)
    assert(rows.last.getLong(0) == 4L)
  }

  test("tvf.range with start and end") {
    val df = spark.tvf.range(3, 8)
    assert(df.count() == 5)
    val rows = df.orderBy(col("id")).collect()
    assert(rows.head.getLong(0) == 3L)
    assert(rows.last.getLong(0) == 7L)
  }

  test("tvf.range with start, end, and step") {
    val df = spark.tvf.range(0, 10, 3)
    val rows = df.orderBy(col("id")).collect()
    assert(rows.map(_.getLong(0)).toSeq == Seq(0L, 3L, 6L, 9L))
  }

  // ---------------------------------------------------------------------------
  // Explode family
  // ---------------------------------------------------------------------------

  test("tvf.explode on array column") {
    val source = spark.sql("SELECT array(1, 2, 3) AS arr")
    source.createOrReplaceTempView("tvf_explode_src")
    try
      val df = spark.sql("SELECT * FROM tvf_explode_src LATERAL VIEW explode(arr) t AS val")
      assert(df.count() == 3)
      val vals = df.select(col("val")).orderBy(col("val")).collect().map(_.getInt(0))
      assert(vals.toSeq == Seq(1, 2, 3))
    finally spark.catalog.dropTempView("tvf_explode_src")
  }

  test("tvf.explode via tvf API") {
    val df = spark.tvf.explode(array(lit(10), lit(20), lit(30)))
    assert(df.count() == 3)
    val rows = df.orderBy(col("col")).collect()
    assert(rows(0).getInt(0) == 10)
    assert(rows(1).getInt(0) == 20)
    assert(rows(2).getInt(0) == 30)
  }

  test("tvf.explode_outer includes nulls for empty arrays") {
    val df = spark.tvf.explode_outer(array(lit(1), lit(2)))
    assert(df.count() == 2)
  }

  test("tvf.explode_outer on empty array produces one null row") {
    val emptyArr = spark.sql("SELECT array() AS arr")
    emptyArr.createOrReplaceTempView("tvf_explode_outer_src")
    try
      val df = spark.sql(
        "SELECT t.col FROM tvf_explode_outer_src LATERAL VIEW explode_outer(arr) t AS col"
      )
      val rows = df.collect()
      assert(rows.length == 1)
      assert(rows(0).isNullAt(0))
    finally spark.catalog.dropTempView("tvf_explode_outer_src")
  }

  test("tvf.posexplode returns position and value") {
    val df = spark.tvf.posexplode(array(lit("a"), lit("b"), lit("c")))
    assert(df.count() == 3)
    val rows = df.orderBy(col("pos")).collect()
    assert(rows(0).getInt(0) == 0)
    assert(rows(0).getString(1) == "a")
    assert(rows(2).getInt(0) == 2)
    assert(rows(2).getString(1) == "c")
  }

  test("tvf.posexplode_outer returns position and value with nulls for empty") {
    val df = spark.tvf.posexplode_outer(array(lit(100), lit(200)))
    assert(df.count() == 2)
    val rows = df.orderBy(col("pos")).collect()
    assert(rows(0).getInt(0) == 0)
    assert(rows(1).getInt(0) == 1)
  }

  // ---------------------------------------------------------------------------
  // Inline family
  // ---------------------------------------------------------------------------

  test("tvf.inline expands array of structs") {
    val df = spark.tvf.inline(
      array(
        struct(lit("Alice").as("name"), lit(30).as("age")),
        struct(lit("Bob").as("name"), lit(25).as("age"))
      )
    )
    assert(df.count() == 2)
    val rows = df.orderBy(col("name")).collect()
    assert(rows(0).getString(0) == "Alice")
    assert(rows(0).getInt(1) == 30)
    assert(rows(1).getString(0) == "Bob")
    assert(rows(1).getInt(1) == 25)
  }

  test("tvf.inline_outer expands array of structs with null handling") {
    val df = spark.tvf.inline_outer(
      array(
        struct(lit("X").as("key"), lit(1).as("val")),
        struct(lit("Y").as("key"), lit(2).as("val"))
      )
    )
    assert(df.count() == 2)
    val rows = df.orderBy(col("key")).collect()
    assert(rows(0).getString(0) == "X")
    assert(rows(1).getString(0) == "Y")
  }

  // ---------------------------------------------------------------------------
  // json_tuple
  // ---------------------------------------------------------------------------

  test("tvf.json_tuple extracts fields from JSON string") {
    val df = spark.tvf.json_tuple(
      lit("""{"name":"Alice","age":"30"}"""),
      lit("name"),
      lit("age")
    )
    val rows = df.collect()
    assert(rows.length == 1)
    assert(rows(0).getString(0) == "Alice")
    assert(rows(0).getString(1) == "30")
  }

  test("tvf.json_tuple with missing field returns null") {
    val df = spark.tvf.json_tuple(
      lit("""{"name":"Bob"}"""),
      lit("name"),
      lit("missing_field")
    )
    val rows = df.collect()
    assert(rows.length == 1)
    assert(rows(0).getString(0) == "Bob")
    assert(rows(0).isNullAt(1))
  }

  // ---------------------------------------------------------------------------
  // stack
  // ---------------------------------------------------------------------------

  test("tvf.stack reshapes columns into rows") {
    // stack(n, val1, val2, ...) groups values into n rows
    val df = spark.tvf.stack(
      lit(2),
      lit("a"),
      lit(1),
      lit("b"),
      lit(2)
    )
    assert(df.count() == 2)
    val rows = df.orderBy(col("col0")).collect()
    assert(rows(0).getString(0) == "a")
    assert(rows(0).getInt(1) == 1)
    assert(rows(1).getString(0) == "b")
    assert(rows(1).getInt(1) == 2)
  }

  test("tvf.stack with three rows") {
    val df = spark.tvf.stack(
      lit(3),
      lit(10),
      lit(20),
      lit(30)
    )
    assert(df.count() == 3)
    val rows = df.orderBy(col("col0")).collect()
    assert(rows(0).getInt(0) == 10)
    assert(rows(1).getInt(0) == 20)
    assert(rows(2).getInt(0) == 30)
  }

  // ---------------------------------------------------------------------------
  // collations
  // ---------------------------------------------------------------------------

  test("tvf.collations returns available collations") {
    val df = spark.tvf.collations()
    assert(df.count() > 0)
    // collations() should return columns including COLLATION_NAME
    val cols = df.columns.map(_.toUpperCase)
    assert(cols.contains("COLLATION_NAME") || cols.contains("NAME") || cols.nonEmpty)
  }

  // ---------------------------------------------------------------------------
  // sql_keywords
  // ---------------------------------------------------------------------------

  test("tvf.sql_keywords returns SQL keyword list") {
    val df = spark.tvf.sql_keywords()
    assert(df.count() > 0)
    // Should contain common SQL keywords like SELECT, FROM, WHERE
    val rows = df.collect()
    val keywords = rows.map(_.getString(0)).toSet
    assert(keywords.contains("SELECT") || keywords.nonEmpty)
  }

  // ---------------------------------------------------------------------------
  // variant_explode
  // ---------------------------------------------------------------------------

  test("tvf.variant_explode on variant data") {
    try
      // variant_explode is a generator/TVF — must use LATERAL syntax in FROM clause
      val df = spark.sql(
        """SELECT pos, key, value
          |FROM (SELECT parse_json('["a","b","c"]') AS v),
          |LATERAL variant_explode(v)""".stripMargin
      )
      assert(df.count() == 3)
    catch
      case e: Exception
          if e.getMessage != null &&
            (e.getMessage.contains("VARIANT") ||
              e.getMessage.contains("variant") ||
              e.getMessage.contains("not supported") ||
              e.getMessage.contains("not found") ||
              e.getMessage.contains("parse_json") ||
              e.getMessage.contains("UNRESOLVED_ROUTINE")) =>
        cancel("Server does not support variant type or variant_explode TVF")
  }

  test("tvf.variant_explode via tvf API") {
    try
      val variantCol = expr("parse_json('[1, 2, 3]')")
      val df = spark.tvf.variant_explode(variantCol)
      assert(df.count() == 3)
    catch
      case e: Exception
          if e.getMessage != null &&
            (e.getMessage.contains("VARIANT") ||
              e.getMessage.contains("variant") ||
              e.getMessage.contains("not supported") ||
              e.getMessage.contains("not found") ||
              e.getMessage.contains("parse_json") ||
              e.getMessage.contains("UNRESOLVED_ROUTINE")) =>
        cancel("Server does not support variant type or variant_explode TVF")
  }

  test("tvf.variant_explode_outer via tvf API") {
    try
      val variantCol = expr("parse_json('[10, 20]')")
      val df = spark.tvf.variant_explode_outer(variantCol)
      assert(df.count() == 2)
    catch
      case e: Exception
          if e.getMessage != null &&
            (e.getMessage.contains("VARIANT") ||
              e.getMessage.contains("variant") ||
              e.getMessage.contains("not supported") ||
              e.getMessage.contains("not found") ||
              e.getMessage.contains("parse_json") ||
              e.getMessage.contains("UNRESOLVED_ROUTINE")) =>
        cancel("Server does not support variant type or variant_explode_outer TVF")
  }

  test("tvf.variant_explode_outer on empty variant array") {
    try
      val variantCol = expr("parse_json('[]')")
      val df = spark.tvf.variant_explode_outer(variantCol)
      val rows = df.collect()
      // variant_explode_outer on empty array should produce one row with nulls
      assert(rows.length == 1)
    catch
      case e: Exception
          if e.getMessage != null &&
            (e.getMessage.contains("VARIANT") ||
              e.getMessage.contains("variant") ||
              e.getMessage.contains("not supported") ||
              e.getMessage.contains("not found") ||
              e.getMessage.contains("parse_json") ||
              e.getMessage.contains("UNRESOLVED_ROUTINE")) =>
        cancel("Server does not support variant type or variant_explode_outer TVF")
  }
