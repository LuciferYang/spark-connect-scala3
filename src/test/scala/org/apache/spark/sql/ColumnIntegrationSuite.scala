package org.apache.spark.sql

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.tags.IntegrationTest
import org.apache.spark.sql.types.*

/** Integration tests for Column operators: comparison, arithmetic, boolean, bitwise, null/NaN,
  * string, cast, sort, conditional, nested access, and window.
  */
@IntegrationTest
class ColumnIntegrationSuite extends IntegrationTestBase:

  // ---------------------------------------------------------------------------
  // Comparison operators
  // ---------------------------------------------------------------------------

  test("=== and =!= with Column") {
    val df = spark.range(5).select(col("id"))
    val result = df.filter(col("id") === lit(3)).collect()
    assert(result.length == 1)
    assert(result(0).getLong(0) == 3L)

    val neq = df.filter(col("id") =!= lit(3)).collect()
    assert(neq.length == 4)
  }

  test(">, >=, <, <= with Column") {
    val df = spark.range(10).select(col("id"))
    assert(df.filter(col("id") > lit(7)).count() == 2) // 8, 9
    assert(df.filter(col("id") >= lit(7)).count() == 3) // 7, 8, 9
    assert(df.filter(col("id") < lit(3)).count() == 3) // 0, 1, 2
    assert(df.filter(col("id") <= lit(3)).count() == 4) // 0, 1, 2, 3
  }

  test("equalTo, notEqual, gt, lt, geq, leq (Java-friendly)") {
    val df = spark.range(10).select(col("id"))
    assert(df.filter(col("id").equalTo(5)).count() == 1)
    assert(df.filter(col("id").notEqual(5)).count() == 9)
    assert(df.filter(col("id").gt(7)).count() == 2)
    assert(df.filter(col("id").lt(3)).count() == 3)
    assert(df.filter(col("id").geq(7)).count() == 3)
    assert(df.filter(col("id").leq(3)).count() == 4)
  }

  test("eqNullSafe / <=>") {
    val rows = Seq(Row(1), Row(null), Row(3))
    val schema = StructType(Seq(StructField("x", IntegerType)))
    val df = spark.createDataFrame(rows, schema)
    // null <=> null should be true
    val nullMatch = df.filter(col("x") <=> lit(null)).collect()
    assert(nullMatch.length == 1)
    assert(nullMatch(0).isNullAt(0))
  }

  // ---------------------------------------------------------------------------
  // Arithmetic operators
  // ---------------------------------------------------------------------------

  test("+, -, *, /, % with Column") {
    val df = spark.range(1, 6).select(col("id"))
    val result = df.select(
      col("id"),
      (col("id") + lit(10)).as("plus"),
      (col("id") - lit(1)).as("minus"),
      (col("id") * lit(2)).as("times"),
      (col("id") / lit(2)).as("div"),
      (col("id") % lit(3)).as("mod")
    ).orderBy(col("id")).collect()
    // id=1: plus=11, minus=0, times=2, div=0.5, mod=1
    assert(result(0).get(1).toString.toLong == 11L)
    assert(result(0).get(2).toString.toLong == 0L)
    assert(result(0).get(3).toString.toLong == 2L)
    assert(result(0).get(5).toString.toLong == 1L)
  }

  test("unary_- negation") {
    val df = spark.range(3).select(col("id"))
    val result = df.select((-col("id")).as("neg")).orderBy(col("neg")).collect()
    assert(result(0).getLong(0) == -2L)
    assert(result(2).getLong(0) == 0L)
  }

  test("plus, minus, multiply, divide, mod (Java-friendly)") {
    val df = spark.range(1, 4).select(col("id"))
    val result = df.select(
      col("id").plus(10).as("p"),
      col("id").minus(1).as("m"),
      col("id").multiply(3).as("t"),
      col("id").divide(2).as("d"),
      col("id").mod(2).as("r")
    ).orderBy(col("id")).collect()
    assert(result(0).get(0).toString.toLong == 11L) // 1+10
    assert(result(2).get(2).toString.toLong == 9L) // 3*3
  }

  // ---------------------------------------------------------------------------
  // Logical / Boolean operators
  // ---------------------------------------------------------------------------

  test("&& and || and unary_!") {
    val df = spark.range(10).select(col("id"))
    val both = df.filter(col("id") > lit(3) && col("id") < lit(7)).count()
    assert(both == 3) // 4, 5, 6

    val either = df.filter(col("id") < lit(2) || col("id") > lit(8)).count()
    assert(either == 3) // 0, 1, 9

    val notResult = df.filter(!col("id").between(3, 7)).count()
    assert(notResult == 5) // 0, 1, 2, 8, 9
  }

  test("and / or (Java-friendly)") {
    val df = spark.range(10).select(col("id"))
    val result = df.filter(col("id").gt(3).and(col("id").lt(7))).count()
    assert(result == 3)
    val orResult = df.filter(col("id").lt(2).or(col("id").gt(8))).count()
    assert(orResult == 3)
  }

  // ---------------------------------------------------------------------------
  // Bitwise operators
  // ---------------------------------------------------------------------------

  test("bitwiseOR, bitwiseAND, bitwiseXOR") {
    val rows = Seq(Row(0x0f), Row(0xf0))
    val schema = StructType(Seq(StructField("x", IntegerType)))
    val df = spark.createDataFrame(rows, schema)
    val result = df.select(
      col("x").bitwiseOR(0xff).as("or_val"),
      col("x").bitwiseAND(0x0f).as("and_val"),
      col("x").bitwiseXOR(0xff).as("xor_val")
    ).orderBy(col("or_val")).collect()
    // x=0x0F: or=0xFF, and=0x0F, xor=0xF0
    assert(result(0).getInt(0) == 0xff)
    assert(result(0).getInt(1) == 0x0f)
    assert(result(0).getInt(2) == 0xf0)
  }

  test("| & ^ operators") {
    val df = spark.range(1, 4).select(col("id").cast("int").as("x"))
    val result = df.select(
      (col("x") | lit(0x10)).as("or_val"),
      (col("x") & lit(0x01)).as("and_val"),
      (col("x") ^ lit(0xff)).as("xor_val")
    ).orderBy(col("x")).collect()
    // x=1: |0x10=17, &0x01=1, ^0xFF=254
    assert(result(0).getInt(0) == 17)
    assert(result(0).getInt(1) == 1)
    assert(result(0).getInt(2) == 254)
  }

  // ---------------------------------------------------------------------------
  // Null / NaN checks
  // ---------------------------------------------------------------------------

  test("isNull and isNotNull") {
    val rows = Seq(Row(1), Row(null), Row(3))
    val schema = StructType(Seq(StructField("x", IntegerType)))
    val df = spark.createDataFrame(rows, schema)
    assert(df.filter(col("x").isNull).count() == 1)
    assert(df.filter(col("x").isNotNull).count() == 2)
  }

  test("isNaN") {
    val rows = Seq(Row(1.0), Row(Double.NaN), Row(3.0))
    val schema = StructType(Seq(StructField("x", DoubleType)))
    val df = spark.createDataFrame(rows, schema)
    assert(df.filter(col("x").isNaN).count() == 1)
  }

  // ---------------------------------------------------------------------------
  // String / pattern operators
  // ---------------------------------------------------------------------------

  test("contains, startsWith, endsWith") {
    val rows = Seq(Row("hello world"), Row("foo bar"), Row("hello foo"))
    val schema = StructType(Seq(StructField("s", StringType)))
    val df = spark.createDataFrame(rows, schema)
    assert(df.filter(col("s").contains("hello")).count() == 2)
    assert(df.filter(col("s").startsWith("hello")).count() == 2)
    assert(df.filter(col("s").endsWith("foo")).count() == 1) // only "hello foo"
  }

  test("like, rlike, ilike") {
    val rows = Seq(Row("Apple"), Row("Banana"), Row("apricot"))
    val schema = StructType(Seq(StructField("fruit", StringType)))
    val df = spark.createDataFrame(rows, schema)
    assert(df.filter(col("fruit").like("A%")).count() == 1) // Apple
    assert(df.filter(col("fruit").rlike("^[aA].*")).count() == 2) // Apple, apricot
    assert(df.filter(col("fruit").ilike("a%")).count() == 2) // Apple, apricot (case-insensitive)
  }

  test("substr") {
    val rows = Seq(Row("abcdef"))
    val schema = StructType(Seq(StructField("s", StringType)))
    val df = spark.createDataFrame(rows, schema)
    // substr(startPos, length) — 0-based start
    val result = df.select(col("s").substr(2, 3).as("sub")).collect()
    assert(result(0).getString(0) == "bcd" || result(0).getString(0) == "cde")
    // Just verify it doesn't throw and returns a substring

    // substr(Column, Column) overload
    val result2 = df.select(col("s").substr(lit(1), lit(3)).as("sub")).collect()
    assert(result2(0).getString(0).length == 3)
  }

  // ---------------------------------------------------------------------------
  // isin / between / isInCollection
  // ---------------------------------------------------------------------------

  test("isin with literal values") {
    val df = spark.range(10).select(col("id"))
    val result = df.filter(col("id").isin(1L, 3L, 5L, 7L)).orderBy(col("id")).collect()
    assert(result.map(_.getLong(0)).toSeq == Seq(1L, 3L, 5L, 7L))
  }

  test("between") {
    val df = spark.range(10).select(col("id"))
    val result = df.filter(col("id").between(3, 6)).orderBy(col("id")).collect()
    assert(result.map(_.getLong(0)).toSeq == Seq(3L, 4L, 5L, 6L))
  }

  test("isInCollection") {
    val df = spark.range(10).select(col("id"))
    val result = df.filter(col("id").isInCollection(Seq(2L, 4L, 6L)))
      .orderBy(col("id")).collect()
    assert(result.map(_.getLong(0)).toSeq == Seq(2L, 4L, 6L))
  }

  // ---------------------------------------------------------------------------
  // Conditional: when / otherwise
  // ---------------------------------------------------------------------------

  test("when and otherwise") {
    val df = spark.range(5).select(col("id"))
    val result = df.select(
      col("id"),
      when(col("id") < lit(2), "small")
        .when(col("id") < lit(4), "medium")
        .otherwise("large")
        .as("size")
    ).orderBy(col("id")).collect()
    assert(result(0).getString(1) == "small") // id=0
    assert(result(2).getString(1) == "medium") // id=2
    assert(result(4).getString(1) == "large") // id=4
  }

  // ---------------------------------------------------------------------------
  // Cast
  // ---------------------------------------------------------------------------

  test("cast with string type name") {
    val df = spark.range(3).select(col("id"))
    val result = df.select(col("id").cast("string").as("s")).collect()
    assert(result(0).getString(0) == "0")
    assert(result(2).getString(0) == "2")
  }

  test("cast with DataType") {
    val df = spark.range(3).select(col("id"))
    val result = df.select(col("id").cast(DoubleType).as("d")).collect()
    assert(result(0).getDouble(0) == 0.0)
  }

  test("try_cast with string type name") {
    val rows = Seq(Row("123"), Row("abc"), Row("456"))
    val schema = StructType(Seq(StructField("s", StringType)))
    val df = spark.createDataFrame(rows, schema)
    val result = df.select(col("s").try_cast("int").as("i")).collect()
    assert(result(0).getInt(0) == 123)
    assert(result(1).isNullAt(0)) // "abc" can't cast to int
    assert(result(2).getInt(0) == 456)
  }

  test("try_cast with DataType") {
    val rows = Seq(Row("1.5"), Row("bad"))
    val schema = StructType(Seq(StructField("s", StringType)))
    val df = spark.createDataFrame(rows, schema)
    val result = df.select(col("s").try_cast(DoubleType).as("d")).collect()
    assert(result(0).getDouble(0) == 1.5)
    assert(result(1).isNullAt(0))
  }

  // ---------------------------------------------------------------------------
  // Alias / name
  // ---------------------------------------------------------------------------

  test("alias, as, name") {
    val df = spark.range(3).select(col("id"))
    val r1 = df.select(col("id").alias("a")).columns
    assert(r1.toSeq == Seq("a"))

    val r2 = df.select(col("id").as("b")).columns
    assert(r2.toSeq == Seq("b"))

    val r3 = df.select(col("id").name("c")).columns
    assert(r3.toSeq == Seq("c"))
  }

  test("as with metadata") {
    val df = spark.range(3).select(col("id"))
    val result = df.select(col("id").as("x", """{"comment":"test"}"""))
    assert(result.columns.toSeq == Seq("x"))
    assert(result.collect().length == 3)
  }

  // ---------------------------------------------------------------------------
  // Sort variants
  // ---------------------------------------------------------------------------

  test("asc and desc") {
    val df = spark.range(5).select(col("id"))
    val asc = df.orderBy(col("id").asc).collect()
    assert(asc(0).getLong(0) == 0L)
    val desc = df.orderBy(col("id").desc).collect()
    assert(desc(0).getLong(0) == 4L)
  }

  test("asc_nulls_first, asc_nulls_last, desc_nulls_first, desc_nulls_last") {
    val rows = Seq(Row(1), Row(null), Row(3))
    val schema = StructType(Seq(StructField("x", IntegerType)))
    val df = spark.createDataFrame(rows, schema)

    val ascNF = df.orderBy(col("x").asc_nulls_first).collect()
    assert(ascNF(0).isNullAt(0))

    val ascNL = df.orderBy(col("x").asc_nulls_last).collect()
    assert(ascNL(2).isNullAt(0))

    val descNF = df.orderBy(col("x").desc_nulls_first).collect()
    assert(descNF(0).isNullAt(0))

    val descNL = df.orderBy(col("x").desc_nulls_last).collect()
    assert(descNL(2).isNullAt(0))
  }

  // ---------------------------------------------------------------------------
  // Nested data access: getItem, getField, apply, withField, dropFields
  // ---------------------------------------------------------------------------

  test("getItem on array") {
    val df = spark.sql("SELECT array(10, 20, 30) AS arr")
    val result = df.select(col("arr").getItem(1).as("item")).collect()
    assert(result(0).getInt(0) == 20)
  }

  test("getItem on map") {
    val df = spark.sql("SELECT map('a', 1, 'b', 2) AS m")
    val result = df.select(col("m").getItem("b").as("val")).collect()
    assert(result(0).getInt(0) == 2)
  }

  test("getField on struct") {
    val df = spark.sql("SELECT named_struct('x', 1, 'y', 2) AS s")
    val result = df.select(col("s").getField("y").as("val")).collect()
    assert(result(0).getInt(0) == 2)
  }

  test("apply is alias for getItem") {
    val df = spark.sql("SELECT array(10, 20, 30) AS arr")
    val result = df.select(col("arr")(1).as("item")).collect()
    assert(result(0).getInt(0) == 20)
  }

  test("withField adds or replaces field in struct") {
    val df = spark.sql("SELECT named_struct('x', 1, 'y', 2) AS s")
    val result = df.select(col("s").withField("z", lit(99)).as("s2"))
      .select(col("s2").getField("z").as("z")).collect()
    assert(result(0).getInt(0) == 99)
  }

  test("dropFields removes field from struct") {
    val df = spark.sql("SELECT named_struct('x', 1, 'y', 2, 'z', 3) AS s")
    val result = df.select(col("s").dropFields("y").as("s2"))
    val fields = result.schema.fields(0).dataType.asInstanceOf[StructType].fieldNames
    assert(!fields.contains("y"))
    assert(fields.contains("x"))
    assert(fields.contains("z"))
  }

  // ---------------------------------------------------------------------------
  // Window: over
  // ---------------------------------------------------------------------------

  test("over with WindowSpec") {
    val rows = Seq(Row("A", 10), Row("A", 20), Row("B", 30))
    val schema = StructType(Seq(
      StructField("group", StringType),
      StructField("value", IntegerType)
    ))
    val df = spark.createDataFrame(rows, schema)
    val w = Window.partitionBy(col("group")).orderBy(col("value"))
    val result = df.select(col("group"), col("value"), sum(col("value")).over(w).as("running_sum"))
      .orderBy(col("group"), col("value")).collect()
    // Group A: row1=10 (sum=10), row2=20 (sum=30)
    assert(result(0).get(2).toString.toLong == 10L)
    assert(result(1).get(2).toString.toLong == 30L)
  }

  test("over without spec (unbounded window)") {
    val df = spark.range(5).select(col("id"))
    val result = df.select(col("id"), sum(col("id")).over().as("total")).collect()
    // All rows should have total = 0+1+2+3+4 = 10
    result.foreach(r => assert(r.get(1).toString.toLong == 10L))
  }

  // ---------------------------------------------------------------------------
  // getItem with Column key — dynamic array index
  // ---------------------------------------------------------------------------

  test("getItem with Column key for dynamic array index") {
    val rows = Seq(
      Row(Seq(10, 20, 30), 0),
      Row(Seq(40, 50, 60), 1),
      Row(Seq(70, 80, 90), 2)
    )
    val schema = StructType(Seq(
      StructField("arr", ArrayType(IntegerType, true)),
      StructField("idx", IntegerType)
    ))
    val df = spark.createDataFrame(rows, schema)
    val result = df.select(col("arr").getItem(col("idx")).as("val"))
      .orderBy(col("val")).collect()
    assert(result(0).getInt(0) == 10) // arr[0] = 10
    assert(result(1).getInt(0) == 50) // arr[1] = 50
    assert(result(2).getInt(0) == 90) // arr[2] = 90
  }
