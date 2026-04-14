package org.apache.spark.sql

import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RowSuite extends AnyFunSuite with Matchers:

  test("Row.apply and basic accessors") {
    val row = Row("hello", 42, 3.14, true, null)
    row.size shouldBe 5
    row.length shouldBe 5
    row.getString(0) shouldBe "hello"
    row.getInt(1) shouldBe 42
    row.getDouble(2) shouldBe 3.14
    row.getBoolean(3) shouldBe true
    row.isNullAt(4) shouldBe true
    row.isNullAt(0) shouldBe false
  }

  test("Row.fromSeq") {
    val row = Row.fromSeq(Seq(1, 2, 3))
    row.size shouldBe 3
    row.getInt(0) shouldBe 1
    row.getInt(2) shouldBe 3
  }

  test("Row.empty") {
    Row.empty.size shouldBe 0
  }

  test("Row.toSeq") {
    val values = Seq("a", 1, null)
    Row.fromSeq(values).toSeq shouldBe values
  }

  test("Row.toString") {
    Row("a", 1, null).toString shouldBe "[a,1,null]"
  }

  test("Row.mkString") {
    Row("a", "b", "c").mkString("|") shouldBe "a|b|c"
  }

  test("Row equality") {
    Row(1, "a") shouldBe Row(1, "a")
    Row(1, "a") should not be Row(1, "b")
  }

  test("Row.getAs") {
    val row = Row("hello", 42L)
    row.getAs[String](0) shouldBe "hello"
    row.getAs[Long](1) shouldBe 42L
  }

  test("numeric conversions via Number") {
    val row = Row(42.asInstanceOf[Any])
    row.getLong(0) shouldBe 42L
    row.getShort(0) shouldBe 42.toShort
    row.getByte(0) shouldBe 42.toByte
    row.getFloat(0) shouldBe 42.0f
  }

  test("Row.fromSeqWithSchema") {
    val schema = StructType(
      Seq(
        StructField("name", StringType),
        StructField("age", IntegerType)
      )
    )
    val row = Row.fromSeqWithSchema(Seq("Alice", 30), schema)
    row.schema shouldBe Some(schema)
    row.getString(0) shouldBe "Alice"
    row.getInt(1) shouldBe 30
  }

  test("Row.getValuesMap with schema") {
    val schema = StructType(
      Seq(
        StructField("count", LongType),
        StructField("max_id", IntegerType),
        StructField("label", StringType)
      )
    )
    val row = Row.fromSeqWithSchema(Seq(100L, 42, "test"), schema)
    val result = row.getValuesMap[Any](Seq("count", "label"))
    result shouldBe Map("count" -> 100L, "label" -> "test")
  }

  test("Row.getValuesMap without schema throws") {
    val row = Row(1, 2, 3)
    an[UnsupportedOperationException] should be thrownBy
      row.getValuesMap[Any](Seq("x"))
  }

  test("Row without schema has None schema") {
    Row(1, 2).schema shouldBe None
    Row.empty.schema shouldBe None
    Row.fromSeq(Seq(1)).schema shouldBe None
  }

  // ---------------------------------------------------------------------------
  // getAs by field name
  // ---------------------------------------------------------------------------

  test("getAs[T](fieldName) by name with schema") {
    val schema = StructType(
      Seq(
        StructField("name", StringType),
        StructField("age", IntegerType)
      )
    )
    val row = Row.fromSeqWithSchema(Seq("Alice", 30), schema)
    row.getAs[String]("name") shouldBe "Alice"
    row.getAs[Int]("age") shouldBe 30
  }

  test("getAs[T](fieldName) without schema throws") {
    val row = Row(1, 2, 3)
    an[UnsupportedOperationException] should be thrownBy
      row.getAs[Int]("x")
  }

  // ---------------------------------------------------------------------------
  // Typed getters
  // ---------------------------------------------------------------------------

  test("getDecimal returns BigDecimal") {
    val bd = java.math.BigDecimal.valueOf(3.14)
    val row = Row(bd)
    row.getDecimal(0) shouldBe bd
  }

  test("getDecimal from Scala BigDecimal") {
    val bd = BigDecimal("3.14")
    val row = Row(bd)
    row.getDecimal(0) shouldBe bd.underlying
  }

  test("getDecimal from Number") {
    val row = Row(42.asInstanceOf[Number])
    row.getDecimal(0) shouldBe java.math.BigDecimal.valueOf(42.0)
  }

  test("getDate returns java.sql.Date") {
    val d = java.sql.Date.valueOf("2024-01-15")
    val row = Row(d)
    row.getDate(0) shouldBe d
  }

  test("getTimestamp returns java.sql.Timestamp") {
    val ts = java.sql.Timestamp.valueOf("2024-01-15 10:30:00")
    val row = Row(ts)
    row.getTimestamp(0) shouldBe ts
  }

  test("getInstant returns java.time.Instant") {
    val inst = java.time.Instant.parse("2024-01-15T10:30:00Z")
    val row = Row(inst)
    row.getInstant(0) shouldBe inst
  }

  test("getLocalDate returns java.time.LocalDate") {
    val ld = java.time.LocalDate.of(2024, 1, 15)
    val row = Row(ld)
    row.getLocalDate(0) shouldBe ld
  }

  test("getSeq returns Seq[T]") {
    val seq = Seq(1, 2, 3)
    val row = Row(seq)
    row.getSeq[Int](0) shouldBe seq
  }

  test("getList returns java.util.List") {
    val seq = Seq("a", "b", "c")
    val row = Row(seq)
    val jList = row.getList[String](0)
    jList shouldBe a[java.util.List[?]]
    jList.size() shouldBe 3
    jList.get(0) shouldBe "a"
  }

  test("getMap returns Map[K,V]") {
    val m = Map("a" -> 1, "b" -> 2)
    val row = Row(m)
    row.getMap[String, Int](0) shouldBe m
  }

  test("getJavaMap returns java.util.Map") {
    val m = Map("x" -> 10)
    val row = Row(m)
    val jm = row.getJavaMap[String, Int](0)
    jm shouldBe a[java.util.Map[?, ?]]
    jm.get("x") shouldBe 10
  }

  test("getStruct returns nested Row") {
    val inner = Row("nested", 99)
    val row = Row(inner)
    row.getStruct(0) shouldBe inner
  }

  // ---------------------------------------------------------------------------
  // fieldIndex
  // ---------------------------------------------------------------------------

  test("fieldIndex with schema") {
    val schema = StructType(
      Seq(
        StructField("a", IntegerType),
        StructField("b", StringType),
        StructField("c", DoubleType)
      )
    )
    val row = Row.fromSeqWithSchema(Seq(1, "hello", 3.14), schema)
    row.fieldIndex("a") shouldBe 0
    row.fieldIndex("b") shouldBe 1
    row.fieldIndex("c") shouldBe 2
  }

  test("fieldIndex without schema throws") {
    val row = Row(1, 2, 3)
    an[UnsupportedOperationException] should be thrownBy
      row.fieldIndex("x")
  }

  // ---------------------------------------------------------------------------
  // anyNull
  // ---------------------------------------------------------------------------

  test("anyNull detects null values") {
    Row(1, null, 3).anyNull shouldBe true
    Row(1, 2, 3).anyNull shouldBe false
    Row.empty.anyNull shouldBe false
  }

  // ---------------------------------------------------------------------------
  // json / prettyJson
  // ---------------------------------------------------------------------------

  test("json produces JSON string") {
    val schema = StructType(
      Seq(
        StructField("name", StringType),
        StructField("age", IntegerType)
      )
    )
    val row = Row.fromSeqWithSchema(Seq("Alice", 30), schema)
    row.json shouldBe """{"name":"Alice","age":30}"""
  }

  test("prettyJson produces formatted JSON") {
    val schema = StructType(
      Seq(
        StructField("name", StringType),
        StructField("age", IntegerType)
      )
    )
    val row = Row.fromSeqWithSchema(Seq("Alice", 30), schema)
    val expected = "{\n  \"name\" : \"Alice\",\n  \"age\" : 30\n}"
    row.prettyJson shouldBe expected
  }

  test("json with null values") {
    val schema = StructType(
      Seq(
        StructField("a", StringType),
        StructField("b", IntegerType)
      )
    )
    val row = Row.fromSeqWithSchema(Seq("hello", null), schema)
    row.json shouldBe """{"a":"hello","b":null}"""
  }

  test("json without schema throws") {
    val row = Row(1, 2)
    an[UnsupportedOperationException] should be thrownBy row.json
  }

  test("prettyJson without schema throws") {
    val row = Row(1, 2)
    an[UnsupportedOperationException] should be thrownBy row.prettyJson
  }

  // ---------------------------------------------------------------------------
  // copy
  // ---------------------------------------------------------------------------

  test("copy creates independent Row") {
    val original = Row(1, "hello", 3.14)
    val copied = original.copy()
    copied shouldBe original
    copied should not be theSameInstanceAs(original)
  }

  // ---------------------------------------------------------------------------
  // fromTuple
  // ---------------------------------------------------------------------------

  test("Row.fromTuple creates Row from Product") {
    val row = Row.fromTuple((1, "hello", 3.14))
    row.size shouldBe 3
    row.get(0) shouldBe 1
    row.get(1) shouldBe "hello"
    row.get(2) shouldBe 3.14
  }
