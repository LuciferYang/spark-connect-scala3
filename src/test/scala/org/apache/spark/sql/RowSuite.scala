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

  test("copy returns the same instance because Row is immutable (R5/R27)") {
    val original = Row(1, "hello", 3.14)
    val copied = original.copy()
    copied shouldBe original
    // Row is immutable, so copy() is the identity operation — matching upstream
    // GenericRowWithSchema.copy() semantics.
    copied should be theSameInstanceAs original
  }

  test("copy preserves schema so by-name access still works on the copy (R5/R27)") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("name", StringType)
    ))
    val original = Row.fromSeqWithSchema(Seq(7, "alice"), schema)
    val copied = original.copy()
    copied.getAs[String]("name") shouldBe "alice"
    copied.fieldIndex("id") shouldBe 0
    copied.json should include("alice")
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

  // ---------------------------------------------------------------------------
  // Spatial type getters
  // ---------------------------------------------------------------------------

  test("Row.getGeometry returns Geometry value") {
    val geom = Geometry.fromWKB(Array[Byte](1, 2, 3), 4326)
    val row = Row(geom)
    row.getGeometry(0) shouldBe geom
    row.getGeometry(0).getSrid shouldBe 4326
    row.getGeometry(0).getBytes shouldBe Array[Byte](1, 2, 3)
  }

  test("Row.getGeography returns Geography value") {
    val geog = Geography.fromWKB(Array[Byte](4, 5, 6), 4326)
    val row = Row(geog)
    row.getGeography(0) shouldBe geog
    row.getGeography(0).getSrid shouldBe 4326
    row.getGeography(0).getBytes shouldBe Array[Byte](4, 5, 6)
  }

  // ---------------------------------------------------------------------------
  // R15: primitive accessors raise a uniform NPE on null rather than silently
  // unboxing Boolean → false (Scala primitive default) or NPE'ing inside the
  // numeric Number cast with no field context.
  // ---------------------------------------------------------------------------

  private def expectNpeAt(i: Int)(thunk: => Any): Unit =
    val ex = intercept[NullPointerException](thunk)
    ex.getMessage should include(s"index $i")
    ex.getMessage should include("null")

  test("Row.getBoolean throws NPE on null instead of silently returning false (R15)") {
    val row = Row(null)
    expectNpeAt(0)(row.getBoolean(0))
  }

  test("Row.getByte throws NPE with field context on null (R15)") {
    val row = Row(null)
    expectNpeAt(0)(row.getByte(0))
  }

  test("Row.getShort throws NPE with field context on null (R15)") {
    val row = Row(null)
    expectNpeAt(0)(row.getShort(0))
  }

  test("Row.getInt throws NPE with field context on null (R15)") {
    val row = Row(null)
    expectNpeAt(0)(row.getInt(0))
  }

  test("Row.getLong throws NPE with field context on null (R15)") {
    val row = Row(null)
    expectNpeAt(0)(row.getLong(0))
  }

  test("Row.getFloat throws NPE with field context on null (R15)") {
    val row = Row(null)
    expectNpeAt(0)(row.getFloat(0))
  }

  test("Row.getDouble throws NPE with field context on null (R15)") {
    val row = Row(null)
    expectNpeAt(0)(row.getDouble(0))
  }

  // ---------------------------------------------------------------------------
  // R7: equals/hashCode handle Array[Byte] and other arrays by content
  // ---------------------------------------------------------------------------

  test("Row.equals returns true for content-equal Array[Byte] fields (R7)") {
    val a = Row(Array[Byte](1, 2, 3))
    val b = Row(Array[Byte](1, 2, 3))
    a shouldBe b
  }

  test("Row.equals returns false for differing Array[Byte] fields (R7)") {
    val a = Row(Array[Byte](1, 2, 3))
    val b = Row(Array[Byte](1, 2, 4))
    a should not be b
  }

  test("Row.hashCode matches for content-equal Array[Byte] fields (R7)") {
    val a = Row(Array[Byte](9, 8, 7))
    val b = Row(Array[Byte](9, 8, 7))
    a.hashCode shouldBe b.hashCode
  }

  test("Row works as a Set element with binary fields (R7)") {
    val a = Row("k", Array[Byte](1, 2))
    val b = Row("k", Array[Byte](1, 2))
    val set = Set(a)
    set.contains(b) shouldBe true
  }

  test("Row.equals recurses into nested Row (R7)") {
    val inner1 = Row(Array[Byte](1, 2))
    val inner2 = Row(Array[Byte](1, 2))
    Row("x", inner1) shouldBe Row("x", inner2)
  }

  test("Row.equals handles null array fields (R7)") {
    val a = Row(null.asInstanceOf[Array[Byte]])
    val b = Row(null.asInstanceOf[Array[Byte]])
    a shouldBe b
  }

  // ---------------------------------------------------------------------------
  // R9: type-aware json/prettyJson encoding for non-String values
  // ---------------------------------------------------------------------------

  test("json quotes Date as ISO-8601 (R9)") {
    val schema = StructType(Seq(StructField("d", DateType)))
    val row = Row.fromSeqWithSchema(Seq(java.sql.Date.valueOf("2024-06-15")), schema)
    row.json shouldBe """{"d":"2024-06-15"}"""
  }

  test("json quotes Timestamp as ISO-8601 instant (R9)") {
    val schema = StructType(Seq(StructField("ts", TimestampType)))
    val ts = java.sql.Timestamp.from(java.time.Instant.parse("2024-01-15T10:30:00Z"))
    val row = Row.fromSeqWithSchema(Seq(ts), schema)
    row.json shouldBe """{"ts":"2024-01-15T10:30:00Z"}"""
  }

  test("json base64-encodes Array[Byte] (R9)") {
    val schema = StructType(Seq(StructField("b", BinaryType)))
    val row = Row.fromSeqWithSchema(Seq(Array[Byte](1, 2, 3)), schema)
    row.json shouldBe """{"b":"AQID"}"""
  }

  test("json recurses into nested Row (R9)") {
    val innerSchema = StructType(Seq(StructField("name", StringType)))
    val outerSchema = StructType(Seq(StructField("inner", innerSchema)))
    val inner = Row.fromSeqWithSchema(Seq("alice"), innerSchema)
    val outer = Row.fromSeqWithSchema(Seq(inner), outerSchema)
    outer.json shouldBe """{"inner":{"name":"alice"}}"""
  }

  test("json renders Seq as JSON array (R9)") {
    val schema = StructType(Seq(
      StructField("a", ArrayType(IntegerType, containsNull = true))
    ))
    val row = Row.fromSeqWithSchema(Seq(Seq(1, 2, 3)), schema)
    row.json shouldBe """{"a":[1,2,3]}"""
  }

  test("json renders Map as JSON object (R9)") {
    val schema = StructType(Seq(
      StructField("m", MapType(StringType, IntegerType, valueContainsNull = true))
    ))
    val row = Row.fromSeqWithSchema(Seq(Map("x" -> 1)), schema)
    row.json shouldBe """{"m":{"x":1}}"""
  }

  test("json keeps BigDecimal as plain number (R9)") {
    val schema = StructType(Seq(StructField("bd", DecimalType(10, 2))))
    val row = Row.fromSeqWithSchema(Seq(new java.math.BigDecimal("1234567890.12")), schema)
    row.json shouldBe """{"bd":1234567890.12}"""
  }

  // ---------------------------------------------------------------------------
  // R19: fromSeqWithSchema rejects size mismatches at construction time
  // ---------------------------------------------------------------------------

  test("Row.fromSeqWithSchema rejects too-many values (R19)") {
    val schema = StructType(Seq(StructField("a", IntegerType), StructField("b", StringType)))
    an[IllegalArgumentException] should be thrownBy
      Row.fromSeqWithSchema(Seq(1, "x", "extra"), schema)
  }

  test("Row.fromSeqWithSchema rejects too-few values (R19)") {
    val schema = StructType(Seq(StructField("a", IntegerType), StructField("b", StringType)))
    an[IllegalArgumentException] should be thrownBy
      Row.fromSeqWithSchema(Seq(1), schema)
  }
