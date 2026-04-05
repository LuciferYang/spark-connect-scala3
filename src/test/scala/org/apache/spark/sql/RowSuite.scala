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
