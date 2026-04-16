package org.apache.spark.sql

import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Proto-only tests for ArrowSerializer — encodes Row sequences into Arrow IPC bytes. */
class ArrowSerializerSuite extends AnyFunSuite with Matchers:

  // ---------------------------------------------------------------------------
  // Empty rows
  // ---------------------------------------------------------------------------

  test("encodeRows with empty rows returns empty byte array") {
    val schema = StructType(Seq(StructField("id", IntegerType)))
    val result = ArrowSerializer.encodeRows(Seq.empty, schema)
    result shouldBe empty
  }

  // ---------------------------------------------------------------------------
  // Primitive types
  // ---------------------------------------------------------------------------

  test("encodeRows with BooleanType") {
    val schema = StructType(Seq(StructField("b", BooleanType)))
    val rows = Seq(Row(true), Row(false))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with ByteType") {
    val schema = StructType(Seq(StructField("b", ByteType)))
    val rows = Seq(Row(1.toByte), Row(127.toByte))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with ShortType") {
    val schema = StructType(Seq(StructField("s", ShortType)))
    val rows = Seq(Row(42.toShort))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with IntegerType") {
    val schema = StructType(Seq(StructField("i", IntegerType)))
    val rows = Seq(Row(1), Row(2), Row(3))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with LongType") {
    val schema = StructType(Seq(StructField("l", LongType)))
    val rows = Seq(Row(100L))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with FloatType") {
    val schema = StructType(Seq(StructField("f", FloatType)))
    val rows = Seq(Row(3.14f))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with DoubleType") {
    val schema = StructType(Seq(StructField("d", DoubleType)))
    val rows = Seq(Row(2.718))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with StringType") {
    val schema = StructType(Seq(StructField("s", StringType)))
    val rows = Seq(Row("hello"), Row("world"))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with BinaryType") {
    val schema = StructType(Seq(StructField("b", BinaryType)))
    val rows = Seq(Row(Array[Byte](1, 2, 3)))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  // ---------------------------------------------------------------------------
  // Date / Timestamp / Decimal
  // ---------------------------------------------------------------------------

  test("encodeRows with DateType using java.sql.Date") {
    val schema = StructType(Seq(StructField("d", DateType)))
    val rows = Seq(Row(java.sql.Date.valueOf("2024-01-15")))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with DateType using LocalDate") {
    val schema = StructType(Seq(StructField("d", DateType)))
    val rows = Seq(Row(java.time.LocalDate.of(2024, 6, 15)))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with DateType using Number") {
    val schema = StructType(Seq(StructField("d", DateType)))
    val rows = Seq(Row(19000))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with DateType using String") {
    val schema = StructType(Seq(StructField("d", DateType)))
    val rows = Seq(Row("19000"))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with TimestampType using java.sql.Timestamp") {
    val schema = StructType(Seq(StructField("ts", TimestampType)))
    val rows = Seq(Row(java.sql.Timestamp.valueOf("2024-01-15 10:30:00")))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with TimestampType using Instant") {
    val schema = StructType(Seq(StructField("ts", TimestampType)))
    val rows = Seq(Row(java.time.Instant.parse("2024-01-15T10:30:00Z")))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with TimestampType using Number") {
    val schema = StructType(Seq(StructField("ts", TimestampType)))
    val rows = Seq(Row(1000000L))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with TimestampType using String") {
    val schema = StructType(Seq(StructField("ts", TimestampType)))
    val rows = Seq(Row("1000000"))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with DecimalType using java.math.BigDecimal") {
    val schema = StructType(Seq(StructField("d", DecimalType(10, 2))))
    val rows = Seq(Row(java.math.BigDecimal("123.45")))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with DecimalType using scala BigDecimal") {
    val schema = StructType(Seq(StructField("d", DecimalType(10, 2))))
    val rows = Seq(Row(BigDecimal("99.99")))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with DecimalType using String") {
    val schema = StructType(Seq(StructField("d", DecimalType(10, 2))))
    val rows = Seq(Row("42.00"))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  // ---------------------------------------------------------------------------
  // Null values
  // ---------------------------------------------------------------------------

  test("encodeRows with null value") {
    val schema = StructType(Seq(StructField("s", StringType, nullable = true)))
    val rows = Seq(Row(null))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with mixed null and non-null values") {
    val schema = StructType(Seq(StructField("i", IntegerType, nullable = true)))
    val rows = Seq(Row(1), Row(null), Row(3))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  // ---------------------------------------------------------------------------
  // Complex types
  // ---------------------------------------------------------------------------

  test("encodeRows with ArrayType of ints") {
    val schema = StructType(Seq(StructField("arr", ArrayType(IntegerType, false))))
    val rows = Seq(Row(Seq(1, 2, 3)))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with ArrayType of longs") {
    val schema = StructType(Seq(StructField("arr", ArrayType(LongType, false))))
    val rows = Seq(Row(Seq(100L, 200L)))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with ArrayType of doubles") {
    val schema = StructType(Seq(StructField("arr", ArrayType(DoubleType, false))))
    val rows = Seq(Row(Seq(1.1, 2.2)))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with ArrayType of floats") {
    val schema = StructType(Seq(StructField("arr", ArrayType(FloatType, false))))
    val rows = Seq(Row(Seq(1.1f, 2.2f)))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with ArrayType of shorts") {
    val schema = StructType(Seq(StructField("arr", ArrayType(ShortType, false))))
    val rows = Seq(Row(Seq(1.toShort, 2.toShort)))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with ArrayType of bytes") {
    val schema = StructType(Seq(StructField("arr", ArrayType(ByteType, false))))
    val rows = Seq(Row(Seq(1.toByte, 2.toByte)))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with ArrayType of booleans") {
    val schema = StructType(Seq(StructField("arr", ArrayType(BooleanType, false))))
    val rows = Seq(Row(Seq(true, false)))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with ArrayType of strings") {
    val schema = StructType(Seq(StructField("arr", ArrayType(StringType, false))))
    val rows = Seq(Row(Seq("hello", "world")))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with ArrayType containing nulls") {
    val schema = StructType(Seq(StructField("arr", ArrayType(IntegerType, true))))
    val rows = Seq(Row(Seq(1, null, 3)))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with ArrayType from Java Array") {
    val schema = StructType(Seq(StructField("arr", ArrayType(IntegerType, false))))
    val rows = Seq(Row(Array(1, 2, 3)))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with StructType (nested)") {
    val innerSchema = StructType(Seq(StructField("x", StringType)))
    val schema = StructType(Seq(StructField("s", innerSchema)))
    val rows = Seq(Row(Row("hello")))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with MapType") {
    val schema = StructType(Seq(
      StructField("m", MapType(StringType, IntegerType, valueContainsNull = false))
    ))
    val rows = Seq(Row(Map("a" -> 1)))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with TimestampNTZType using java.sql.Timestamp") {
    val schema = StructType(Seq(StructField("ts", TimestampNTZType)))
    val rows = Seq(Row(java.sql.Timestamp.valueOf("2024-01-15 10:30:00")))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with TimestampNTZType using Instant") {
    val schema = StructType(Seq(StructField("ts", TimestampNTZType)))
    val rows = Seq(Row(java.time.Instant.parse("2024-01-15T10:30:00Z")))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  // ---------------------------------------------------------------------------
  // Multiple columns
  // ---------------------------------------------------------------------------

  test("encodeRows with multiple columns") {
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("active", BooleanType)
    ))
    val rows = Seq(
      Row("Alice", 30, true),
      Row("Bob", 25, false)
    )
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("encodeRows with multiple rows") {
    val schema = StructType(Seq(StructField("i", IntegerType)))
    val rows = (1 to 100).map(i => Row(i))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }
