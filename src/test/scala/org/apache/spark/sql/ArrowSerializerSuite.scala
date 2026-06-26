package org.apache.spark.sql

import org.apache.spark.sql.connect.client.ArrowDeserializer
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

  // R23: BigDecimal scale must be rescaled to the schema's scale before Arrow accepts it.
  test("encodeRows rescales lower-scale BigDecimal up to schema scale (R23)") {
    val schema = StructType(Seq(StructField("d", DecimalType(10, 2))))
    // scale=0 against DecimalType(10, 2) — rescale should round to "99.00".
    val bytes = ArrowSerializer.encodeRows(Seq(Row(java.math.BigDecimal("99"))), schema)
    val (decoded, _) = ArrowDeserializer.fromArrowBatchWithSchema(bytes)
    decoded.head.get(0).asInstanceOf[java.math.BigDecimal] shouldBe
      new java.math.BigDecimal("99.00")
  }

  test("encodeRows rescales higher-scale BigDecimal down to schema scale (R23)") {
    val schema = StructType(Seq(StructField("d", DecimalType(10, 2))))
    // scale=3 against DecimalType(10, 2) — rescale (HALF_UP) → "100.00".
    val bytes = ArrowSerializer.encodeRows(Seq(Row(java.math.BigDecimal("99.999"))), schema)
    val (decoded, _) = ArrowDeserializer.fromArrowBatchWithSchema(bytes)
    decoded.head.get(0).asInstanceOf[java.math.BigDecimal] shouldBe
      new java.math.BigDecimal("100.00")
  }

  test("encodeRows rescales scala BigDecimal with mismatched scale (R23)") {
    val schema = StructType(Seq(StructField("d", DecimalType(10, 2))))
    val bytes = ArrowSerializer.encodeRows(Seq(Row(BigDecimal("99.9"))), schema)
    val (decoded, _) = ArrowDeserializer.fromArrowBatchWithSchema(bytes)
    decoded.head.get(0).asInstanceOf[java.math.BigDecimal] shouldBe
      new java.math.BigDecimal("99.90")
  }

  test("encodeRows rescales String fallback path against DecimalType (R23)") {
    val schema = StructType(Seq(StructField("d", DecimalType(10, 2))))
    val bytes = ArrowSerializer.encodeRows(Seq(Row("100")), schema)
    val (decoded, _) = ArrowDeserializer.fromArrowBatchWithSchema(bytes)
    decoded.head.get(0).asInstanceOf[java.math.BigDecimal] shouldBe
      new java.math.BigDecimal("100.00")
  }

  test("encodeRows nulls out BigDecimal exceeding schema precision (R23)") {
    val schema = StructType(Seq(StructField("d", DecimalType(4, 2)))) // max value 99.99
    val bytes = ArrowSerializer.encodeRows(Seq(Row(java.math.BigDecimal("12345.67"))), schema)
    val (decoded, _) = ArrowDeserializer.fromArrowBatchWithSchema(bytes)
    decoded.head.isNullAt(0) shouldBe true
  }

  // R24: setArrowValue must dispatch DayTimeIntervalType / YearMonthIntervalType / TimeType
  // through DurationVector / IntervalYearVector / TimeMicroVector — sparkTypeToArrow maps
  // them but the previous case list omitted these branches.

  test("encodeRows round-trips java.time.Duration via DurationVector (R24)") {
    val schema = StructType(Seq(StructField("d", DayTimeIntervalType, nullable = false)))
    val input = java.time.Duration.ofSeconds(7).plusNanos(123_456_000L) // 7s + 123ms 456us
    val bytes = ArrowSerializer.encodeRows(Seq(Row(input)), schema)
    val (decoded, _) = ArrowDeserializer.fromArrowBatchWithSchema(bytes)
    val out = decoded.head.get(0).asInstanceOf[java.time.Duration]
    out.toNanos shouldBe input.toNanos // microsecond precision matches Arrow vector unit
  }

  test("encodeRows round-trips java.time.Period via IntervalYearVector (R24)") {
    val schema = StructType(Seq(StructField("p", YearMonthIntervalType, nullable = false)))
    val input = java.time.Period.of(3, 4, 0) // 40 months total — Period.ofMonths normalizes
    val bytes = ArrowSerializer.encodeRows(Seq(Row(input)), schema)
    val (decoded, _) = ArrowDeserializer.fromArrowBatchWithSchema(bytes)
    // R42: decoder now surfaces a normalized Period rather than the raw int.
    decoded.head.get(0).asInstanceOf[java.time.Period] shouldBe java.time.Period.of(3, 4, 0)
  }

  test("encodeRows round-trips java.time.LocalTime via TimeMicroVector (R24)") {
    val schema = StructType(Seq(StructField("t", TimeType(), nullable = false)))
    val input = java.time.LocalTime.of(13, 45, 7, 999_000_000) // 13:45:07.999_000 (microsecond)
    val bytes = ArrowSerializer.encodeRows(Seq(Row(input)), schema)
    val (decoded, _) = ArrowDeserializer.fromArrowBatchWithSchema(bytes)
    decoded.head.get(0).asInstanceOf[java.time.LocalTime] shouldBe input
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

  // ---------------------------------------------------------------------------
  // Array / Map value round-trips through the deserializer.
  //
  // The bytes-only tests above prove encoding succeeds, but they never decode
  // the populated MapVector / ListVector — the per-element extraction loop in
  // ArrowDeserializer (start..end over the data vector) is what these lock in:
  // element ordering, per-row offset boundaries, empty containers, and nulls.
  // ---------------------------------------------------------------------------

  test("round-trips ArrayType(Int) element values across rows") {
    val schema = StructType(Seq(StructField("arr", ArrayType(IntegerType, false))))
    val rows = Seq(Row(Seq(1, 2, 3)), Row(Seq.empty[Int]), Row(Seq(42)))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    val (decoded, _) = ArrowDeserializer.fromArrowBatchWithSchema(bytes)
    decoded.map(_.get(0)) shouldBe Seq(Seq(1, 2, 3), Seq.empty[Int], Seq(42))
  }

  test("round-trips ArrayType(String) preserving element order") {
    val schema = StructType(Seq(StructField("arr", ArrayType(StringType, false))))
    val bytes = ArrowSerializer.encodeRows(Seq(Row(Seq("x", "y", "z"))), schema)
    val (decoded, _) = ArrowDeserializer.fromArrowBatchWithSchema(bytes)
    decoded.head.get(0) shouldBe Seq("x", "y", "z")
  }

  test("round-trips ArrayType with null elements") {
    val schema = StructType(Seq(StructField("arr", ArrayType(IntegerType, true))))
    val bytes = ArrowSerializer.encodeRows(Seq(Row(Seq(1, null, 3))), schema)
    val (decoded, _) = ArrowDeserializer.fromArrowBatchWithSchema(bytes)
    decoded.head.get(0) shouldBe Seq(1, null, 3)
  }

  test("round-trips MapType entries across rows") {
    val schema = StructType(Seq(
      StructField("m", MapType(StringType, IntegerType, valueContainsNull = false))
    ))
    val rows = Seq(Row(Map("a" -> 1, "b" -> 2)), Row(Map.empty[String, Int]), Row(Map("c" -> 3)))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    val (decoded, _) = ArrowDeserializer.fromArrowBatchWithSchema(bytes)
    decoded.map(_.get(0)) shouldBe
      Seq(Map("a" -> 1, "b" -> 2), Map.empty[String, Int], Map("c" -> 3))
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

  test("encodeRows throws on null Map key (R25)") {
    val schema = StructType(Seq(
      StructField("m", MapType(StringType, IntegerType, valueContainsNull = true))
    ))
    val nullKeyMap = new java.util.HashMap[String, Int]()
    nullKeyMap.put(null, 1)
    import scala.jdk.CollectionConverters.*
    assertThrows[NullPointerException] {
      ArrowSerializer.encodeRows(Seq(Row(nullKeyMap.asScala.toMap)), schema)
    }
  }

  // ---------------------------------------------------------------------------
  // Value-level round-trips through the deserializer.
  //
  // The bytes-only assertions above only prove encoding does not crash — a wrong
  // per-column setter (mis-cast, wrong vector branch) would still emit non-empty
  // bytes. These decode the result and assert the actual cell values per Arrow
  // vector type, locking in the setArrowValue dispatch before it is refactored.
  // ---------------------------------------------------------------------------

  private def roundTrip(schema: StructType, rows: Seq[Row]): Seq[Row] =
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    ArrowDeserializer.fromArrowBatchWithSchema(bytes)._1

  test("round-trips every primitive column to its exact value") {
    val schema = StructType(Seq(
      StructField("bool", BooleanType),
      StructField("byte", ByteType),
      StructField("short", ShortType),
      StructField("int", IntegerType),
      StructField("long", LongType),
      StructField("float", FloatType),
      StructField("double", DoubleType),
      StructField("str", StringType)
    ))
    val r = roundTrip(schema, Seq(Row(true, 1.toByte, 2.toShort, 3, 4L, 5.5f, 6.5, "hi"))).head
    r.get(0) shouldBe true
    r.get(1) shouldBe 1.toByte
    r.get(2) shouldBe 2.toShort
    r.get(3) shouldBe 3
    r.get(4) shouldBe 4L
    r.get(5) shouldBe 5.5f
    r.get(6) shouldBe 6.5
    r.get(7) shouldBe "hi"
  }

  test("round-trips BinaryType bytes") {
    val schema = StructType(Seq(StructField("b", BinaryType)))
    val r = roundTrip(schema, Seq(Row(Array[Byte](1, 2, 3)))).head
    r.get(0).asInstanceOf[Array[Byte]].toSeq shouldBe Seq[Byte](1, 2, 3)
  }

  test("round-trips DateType via java.sql.Date") {
    val schema = StructType(Seq(StructField("d", DateType)))
    val r = roundTrip(schema, Seq(Row(java.sql.Date.valueOf("2024-01-15")))).head
    r.get(0).asInstanceOf[java.sql.Date].toString shouldBe "2024-01-15"
  }

  test("round-trips TimestampType at microsecond precision") {
    val schema = StructType(Seq(StructField("ts", TimestampType)))
    val input = java.sql.Timestamp.valueOf("2024-01-15 10:30:00")
    val r = roundTrip(schema, Seq(Row(input))).head
    r.get(0).asInstanceOf[java.sql.Timestamp] shouldBe input
  }

  test("round-trips DecimalType preserving numeric value") {
    val schema = StructType(Seq(StructField("dec", DecimalType(10, 2))))
    val r = roundTrip(schema, Seq(Row(BigDecimal("123.45")))).head
    val decoded = r.get(0).asInstanceOf[java.math.BigDecimal]
    decoded.compareTo(new java.math.BigDecimal("123.45")) shouldBe 0
  }

  test("round-trips nested StructType field values") {
    val inner = StructType(Seq(StructField("x", StringType), StructField("y", IntegerType)))
    val schema = StructType(Seq(StructField("s", inner)))
    val r = roundTrip(schema, Seq(Row(Row("hello", 42)))).head
    val nested = r.get(0).asInstanceOf[Row]
    nested.get(0) shouldBe "hello"
    nested.get(1) shouldBe 42
  }

  test("round-trips a null in a nullable column") {
    val schema = StructType(Seq(StructField("i", IntegerType, nullable = true)))
    val out = roundTrip(schema, Seq(Row(1), Row(null), Row(3)))
    out.map(_.get(0)) shouldBe Seq(1, null, 3)
  }

  test("round-trips multiple rows and columns to the right cells") {
    val schema = StructType(Seq(StructField("i", IntegerType), StructField("s", StringType)))
    val rows = Seq(Row(1, "a"), Row(2, "b"), Row(3, "c"))
    val out = roundTrip(schema, rows)
    out.map(r => (r.get(0), r.get(1))) shouldBe Seq((1, "a"), (2, "b"), (3, "c"))
  }
