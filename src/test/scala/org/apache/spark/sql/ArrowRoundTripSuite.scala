package org.apache.spark.sql

import org.apache.spark.sql.connect.client.ArrowDeserializer
import org.apache.spark.sql.types.*
import org.scalacheck.Gen
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

/** Property-based round-trip tests: `decode(encode(rows)) == rows` for randomly generated schemas
  * and values. Each property focuses on one type family so failures are easy to localise.
  *
  * Generators produce values in their canonical post-decode form (e.g. `java.sql.Date`, microsecond
  * timestamps) so equality holds without normalisation. Sub-microsecond precision and NaN edge
  * cases are excluded — they are covered by explicit unit tests in [[ArrowSerializerSuite]] and
  * [[org.apache.spark.sql.connect.client.ArrowDeserializerSuite]].
  */
class ArrowRoundTripSuite
    extends AnyFunSuite
    with Matchers
    with ScalaCheckPropertyChecks:

  // Bound size to keep individual properties fast (default scalacheck-min-successful=100 runs).
  override implicit val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = 30, sizeRange = 20)

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Deep equality that handles `Array[Byte]` (which has reference equality by default). */
  private def deepEqual(a: Any, b: Any): Boolean = (a, b) match
    case (null, null)                     => true
    case (null, _) | (_, null)            => false
    case (x: Array[Byte], y: Array[Byte]) => java.util.Arrays.equals(x, y)
    case (x: Row, y: Row) =>
      x.size == y.size && (0 until x.size).forall(i => deepEqual(x.get(i), y.get(i)))
    case (x: Seq[?], y: Seq[?]) =>
      x.size == y.size && x.lazyZip(y).forall(deepEqual)
    case (x: Map[?, ?], y: Map[?, ?]) =>
      x.size == y.size && x.forall { (k, v) =>
        y.asInstanceOf[Map[Any, Any]].get(k).exists(deepEqual(_, v))
      }
    case _ => a == b

  private def deepEqualRows(a: Seq[Row], b: Seq[Row]): Boolean =
    a.size == b.size && a.lazyZip(b).forall(deepEqual)

  /** Encode then decode; assert structural equality. */
  private def assertRoundTrip(rows: Seq[Row], schema: StructType): Unit =
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    if rows.isEmpty then
      bytes shouldBe empty
    else
      val (decoded, decodedSchema) = ArrowDeserializer.fromArrowBatchWithSchema(bytes)
      decodedSchema shouldBe Some(schema)
      assert(
        deepEqualRows(rows, decoded.toSeq),
        s"round-trip mismatch:\n  in=$rows\n  out=$decoded"
      )

  /** Wrap a value generator to occasionally produce nulls when the column is nullable. */
  private def nullable[A](gen: Gen[A], pNull: Int = 1, pValue: Int = 9): Gen[Any] =
    Gen.frequency(pNull -> Gen.const(null), pValue -> gen.map(_.asInstanceOf[Any]))

  // ---------------------------------------------------------------------------
  // Generators for primitive values (canonical post-decode types)
  // ---------------------------------------------------------------------------

  private val genBoolean: Gen[Boolean] = Gen.oneOf(true, false)
  private val genByte: Gen[Byte]       = Gen.choose(Byte.MinValue, Byte.MaxValue)
  private val genShort: Gen[Short]     = Gen.choose(Short.MinValue, Short.MaxValue)
  private val genInt: Gen[Int]         = Gen.choose(Int.MinValue, Int.MaxValue)
  private val genLong: Gen[Long]       = Gen.choose(Long.MinValue, Long.MaxValue)
  // Exclude NaN to keep equality straightforward.
  private val genFloat: Gen[Float] =
    Gen.choose(-1e6f, 1e6f).suchThat(f => !java.lang.Float.isNaN(f))
  private val genDouble: Gen[Double] =
    Gen.choose(-1e9, 1e9).suchThat(d => !java.lang.Double.isNaN(d))
  // ASCII printable; the decoder uses UTF-8 but we keep generators boring.
  private val genString: Gen[String] = Gen.listOf(Gen.choose(' ', '~')).map(_.mkString)

  // Encoder extracts `epochDay` via `Date.toLocalDate.toEpochDay` (JVM default TZ);
  // decoder reconstructs via `Date.valueOf(LocalDate.ofEpochDay(epochDay))` (JVM default TZ).
  // Generate canonical Date values via the same `LocalDate.ofEpochDay → Date.valueOf` path so
  // equality holds regardless of the test JVM's default time zone.
  private val genDate: Gen[java.sql.Date] =
    Gen.choose(0, 50_000).map(epochDay =>
      java.sql.Date.valueOf(java.time.LocalDate.ofEpochDay(epochDay.toLong))
    )

  // Microsecond-aligned timestamps so encode→decode round-trips losslessly. Build via
  // `Timestamp.from(Instant)` to match the decoder's internal representation exactly.
  private val genTimestamp: Gen[java.sql.Timestamp] =
    for
      epochSecond <- Gen.choose(0L, 4_000_000_000L) // up to year ~2096
      micros      <- Gen.choose(0, 999_999)
    yield java.sql.Timestamp.from(java.time.Instant.ofEpochSecond(epochSecond, micros * 1000L))

  // Fixed precision/scale so generated BigDecimal matches the schema after decode.
  private val genDecimal: Gen[java.math.BigDecimal] =
    Gen.choose(-99_999_999L, 99_999_999L).map { unscaled =>
      java.math.BigDecimal.valueOf(unscaled, 2) // DecimalType(10, 2)
    }

  private val genBinary: Gen[Array[Byte]] =
    Gen.listOf(Gen.choose(Byte.MinValue, Byte.MaxValue)).map(_.toArray)

  // ---------------------------------------------------------------------------
  // Per-type single-column round-trip
  // ---------------------------------------------------------------------------

  private def testSingleColumn[A](
      name: String,
      dt: DataType,
      gen: Gen[A]
  ): Unit =
    test(s"round-trip: single column $name (non-null)") {
      forAll(Gen.listOf(gen)) { values =>
        val schema = StructType(Seq(StructField("c", dt, nullable = false)))
        val rows   = values.map(v => Row(v))
        assertRoundTrip(rows, schema)
      }
    }
    test(s"round-trip: single column $name (nullable)") {
      forAll(Gen.listOf(nullable(gen))) { values =>
        val schema = StructType(Seq(StructField("c", dt, nullable = true)))
        val rows   = values.map(v => Row(v))
        assertRoundTrip(rows, schema)
      }
    }

  testSingleColumn("Boolean", BooleanType, genBoolean)
  testSingleColumn("Byte", ByteType, genByte)
  testSingleColumn("Short", ShortType, genShort)
  testSingleColumn("Integer", IntegerType, genInt)
  testSingleColumn("Long", LongType, genLong)
  testSingleColumn("Float", FloatType, genFloat)
  testSingleColumn("Double", DoubleType, genDouble)
  testSingleColumn("String", StringType, genString)
  testSingleColumn("Date", DateType, genDate)
  testSingleColumn("Timestamp", TimestampType, genTimestamp)
  testSingleColumn("Decimal(10,2)", DecimalType(10, 2), genDecimal)
  testSingleColumn("Binary", BinaryType, genBinary)

  // ---------------------------------------------------------------------------
  // Multi-column random schemas
  // ---------------------------------------------------------------------------

  /** A column = (StructField, value generator). */
  private case class ColumnSpec(field: StructField, gen: Gen[Any])

  private def primitiveColumn(name: String, nullable: Boolean): Gen[ColumnSpec] =
    Gen.oneOf(
      ColumnSpec(StructField(name, BooleanType, nullable), genBoolean.map(_.asInstanceOf[Any])),
      ColumnSpec(StructField(name, IntegerType, nullable), genInt.map(_.asInstanceOf[Any])),
      ColumnSpec(StructField(name, LongType, nullable), genLong.map(_.asInstanceOf[Any])),
      ColumnSpec(StructField(name, DoubleType, nullable), genDouble.map(_.asInstanceOf[Any])),
      ColumnSpec(StructField(name, StringType, nullable), genString.map(_.asInstanceOf[Any])),
      ColumnSpec(StructField(name, DateType, nullable), genDate.map(_.asInstanceOf[Any]))
    )

  test("round-trip: random multi-column primitive schemas") {
    val genSchemaAndRows: Gen[(StructType, Seq[Row])] =
      for
        numCols <- Gen.choose(1, 6)
        cols <- Gen.sequence[List[ColumnSpec], ColumnSpec](
          (0 until numCols).map(i => primitiveColumn(s"c$i", nullable = true)).toList
        )
        numRows <- Gen.choose(0, 20)
        rows <- Gen.listOfN(
          numRows,
          Gen.sequence[List[Any], Any](
            cols.map(c => nullable(c.gen, pNull = 2, pValue = 8))
          ).map(values => Row.fromSeq(values))
        )
      yield (StructType(cols.map(_.field)), rows)

    forAll(genSchemaAndRows) { case (schema, rows) =>
      assertRoundTrip(rows, schema)
    }
  }

  // ---------------------------------------------------------------------------
  // Arrays of primitives
  // ---------------------------------------------------------------------------

  test("round-trip: ArrayType[Int] (non-null elements)") {
    val genArrayCol = Gen.listOf(Gen.listOf(genInt))
    forAll(genArrayCol) { lists =>
      // Decoder always reports containsNull = true for ArrayType, so use that here.
      val schema = StructType(Seq(
        StructField("arr", ArrayType(IntegerType, containsNull = true), nullable = false)
      ))
      val rows = lists.map(xs => Row(xs.toSeq))
      assertRoundTrip(rows, schema)
    }
  }

  test("round-trip: ArrayType[String] (non-null elements)") {
    val genArrayCol = Gen.listOf(Gen.listOf(genString))
    forAll(genArrayCol) { lists =>
      val schema = StructType(Seq(
        StructField("arr", ArrayType(StringType, containsNull = true), nullable = false)
      ))
      val rows = lists.map(xs => Row(xs.toSeq))
      assertRoundTrip(rows, schema)
    }
  }

  test("round-trip: ArrayType[Date] (non-null elements)") {
    val genArrayCol = Gen.listOf(Gen.listOf(genDate))
    forAll(genArrayCol) { lists =>
      val schema = StructType(Seq(
        StructField("arr", ArrayType(DateType, containsNull = true), nullable = false)
      ))
      val rows = lists.map(xs => Row(xs.toSeq))
      assertRoundTrip(rows, schema)
    }
  }

  test("round-trip: ArrayType[Timestamp] (non-null elements)") {
    val genArrayCol = Gen.listOf(Gen.listOf(genTimestamp))
    forAll(genArrayCol) { lists =>
      val schema = StructType(Seq(
        StructField("arr", ArrayType(TimestampType, containsNull = true), nullable = false)
      ))
      val rows = lists.map(xs => Row(xs.toSeq))
      assertRoundTrip(rows, schema)
    }
  }

  test("round-trip: ArrayType[Decimal(10,2)] (non-null elements)") {
    val genArrayCol = Gen.listOf(Gen.listOf(genDecimal))
    forAll(genArrayCol) { lists =>
      val schema = StructType(Seq(
        StructField("arr", ArrayType(DecimalType(10, 2), containsNull = true), nullable = false)
      ))
      val rows = lists.map(xs => Row(xs.toSeq))
      assertRoundTrip(rows, schema)
    }
  }

  test("round-trip: ArrayType[Binary] (non-null elements)") {
    val genArrayCol = Gen.listOf(Gen.listOf(genBinary))
    forAll(genArrayCol) { lists =>
      val schema = StructType(Seq(
        StructField("arr", ArrayType(BinaryType, containsNull = true), nullable = false)
      ))
      val rows = lists.map(xs => Row(xs.toSeq))
      assertRoundTrip(rows, schema)
    }
  }

  // ---------------------------------------------------------------------------
  // R20: DateType round-trip stays stable in non-UTC JVM time zones
  //
  // Previously the decoder reconstructed `Date(epochDay * 86400000L)` (UTC midnight) while
  // the encoder extracted `epochDay` via `Date.toLocalDate.toEpochDay` (JVM default TZ).
  // Under non-UTC TZ that asymmetry shifted dates by one day on round-trip — silently.
  // ---------------------------------------------------------------------------

  test("round-trip: DateType is stable across non-UTC default time zone (R20)") {
    val originalTz = java.util.TimeZone.getDefault
    try
      // -05:00 — would have caused a one-day drift before the deserializer fix.
      java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("America/New_York"))
      val schema = StructType(Seq(StructField("d", DateType, nullable = false)))
      val inputs = Seq(
        java.sql.Date.valueOf("2020-06-15"),
        java.sql.Date.valueOf("1970-01-01"),
        java.sql.Date.valueOf("2025-12-31")
      )
      val rows = inputs.map(d => Row(d))
      val bytes = ArrowSerializer.encodeRows(rows, schema)
      val (decoded, _) = ArrowDeserializer.fromArrowBatchWithSchema(bytes)
      decoded.toSeq.zip(inputs).foreach { (row, expected) =>
        val got = row.get(0).asInstanceOf[java.sql.Date]
        got.toLocalDate shouldBe expected.toLocalDate
      }
    finally java.util.TimeZone.setDefault(originalTz)
  }

  // ---------------------------------------------------------------------------
  // R84: VariantType is wire-encoded as Struct{value:Binary, metadata:Binary}
  //
  // Previously the encoder mapped VariantType to a single Binary buffer, which collided with
  // VarBinaryVector's Array[Byte] cast and produced ClassCastException on the client. The
  // deserializer was already implementing the upstream struct shape — this test pins both
  // sides to the same wire format.
  // ---------------------------------------------------------------------------

  test("round-trip: VariantType encodes as struct with value/metadata binary children (R84)") {
    val schema = StructType(Seq(StructField("v", VariantType, nullable = false)))
    val variants = Seq(
      VariantVal(Array[Byte](1, 2, 3), Array[Byte](10)),
      VariantVal(Array[Byte](42), Array[Byte](20, 21)),
      VariantVal(Array.emptyByteArray, Array[Byte](30))
    )
    val rows = variants.map(v => Row(v))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    val (decoded, decodedSchema) = ArrowDeserializer.fromArrowBatchWithSchema(bytes)
    decodedSchema shouldBe Some(schema)
    decoded should have size variants.size
    decoded.toSeq.zip(variants).foreach { (row, expected) =>
      val got = row.get(0).asInstanceOf[VariantVal]
      java.util.Arrays.equals(got.value, expected.value) shouldBe true
      java.util.Arrays.equals(got.metadata, expected.metadata) shouldBe true
    }
  }
