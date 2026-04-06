package org.apache.spark.sql

import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

// Test case classes with derived Encoders
case class Person(name: String, age: Int) derives Encoder
case class Score(subject: String, value: Double) derives Encoder
case class WithOption(name: String, score: Option[Int]) derives Encoder

class EncoderSuite extends AnyFunSuite with Matchers:

  // ---------------------------------------------------------------------------
  // Primitive Encoders
  // ---------------------------------------------------------------------------

  test("Int encoder schema and round-trip") {
    val enc = summon[Encoder[Int]]
    enc.schema shouldBe StructType(Seq(StructField("value", IntegerType)))
    enc.fromRow(enc.toRow(42)) shouldBe 42
  }

  test("Long encoder schema and round-trip") {
    val enc = summon[Encoder[Long]]
    enc.schema shouldBe StructType(Seq(StructField("value", LongType)))
    enc.fromRow(enc.toRow(123L)) shouldBe 123L
  }

  test("Double encoder schema and round-trip") {
    val enc = summon[Encoder[Double]]
    enc.schema shouldBe StructType(Seq(StructField("value", DoubleType)))
    enc.fromRow(enc.toRow(3.14)) shouldBe 3.14
  }

  test("String encoder schema and round-trip") {
    val enc = summon[Encoder[String]]
    enc.schema shouldBe StructType(Seq(StructField("value", StringType)))
    enc.fromRow(enc.toRow("hello")) shouldBe "hello"
  }

  test("Boolean encoder schema and round-trip") {
    val enc = summon[Encoder[Boolean]]
    enc.schema shouldBe StructType(Seq(StructField("value", BooleanType)))
    enc.fromRow(enc.toRow(true)) shouldBe true
  }

  // ---------------------------------------------------------------------------
  // Product (case class) Encoder derivation
  // ---------------------------------------------------------------------------

  test("Person encoder has correct schema") {
    val enc = summon[Encoder[Person]]
    enc.schema shouldBe StructType(Seq(
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false)
    ))
  }

  test("Person encoder round-trip") {
    val enc = summon[Encoder[Person]]
    val person = Person("Alice", 30)
    val row = enc.toRow(person)
    row.getString(0) shouldBe "Alice"
    row.getInt(1) shouldBe 30
    enc.fromRow(row) shouldBe person
  }

  test("Score encoder round-trip") {
    val enc = summon[Encoder[Score]]
    val score = Score("Math", 95.5)
    val row = enc.toRow(score)
    row.getString(0) shouldBe "Math"
    row.getDouble(1) shouldBe 95.5
    enc.fromRow(row) shouldBe score
  }

  test("derived encoder schema has correct field names from case class") {
    val enc = summon[Encoder[Person]]
    enc.schema.fieldNames shouldBe Array("name", "age")
  }

  test("derived encoder schema field types match Scala types") {
    val enc = summon[Encoder[Score]]
    enc.schema.fields(0).dataType shouldBe StringType
    enc.schema.fields(1).dataType shouldBe DoubleType
  }

  test("Encoder.derived via `derives` keyword works") {
    // Person has `derives Encoder` so it should compile and work
    val enc: Encoder[Person] = summon[Encoder[Person]]
    enc should not be null
  }

  // ---------------------------------------------------------------------------
  // Multiple case classes
  // ---------------------------------------------------------------------------

  test("different case classes have independent encoders") {
    val personEnc = summon[Encoder[Person]]
    val scoreEnc = summon[Encoder[Score]]
    personEnc.schema should not be scoreEnc.schema
    personEnc.schema.fieldNames shouldBe Array("name", "age")
    scoreEnc.schema.fieldNames shouldBe Array("subject", "value")
  }

  test("toRow produces correct Row values") {
    val enc = summon[Encoder[Person]]
    val row = enc.toRow(Person("Bob", 25))
    row.size shouldBe 2
    row.get(0) shouldBe "Bob"
    row.get(1) shouldBe 25
  }

  // ---------------------------------------------------------------------------
  // Remaining Primitive Encoders (Float, Short, Byte)
  // ---------------------------------------------------------------------------

  test("Float encoder schema and round-trip") {
    val enc = summon[Encoder[Float]]
    enc.schema shouldBe StructType(Seq(StructField("value", FloatType)))
    enc.fromRow(enc.toRow(1.5f)) shouldBe 1.5f
  }

  test("Short encoder schema and round-trip") {
    val enc = summon[Encoder[Short]]
    enc.schema shouldBe StructType(Seq(StructField("value", ShortType)))
    val v: Short = 42
    enc.fromRow(enc.toRow(v)) shouldBe v
  }

  test("Byte encoder schema and round-trip") {
    val enc = summon[Encoder[Byte]]
    enc.schema shouldBe StructType(Seq(StructField("value", ByteType)))
    val v: Byte = 7
    enc.fromRow(enc.toRow(v)) shouldBe v
  }

  // ---------------------------------------------------------------------------
  // AgnosticEncoder references
  // ---------------------------------------------------------------------------

  test("Int encoder provides PrimitiveIntEncoder agnosticEncoder") {
    val enc = summon[Encoder[Int]]
    enc.agnosticEncoder should not be null
  }

  test("Long encoder provides PrimitiveLongEncoder agnosticEncoder") {
    val enc = summon[Encoder[Long]]
    enc.agnosticEncoder should not be null
  }

  test("Double encoder provides PrimitiveDoubleEncoder agnosticEncoder") {
    val enc = summon[Encoder[Double]]
    enc.agnosticEncoder should not be null
  }

  test("Float encoder provides PrimitiveFloatEncoder agnosticEncoder") {
    val enc = summon[Encoder[Float]]
    enc.agnosticEncoder should not be null
  }

  test("Short encoder provides PrimitiveShortEncoder agnosticEncoder") {
    val enc = summon[Encoder[Short]]
    enc.agnosticEncoder should not be null
  }

  test("Byte encoder provides PrimitiveByteEncoder agnosticEncoder") {
    val enc = summon[Encoder[Byte]]
    enc.agnosticEncoder should not be null
  }

  test("Boolean encoder provides PrimitiveBooleanEncoder agnosticEncoder") {
    val enc = summon[Encoder[Boolean]]
    enc.agnosticEncoder should not be null
  }

  test("String encoder provides StringEncoder agnosticEncoder") {
    val enc = summon[Encoder[String]]
    enc.agnosticEncoder should not be null
  }

  // ---------------------------------------------------------------------------
  // Extended Type Encoders
  // ---------------------------------------------------------------------------

  test("java.sql.Date encoder schema and round-trip") {
    val enc = summon[Encoder[java.sql.Date]]
    enc.schema shouldBe StructType(Seq(StructField("value", DateType)))
    val d = java.sql.Date.valueOf("2026-01-15")
    enc.fromRow(enc.toRow(d)) shouldBe d
    enc.agnosticEncoder should not be null
  }

  test("java.sql.Timestamp encoder schema and round-trip") {
    val enc = summon[Encoder[java.sql.Timestamp]]
    enc.schema shouldBe StructType(Seq(StructField("value", TimestampType)))
    val ts = java.sql.Timestamp.valueOf("2026-01-15 10:30:00")
    enc.fromRow(enc.toRow(ts)) shouldBe ts
    enc.agnosticEncoder should not be null
  }

  test("java.time.LocalDate encoder schema and round-trip") {
    val enc = summon[Encoder[java.time.LocalDate]]
    enc.schema shouldBe StructType(Seq(StructField("value", DateType)))
    val ld = java.time.LocalDate.of(2026, 1, 15)
    enc.fromRow(enc.toRow(ld)) shouldBe ld
    enc.agnosticEncoder should not be null
  }

  test("java.time.Instant encoder schema and round-trip") {
    val enc = summon[Encoder[java.time.Instant]]
    enc.schema shouldBe StructType(Seq(StructField("value", TimestampType)))
    val inst = java.time.Instant.parse("2026-01-15T10:30:00Z")
    enc.fromRow(enc.toRow(inst)) shouldBe inst
    enc.agnosticEncoder should not be null
  }

  test("BigDecimal encoder schema and round-trip") {
    val enc = summon[Encoder[BigDecimal]]
    enc.schema shouldBe StructType(Seq(StructField("value", DecimalType.DEFAULT)))
    val bd = BigDecimal("123.456")
    enc.fromRow(enc.toRow(bd)) shouldBe bd
    enc.agnosticEncoder should not be null
  }

  test("BigDecimal encoder fromRow handles java.math.BigDecimal") {
    val enc = summon[Encoder[BigDecimal]]
    val jbd = new java.math.BigDecimal("99.99")
    val row = Row(jbd)
    enc.fromRow(row) shouldBe BigDecimal("99.99")
  }

  test("BigDecimal encoder fromRow handles Number") {
    val enc = summon[Encoder[BigDecimal]]
    val num: Number = java.lang.Double.valueOf(42.5)
    val row = Row(num)
    enc.fromRow(row) shouldBe BigDecimal(42.5)
  }

  test("BigDecimal encoder fromRow handles String-like via toString") {
    val enc = summon[Encoder[BigDecimal]]
    // Pass a string which is not a BigDecimal, Number, or java.math.BigDecimal
    val row = Row("100.25")
    enc.fromRow(row) shouldBe BigDecimal("100.25")
  }

  test("Array[Byte] encoder schema and round-trip") {
    val enc = summon[Encoder[Array[Byte]]]
    enc.schema shouldBe StructType(Seq(StructField("value", BinaryType)))
    val bytes = Array[Byte](1, 2, 3, 4)
    enc.fromRow(enc.toRow(bytes)) shouldBe bytes
    enc.agnosticEncoder should not be null
  }

  // ---------------------------------------------------------------------------
  // Option handling in derived encoders
  // ---------------------------------------------------------------------------

  test("WithOption encoder has correct nullable schema") {
    val enc = summon[Encoder[WithOption]]
    enc.schema.fields(0).nullable shouldBe false
    enc.schema.fields(1).nullable shouldBe true
    enc.schema.fields(1).dataType shouldBe IntegerType
  }

  test("WithOption encoder round-trip with Some value") {
    val enc = summon[Encoder[WithOption]]
    val wo = WithOption("test", Some(42))
    val row = enc.toRow(wo)
    row.get(0) shouldBe "test"
    row.get(1) shouldBe 42 // unwrapped from Some
    enc.fromRow(row) shouldBe wo
  }

  test("WithOption encoder round-trip with None value") {
    val enc = summon[Encoder[WithOption]]
    val wo = WithOption("test", None)
    val row = enc.toRow(wo)
    row.get(0) shouldBe "test"
    row.get(1) shouldBe null.asInstanceOf[AnyRef] // unwrapped from None
    enc.fromRow(row) shouldBe wo
  }

  // ---------------------------------------------------------------------------
  // DerivedEncoder is a named class (not anonymous)
  // ---------------------------------------------------------------------------

  test("derived encoder is a DerivedEncoder instance") {
    val enc = summon[Encoder[Person]]
    enc shouldBe a[Encoder.DerivedEncoder[?]]
  }

  test("derived encoder agnosticEncoder returns null (no AgnosticEncoder for products)") {
    val enc = summon[Encoder[Person]]
    enc.agnosticEncoder shouldBe null
  }

  // ---------------------------------------------------------------------------
  // Compile-time sparkTypeOf (exercised via fieldsOf in derived encoders)
  // ---------------------------------------------------------------------------

  test("sparkTypeOf is exercised through derived product encoder for all field types") {
    // This case class exercises Boolean, Int, Long, Float, Double, String via derived encoder
    case class AllTypes(
        b: Boolean,
        by: Byte,
        s: Short,
        i: Int,
        l: Long,
        f: Float,
        d: Double,
        str: String
    ) derives Encoder

    val enc = summon[Encoder[AllTypes]]
    enc.schema.fields.map(_.dataType) shouldBe Array(
      BooleanType,
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      StringType
    )
  }

  test("sparkTypeOf for Option fields makes the field nullable") {
    case class OptFields(a: Option[Int], b: Option[String]) derives Encoder
    val enc = summon[Encoder[OptFields]]
    enc.schema.fields(0).nullable shouldBe true
    enc.schema.fields(0).dataType shouldBe IntegerType
    enc.schema.fields(1).nullable shouldBe true
    enc.schema.fields(1).dataType shouldBe StringType
  }

  test("sparkTypeOf for BigDecimal field in product") {
    case class WithDecimal(amount: BigDecimal) derives Encoder
    val enc = summon[Encoder[WithDecimal]]
    enc.schema.fields(0).dataType shouldBe DecimalType.DEFAULT
  }

  test("sparkTypeOf for Date and Timestamp fields in product") {
    case class WithDates(d: java.sql.Date, ts: java.sql.Timestamp) derives Encoder
    val enc = summon[Encoder[WithDates]]
    enc.schema.fields(0).dataType shouldBe DateType
    enc.schema.fields(1).dataType shouldBe TimestampType
  }

  test("sparkTypeOf for LocalDate and Instant fields in product") {
    case class WithJavaTime(ld: java.time.LocalDate, inst: java.time.Instant) derives Encoder
    val enc = summon[Encoder[WithJavaTime]]
    enc.schema.fields(0).dataType shouldBe DateType
    enc.schema.fields(1).dataType shouldBe TimestampType
  }

  test("sparkTypeOf for Array[Byte] field in product") {
    case class WithBinary(data: Array[Byte]) derives Encoder
    val enc = summon[Encoder[WithBinary]]
    enc.schema.fields(0).dataType shouldBe BinaryType
  }
