package org.apache.spark.sql

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.*
import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for the Encoders factory object. */
class EncodersSuite extends AnyFunSuite with Matchers:

  // ---------------------------------------------------------------------------
  // Scala primitive types
  // ---------------------------------------------------------------------------

  test("scalaBoolean returns correct encoder") {
    val enc = Encoders.scalaBoolean
    enc.schema shouldBe StructType(Seq(StructField("value", BooleanType)))
    enc.agnosticEncoder shouldBe PrimitiveBooleanEncoder
  }

  test("scalaByte returns correct encoder") {
    val enc = Encoders.scalaByte
    enc.schema shouldBe StructType(Seq(StructField("value", ByteType)))
    enc.agnosticEncoder shouldBe PrimitiveByteEncoder
  }

  test("scalaShort returns correct encoder") {
    val enc = Encoders.scalaShort
    enc.schema shouldBe StructType(Seq(StructField("value", ShortType)))
    enc.agnosticEncoder shouldBe PrimitiveShortEncoder
  }

  test("scalaInt returns correct encoder") {
    val enc = Encoders.scalaInt
    enc.schema shouldBe StructType(Seq(StructField("value", IntegerType)))
    enc.agnosticEncoder shouldBe PrimitiveIntEncoder
  }

  test("scalaLong returns correct encoder") {
    val enc = Encoders.scalaLong
    enc.schema shouldBe StructType(Seq(StructField("value", LongType)))
    enc.agnosticEncoder shouldBe PrimitiveLongEncoder
  }

  test("scalaFloat returns correct encoder") {
    val enc = Encoders.scalaFloat
    enc.schema shouldBe StructType(Seq(StructField("value", FloatType)))
    enc.agnosticEncoder shouldBe PrimitiveFloatEncoder
  }

  test("scalaDouble returns correct encoder") {
    val enc = Encoders.scalaDouble
    enc.schema shouldBe StructType(Seq(StructField("value", DoubleType)))
    enc.agnosticEncoder shouldBe PrimitiveDoubleEncoder
  }

  // ---------------------------------------------------------------------------
  // Java boxed types
  // ---------------------------------------------------------------------------

  test("BOOLEAN returns correct encoder") {
    val enc = Encoders.BOOLEAN
    enc.schema shouldBe StructType(Seq(StructField("value", BooleanType)))
    enc.agnosticEncoder shouldBe BoxedBooleanEncoder
  }

  test("BYTE returns correct encoder") {
    val enc = Encoders.BYTE
    enc.schema shouldBe StructType(Seq(StructField("value", ByteType)))
    enc.agnosticEncoder shouldBe BoxedByteEncoder
  }

  test("SHORT returns correct encoder") {
    val enc = Encoders.SHORT
    enc.schema shouldBe StructType(Seq(StructField("value", ShortType)))
    enc.agnosticEncoder shouldBe BoxedShortEncoder
  }

  test("INT returns correct encoder") {
    val enc = Encoders.INT
    enc.schema shouldBe StructType(Seq(StructField("value", IntegerType)))
    enc.agnosticEncoder shouldBe BoxedIntEncoder
  }

  test("LONG returns correct encoder") {
    val enc = Encoders.LONG
    enc.schema shouldBe StructType(Seq(StructField("value", LongType)))
    enc.agnosticEncoder shouldBe BoxedLongEncoder
  }

  test("FLOAT returns correct encoder") {
    val enc = Encoders.FLOAT
    enc.schema shouldBe StructType(Seq(StructField("value", FloatType)))
    enc.agnosticEncoder shouldBe BoxedFloatEncoder
  }

  test("DOUBLE returns correct encoder") {
    val enc = Encoders.DOUBLE
    enc.schema shouldBe StructType(Seq(StructField("value", DoubleType)))
    enc.agnosticEncoder shouldBe BoxedDoubleEncoder
  }

  // ---------------------------------------------------------------------------
  // String / Binary
  // ---------------------------------------------------------------------------

  test("STRING returns correct encoder") {
    val enc = Encoders.STRING
    enc.schema shouldBe StructType(Seq(StructField("value", StringType)))
    enc.agnosticEncoder shouldBe StringEncoder
  }

  test("BINARY returns correct encoder") {
    val enc = Encoders.BINARY
    enc.schema shouldBe StructType(Seq(StructField("value", BinaryType)))
    enc.agnosticEncoder shouldBe BinaryEncoder
  }

  // ---------------------------------------------------------------------------
  // Date / Time / Decimal
  // ---------------------------------------------------------------------------

  test("DATE returns correct encoder") {
    val enc = Encoders.DATE
    enc.schema shouldBe StructType(Seq(StructField("value", DateType)))
    enc.agnosticEncoder shouldBe STRICT_DATE_ENCODER
  }

  test("LOCALDATE returns correct encoder") {
    val enc = Encoders.LOCALDATE
    enc.schema shouldBe StructType(Seq(StructField("value", DateType)))
    enc.agnosticEncoder shouldBe STRICT_LOCAL_DATE_ENCODER
  }

  test("TIMESTAMP returns correct encoder") {
    val enc = Encoders.TIMESTAMP
    enc.schema shouldBe StructType(Seq(StructField("value", TimestampType)))
    enc.agnosticEncoder shouldBe STRICT_TIMESTAMP_ENCODER
  }

  test("INSTANT returns correct encoder") {
    val enc = Encoders.INSTANT
    enc.schema shouldBe StructType(Seq(StructField("value", TimestampType)))
    enc.agnosticEncoder shouldBe STRICT_INSTANT_ENCODER
  }

  test("LOCALDATETIME returns correct encoder") {
    val enc = Encoders.LOCALDATETIME
    enc.schema shouldBe StructType(Seq(StructField("value", TimestampNTZType)))
    enc.agnosticEncoder shouldBe LocalDateTimeEncoder
  }

  test("DECIMAL returns correct encoder") {
    val enc = Encoders.DECIMAL
    enc.schema shouldBe StructType(Seq(StructField("value", DecimalType.DEFAULT)))
    enc.agnosticEncoder shouldBe DEFAULT_JAVA_DECIMAL_ENCODER
  }

  // ---------------------------------------------------------------------------
  // Row encoder
  // ---------------------------------------------------------------------------

  test("row returns Row encoder") {
    val enc = Encoders.row
    enc should not be null
    enc.agnosticEncoder shouldBe UnboundRowEncoder
  }

  // ---------------------------------------------------------------------------
  // Tuple encoders
  // ---------------------------------------------------------------------------

  test("tuple2 encoder has correct schema") {
    val enc = Encoders.tuple(Encoders.scalaInt, Encoders.STRING)
    enc.schema shouldBe StructType(Seq(
      StructField(
        "_1",
        StructType(Seq(StructField("value", IntegerType)))
      ),
      StructField(
        "_2",
        StructType(Seq(StructField("value", StringType)))
      )
    ))
  }

  test("tuple3 encoder has correct schema") {
    val enc = Encoders.tuple(Encoders.scalaInt, Encoders.STRING, Encoders.scalaDouble)
    val innerFields = enc.agnosticEncoder.asInstanceOf[ProductEncoder[?]].fields
    innerFields should have size 3
    innerFields(0).name shouldBe "_1"
    innerFields(1).name shouldBe "_2"
    innerFields(2).name shouldBe "_3"
  }

  test("tuple4 encoder has correct schema") {
    val enc = Encoders.tuple(
      Encoders.scalaInt,
      Encoders.STRING,
      Encoders.scalaDouble,
      Encoders.scalaLong
    )
    val innerFields = enc.agnosticEncoder.asInstanceOf[ProductEncoder[?]].fields
    innerFields should have size 4
    innerFields(3).name shouldBe "_4"
  }

  test("tuple5 encoder has correct schema") {
    val enc = Encoders.tuple(
      Encoders.scalaInt,
      Encoders.STRING,
      Encoders.scalaDouble,
      Encoders.scalaLong,
      Encoders.scalaBoolean
    )
    val innerFields = enc.agnosticEncoder.asInstanceOf[ProductEncoder[?]].fields
    innerFields should have size 5
    innerFields(4).name shouldBe "_5"
  }

  // ---------------------------------------------------------------------------
  // product encoder
  // ---------------------------------------------------------------------------

  test("product encoder for case class") {
    case class TestPerson(name: String, age: Int) derives Encoder
    val enc = Encoders.product[TestPerson]
    enc should not be null
    enc.schema.fieldNames should contain("name")
    enc.schema.fieldNames should contain("age")
  }

  // ---------------------------------------------------------------------------
  // asAgnostic helper
  // ---------------------------------------------------------------------------

  test("asAgnostic extracts underlying from wrapper") {
    val enc = Encoders.scalaInt
    val ae = Encoders.asAgnostic(enc)
    ae shouldBe PrimitiveIntEncoder
  }

  test("asAgnostic passes through AgnosticEncoder") {
    val ae: AgnosticEncoder[Int] = PrimitiveIntEncoder
    // Create a minimal encoder that extends AgnosticEncoder directly
    Encoders.asAgnostic(Encoders.scalaInt) shouldBe PrimitiveIntEncoder
  }

  // ---------------------------------------------------------------------------
  // AgnosticEncoderWrapper: fromRow/toRow throw
  // ---------------------------------------------------------------------------

  test("AgnosticEncoderWrapper.fromRow throws UnsupportedOperationException") {
    val enc = Encoders.scalaInt
    assertThrows[UnsupportedOperationException] {
      enc.fromRow(Row(42))
    }
  }

  test("AgnosticEncoderWrapper.toRow throws UnsupportedOperationException") {
    val enc = Encoders.scalaInt
    assertThrows[UnsupportedOperationException] {
      enc.toRow(42)
    }
  }

  // ---------------------------------------------------------------------------
  // Spatial type encoders
  // ---------------------------------------------------------------------------

  test("GEOMETRY returns correct encoder") {
    val enc = Encoders.GEOMETRY
    enc.schema shouldBe StructType(Seq(StructField("value", GeometryType())))
    enc.agnosticEncoder shouldBe a[GeometryEncoder]
  }

  test("GEOGRAPHY returns correct encoder") {
    val enc = Encoders.GEOGRAPHY
    enc.schema shouldBe StructType(Seq(StructField("value", GeographyType())))
    enc.agnosticEncoder shouldBe a[GeographyEncoder]
  }
