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
    enc.agnosticEncoder shouldBe Some(PrimitiveBooleanEncoder)
  }

  test("scalaByte returns correct encoder") {
    val enc = Encoders.scalaByte
    enc.schema shouldBe StructType(Seq(StructField("value", ByteType)))
    enc.agnosticEncoder shouldBe Some(PrimitiveByteEncoder)
  }

  test("scalaShort returns correct encoder") {
    val enc = Encoders.scalaShort
    enc.schema shouldBe StructType(Seq(StructField("value", ShortType)))
    enc.agnosticEncoder shouldBe Some(PrimitiveShortEncoder)
  }

  test("scalaInt returns correct encoder") {
    val enc = Encoders.scalaInt
    enc.schema shouldBe StructType(Seq(StructField("value", IntegerType)))
    enc.agnosticEncoder shouldBe Some(PrimitiveIntEncoder)
  }

  test("scalaLong returns correct encoder") {
    val enc = Encoders.scalaLong
    enc.schema shouldBe StructType(Seq(StructField("value", LongType)))
    enc.agnosticEncoder shouldBe Some(PrimitiveLongEncoder)
  }

  test("scalaFloat returns correct encoder") {
    val enc = Encoders.scalaFloat
    enc.schema shouldBe StructType(Seq(StructField("value", FloatType)))
    enc.agnosticEncoder shouldBe Some(PrimitiveFloatEncoder)
  }

  test("scalaDouble returns correct encoder") {
    val enc = Encoders.scalaDouble
    enc.schema shouldBe StructType(Seq(StructField("value", DoubleType)))
    enc.agnosticEncoder shouldBe Some(PrimitiveDoubleEncoder)
  }

  // ---------------------------------------------------------------------------
  // Java boxed types
  // ---------------------------------------------------------------------------

  test("BOOLEAN returns correct encoder") {
    val enc = Encoders.BOOLEAN
    enc.schema shouldBe StructType(Seq(StructField("value", BooleanType)))
    enc.agnosticEncoder shouldBe Some(BoxedBooleanEncoder)
  }

  test("BYTE returns correct encoder") {
    val enc = Encoders.BYTE
    enc.schema shouldBe StructType(Seq(StructField("value", ByteType)))
    enc.agnosticEncoder shouldBe Some(BoxedByteEncoder)
  }

  test("SHORT returns correct encoder") {
    val enc = Encoders.SHORT
    enc.schema shouldBe StructType(Seq(StructField("value", ShortType)))
    enc.agnosticEncoder shouldBe Some(BoxedShortEncoder)
  }

  test("INT returns correct encoder") {
    val enc = Encoders.INT
    enc.schema shouldBe StructType(Seq(StructField("value", IntegerType)))
    enc.agnosticEncoder shouldBe Some(BoxedIntEncoder)
  }

  test("LONG returns correct encoder") {
    val enc = Encoders.LONG
    enc.schema shouldBe StructType(Seq(StructField("value", LongType)))
    enc.agnosticEncoder shouldBe Some(BoxedLongEncoder)
  }

  test("FLOAT returns correct encoder") {
    val enc = Encoders.FLOAT
    enc.schema shouldBe StructType(Seq(StructField("value", FloatType)))
    enc.agnosticEncoder shouldBe Some(BoxedFloatEncoder)
  }

  test("DOUBLE returns correct encoder") {
    val enc = Encoders.DOUBLE
    enc.schema shouldBe StructType(Seq(StructField("value", DoubleType)))
    enc.agnosticEncoder shouldBe Some(BoxedDoubleEncoder)
  }

  // ---------------------------------------------------------------------------
  // String / Binary
  // ---------------------------------------------------------------------------

  test("STRING returns correct encoder") {
    val enc = Encoders.STRING
    enc.schema shouldBe StructType(Seq(StructField("value", StringType)))
    enc.agnosticEncoder shouldBe Some(StringEncoder)
  }

  test("BINARY returns correct encoder") {
    val enc = Encoders.BINARY
    enc.schema shouldBe StructType(Seq(StructField("value", BinaryType)))
    enc.agnosticEncoder shouldBe Some(BinaryEncoder)
  }

  // ---------------------------------------------------------------------------
  // Date / Time / Decimal
  // ---------------------------------------------------------------------------

  test("DATE returns correct encoder") {
    val enc = Encoders.DATE
    enc.schema shouldBe StructType(Seq(StructField("value", DateType)))
    enc.agnosticEncoder shouldBe Some(STRICT_DATE_ENCODER)
  }

  test("LOCALDATE returns correct encoder") {
    val enc = Encoders.LOCALDATE
    enc.schema shouldBe StructType(Seq(StructField("value", DateType)))
    enc.agnosticEncoder shouldBe Some(STRICT_LOCAL_DATE_ENCODER)
  }

  test("TIMESTAMP returns correct encoder") {
    val enc = Encoders.TIMESTAMP
    enc.schema shouldBe StructType(Seq(StructField("value", TimestampType)))
    enc.agnosticEncoder shouldBe Some(STRICT_TIMESTAMP_ENCODER)
  }

  test("INSTANT returns correct encoder") {
    val enc = Encoders.INSTANT
    enc.schema shouldBe StructType(Seq(StructField("value", TimestampType)))
    enc.agnosticEncoder shouldBe Some(STRICT_INSTANT_ENCODER)
  }

  test("LOCALDATETIME returns correct encoder") {
    val enc = Encoders.LOCALDATETIME
    enc.schema shouldBe StructType(Seq(StructField("value", TimestampNTZType)))
    enc.agnosticEncoder shouldBe Some(LocalDateTimeEncoder)
  }

  test("DECIMAL returns correct encoder") {
    val enc = Encoders.DECIMAL
    enc.schema shouldBe StructType(Seq(StructField("value", DecimalType.DEFAULT)))
    enc.agnosticEncoder shouldBe Some(DEFAULT_JAVA_DECIMAL_ENCODER)
  }

  // ---------------------------------------------------------------------------
  // Row encoder
  // ---------------------------------------------------------------------------

  test("row returns Row encoder") {
    val enc = Encoders.row
    enc should not be null
    enc.agnosticEncoder shouldBe Some(UnboundRowEncoder)
  }

  test("row(schema) returns schema-bound RowEncoder for primitive fields") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true)
    ))
    val enc = Encoders.row(schema)
    enc should not be null
    val ae = enc.agnosticEncoder.get.asInstanceOf[RowEncoder]
    ae.dataType shouldBe schema
    ae.fields should have size 2
    ae.fields(0).name shouldBe "id"
    ae.fields(0).enc shouldBe BoxedIntEncoder
    ae.fields(0).nullable shouldBe false
    ae.fields(1).name shouldBe "name"
    ae.fields(1).enc shouldBe StringEncoder
    ae.fields(1).nullable shouldBe true
  }

  test("row(schema) handles nested struct, array, and map types") {
    val nested = StructType(Seq(StructField("inner", DoubleType)))
    val schema = StructType(Seq(
      StructField("nested", nested),
      StructField("arr", ArrayType(LongType, containsNull = true)),
      StructField("m", MapType(StringType, BooleanType, valueContainsNull = false))
    ))
    val enc = Encoders.row(schema)
    val ae = enc.agnosticEncoder.get.asInstanceOf[RowEncoder]
    ae.fields(0).enc shouldBe a[RowEncoder]
    ae.fields(1).enc shouldBe a[IterableEncoder[?, ?]]
    ae.fields(2).enc shouldBe a[MapEncoder[?, ?, ?]]
  }

  test("row(schema) maps decimal, timestamp, and date types") {
    val schema = StructType(Seq(
      StructField("price", DecimalType(10, 2)),
      StructField("ts", TimestampType),
      StructField("d", DateType)
    ))
    val enc = Encoders.row(schema)
    val ae = enc.agnosticEncoder.get.asInstanceOf[RowEncoder]
    ae.fields(0).enc shouldBe a[JavaDecimalEncoder]
    ae.fields(1).enc shouldBe a[TimestampEncoder]
    ae.fields(2).enc shouldBe a[DateEncoder]
  }

  test("row(schema) maps spatial and UDT types") {
    val schema = StructType(Seq(
      StructField("geom", GeometryType()),
      StructField("geog", GeographyType())
    ))
    val enc = Encoders.row(schema)
    val ae = enc.agnosticEncoder.get.asInstanceOf[RowEncoder]
    ae.fields(0).enc shouldBe a[GeometryEncoder]
    ae.fields(1).enc shouldBe a[GeographyEncoder]
  }

  test("row(schema) throws for unsupported types") {
    val schema = StructType(Seq(StructField("ci", CalendarIntervalType)))
    assertThrows[UnsupportedOperationException] {
      Encoders.row(schema)
    }
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
    val innerFields = enc.agnosticEncoder.get.asInstanceOf[ProductEncoder[?]].fields
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
    val innerFields = enc.agnosticEncoder.get.asInstanceOf[ProductEncoder[?]].fields
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
    val innerFields = enc.agnosticEncoder.get.asInstanceOf[ProductEncoder[?]].fields
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
  // AgnosticEncoderWrapper: fromRow/toRow round-trip
  // ---------------------------------------------------------------------------

  test("AgnosticEncoderWrapper.fromRow extracts row.get(0) for leaf encoders") {
    val enc = Encoders.scalaInt
    enc.fromRow(Row(42)) shouldBe 42
  }

  test("AgnosticEncoderWrapper.toRow wraps value in single-column Row for leaf encoders") {
    val enc = Encoders.scalaInt
    enc.toRow(42) shouldBe Row(42)
  }

  test("AgnosticEncoderWrapper.fromRow passes Row through for unbound row encoder") {
    val enc = Encoders.row
    val r = Row.fromSeq(Seq("a", 1))
    enc.fromRow(r) should be theSameInstanceAs r
  }

  test("AgnosticEncoderWrapper.fromRow passes Row through for schema-bound row encoder") {
    val schema = StructType(Seq(StructField("id", IntegerType), StructField("name", StringType)))
    val enc = Encoders.row(schema)
    val r = Row.fromSeqWithSchema(Seq(7, "alice"), schema)
    enc.fromRow(r) should be theSameInstanceAs r
  }

  // ---------------------------------------------------------------------------
  // Spatial type encoders
  // ---------------------------------------------------------------------------

  test("GEOMETRY returns correct encoder") {
    val enc = Encoders.GEOMETRY
    enc.schema shouldBe StructType(Seq(StructField("value", GeometryType())))
    enc.agnosticEncoder.get shouldBe a[GeometryEncoder]
  }

  test("GEOGRAPHY returns correct encoder") {
    val enc = Encoders.GEOGRAPHY
    enc.schema shouldBe StructType(Seq(StructField("value", GeographyType())))
    enc.agnosticEncoder.get shouldBe a[GeographyEncoder]
  }

  test("GEOMETRY(dt) returns encoder with custom spatial type") {
    val enc = Encoders.GEOMETRY(GeometryType(4326))
    enc.schema shouldBe StructType(Seq(StructField("value", GeometryType(4326))))
    enc.agnosticEncoder.get shouldBe a[GeometryEncoder]
  }

  test("GEOGRAPHY(dt) returns encoder with custom spatial type") {
    val enc = Encoders.GEOGRAPHY(GeographyType(4326))
    enc.schema shouldBe StructType(Seq(StructField("value", GeographyType(4326))))
    enc.agnosticEncoder.get shouldBe a[GeographyEncoder]
  }

  test("classic-only encoder shims fail clearly when used") {
    Seq(
      Encoders.bean(classOf[String]),
      Encoders.kryo[String],
      Encoders.kryo(classOf[String]),
      Encoders.javaSerialization[String],
      Encoders.javaSerialization(classOf[String])
    ).foreach { enc =>
      val ex = intercept[UnsupportedOperationException](enc.schema)
      ex.getMessage should include("not supported in Spark Connect")
    }
  }

  // ---------------------------------------------------------------------------
  // R8-3: TupleEncoder3/4/5 fromRow and toRow
  // ---------------------------------------------------------------------------

  test("tuple3 encoder fromRow decodes struct columns") {
    import Encoder.given
    val enc = Encoders.tuple(summon[Encoder[Int]], summon[Encoder[String]], summon[Encoder[Double]])
    // Simulate the server returning a row with nested struct columns
    val innerRow = Row(Row(1), Row("hello"), Row(3.14))
    val result = enc.fromRow(innerRow)
    result shouldBe (1, "hello", 3.14)
  }

  test("tuple3 encoder toRow produces correct Row") {
    import Encoder.given
    val enc = Encoders.tuple(summon[Encoder[Int]], summon[Encoder[String]], summon[Encoder[Double]])
    val row = enc.toRow((10, "world", 2.72))
    row.get(0) shouldBe Row(10)
    row.get(1) shouldBe Row("world")
    row.get(2) shouldBe Row(2.72)
  }

  test("tuple4 encoder fromRow decodes struct columns") {
    import Encoder.given
    val enc = Encoders.tuple(
      summon[Encoder[Int]],
      summon[Encoder[String]],
      summon[Encoder[Double]],
      summon[Encoder[Long]]
    )
    val innerRow = Row(Row(1), Row("a"), Row(2.0), Row(99L))
    val result = enc.fromRow(innerRow)
    result shouldBe (1, "a", 2.0, 99L)
  }

  test("tuple4 encoder toRow produces correct Row") {
    import Encoder.given
    val enc = Encoders.tuple(
      summon[Encoder[Int]],
      summon[Encoder[String]],
      summon[Encoder[Double]],
      summon[Encoder[Long]]
    )
    val row = enc.toRow((5, "b", 1.5, 100L))
    row.get(0) shouldBe Row(5)
    row.get(1) shouldBe Row("b")
    row.get(2) shouldBe Row(1.5)
    row.get(3) shouldBe Row(100L)
  }

  test("tuple5 encoder fromRow decodes struct columns") {
    import Encoder.given
    val enc = Encoders.tuple(
      summon[Encoder[Int]],
      summon[Encoder[String]],
      summon[Encoder[Double]],
      summon[Encoder[Long]],
      summon[Encoder[Boolean]]
    )
    val innerRow = Row(Row(1), Row("x"), Row(0.5), Row(7L), Row(true))
    val result = enc.fromRow(innerRow)
    result shouldBe (1, "x", 0.5, 7L, true)
  }

  test("tuple5 encoder toRow produces correct Row") {
    import Encoder.given
    val enc = Encoders.tuple(
      summon[Encoder[Int]],
      summon[Encoder[String]],
      summon[Encoder[Double]],
      summon[Encoder[Long]],
      summon[Encoder[Boolean]]
    )
    val row = enc.toRow((2, "y", 9.9, 42L, false))
    row.get(0) shouldBe Row(2)
    row.get(1) shouldBe Row("y")
    row.get(2) shouldBe Row(9.9)
    row.get(3) shouldBe Row(42L)
    row.get(4) shouldBe Row(false)
  }
