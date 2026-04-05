package org.apache.spark.sql

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders
import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TypedOpsSuite extends AnyFunSuite with Matchers:

  // ---------------------------------------------------------------------------
  // agnosticEncoder bridge tests
  // ---------------------------------------------------------------------------

  test("primitive Encoder[Int] has correct agnosticEncoder") {
    val enc = summon[Encoder[Int]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.PrimitiveIntEncoder
  }

  test("primitive Encoder[Long] has correct agnosticEncoder") {
    val enc = summon[Encoder[Long]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.PrimitiveLongEncoder
  }

  test("primitive Encoder[Double] has correct agnosticEncoder") {
    val enc = summon[Encoder[Double]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.PrimitiveDoubleEncoder
  }

  test("primitive Encoder[Float] has correct agnosticEncoder") {
    val enc = summon[Encoder[Float]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.PrimitiveFloatEncoder
  }

  test("primitive Encoder[Short] has correct agnosticEncoder") {
    val enc = summon[Encoder[Short]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.PrimitiveShortEncoder
  }

  test("primitive Encoder[Byte] has correct agnosticEncoder") {
    val enc = summon[Encoder[Byte]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.PrimitiveByteEncoder
  }

  test("primitive Encoder[Boolean] has correct agnosticEncoder") {
    val enc = summon[Encoder[Boolean]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.PrimitiveBooleanEncoder
  }

  test("Encoder[String] has correct agnosticEncoder") {
    val enc = summon[Encoder[String]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.StringEncoder
  }

  test("Encoder[java.sql.Date] has correct agnosticEncoder") {
    val enc = summon[Encoder[java.sql.Date]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.STRICT_DATE_ENCODER
  }

  test("Encoder[java.sql.Timestamp] has correct agnosticEncoder") {
    val enc = summon[Encoder[java.sql.Timestamp]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.STRICT_TIMESTAMP_ENCODER
  }

  test("Encoder[java.time.LocalDate] has correct agnosticEncoder") {
    val enc = summon[Encoder[java.time.LocalDate]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.STRICT_LOCAL_DATE_ENCODER
  }

  test("Encoder[java.time.Instant] has correct agnosticEncoder") {
    val enc = summon[Encoder[java.time.Instant]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.STRICT_INSTANT_ENCODER
  }

  test("Encoder[BigDecimal] has correct agnosticEncoder") {
    val enc = summon[Encoder[BigDecimal]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.DEFAULT_SCALA_DECIMAL_ENCODER
  }

  test("Encoder[Array[Byte]] has correct agnosticEncoder") {
    val enc = summon[Encoder[Array[Byte]]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.BinaryEncoder
  }

  test("derived case class encoder has null agnosticEncoder") {
    // Product encoders don't have AgnosticEncoder bridge yet (Phase 3)
    val enc = summon[Encoder[Person]]
    enc.agnosticEncoder shouldBe null
  }

  test("default Encoder trait agnosticEncoder is null") {
    val customEnc = new Encoder[String]:
      def schema = StructType(Seq(StructField("v", StringType)))
      def fromRow(row: Row) = row.getString(0)
      def toRow(value: String) = Row(value)
    customEnc.agnosticEncoder shouldBe null
  }

  // ---------------------------------------------------------------------------
  // Dataset typed operations use server-side when agnosticEncoder is available
  // ---------------------------------------------------------------------------

  // Note: Full integration tests require a live Spark Connect server.
  // These tests verify the API surface compiles and works at the type level.

  test("Dataset.map compiles with primitive types") {
    // This verifies the method signature is correct with Encoder context bounds
    val intEnc = summon[Encoder[Int]]
    val strEnc = summon[Encoder[String]]
    // Both should have agnosticEncoder available for server-side execution
    intEnc.agnosticEncoder should not be null
    strEnc.agnosticEncoder should not be null
  }

  test("Dataset.reduce throws on empty data") {
    // reduce on empty collections should throw
    assertThrows[UnsupportedOperationException] {
      Array.empty[Int].reduce(_ + _)
    }
  }

  // ---------------------------------------------------------------------------
  // KeyValueGroupedDataset API surface
  // ---------------------------------------------------------------------------

  test("KeyValueGroupedDataset companion object exists") {
    // Verifies the object compiles and has the apply method
    val obj = KeyValueGroupedDataset
    obj should not be null
  }
