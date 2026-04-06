package org.apache.spark.sql.internal

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.*
import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for TypedAggregators — proto-only, no live Spark Connect server needed. */
class TypedAggregatorsSuite extends AnyFunSuite with Matchers:

  // ---------------------------------------------------------------------------
  // TypedAverage
  // ---------------------------------------------------------------------------

  test("TypedAverage zero is (0.0, 0L)") {
    val agg = TypedAverage[Int](_.toDouble)
    agg.zero shouldBe (0.0, 0L)
  }

  test("TypedAverage reduce accumulates sum and count") {
    val agg = TypedAverage[Int](_.toDouble)
    val r1 = agg.reduce(agg.zero, 10)
    r1 shouldBe (10.0, 1L)
    val r2 = agg.reduce(r1, 20)
    r2 shouldBe (30.0, 2L)
  }

  test("TypedAverage merge combines buffers") {
    val agg = TypedAverage[Int](_.toDouble)
    val b1 = (10.0, 2L)
    val b2 = (20.0, 3L)
    agg.merge(b1, b2) shouldBe (30.0, 5L)
  }

  test("TypedAverage finish computes average") {
    val agg = TypedAverage[Int](_.toDouble)
    agg.finish((30.0, 3L)) shouldBe 10.0
  }

  test("TypedAverage bufferEncoder is tuple(Double, Long)") {
    val agg = TypedAverage[Int](_.toDouble)
    val bufEnc = agg.bufferEncoder
    bufEnc should not be null
    val ae = bufEnc.agnosticEncoder.asInstanceOf[ProductEncoder[?]]
    ae.fields should have size 2
    ae.fields(0).enc shouldBe PrimitiveDoubleEncoder
    ae.fields(1).enc shouldBe PrimitiveLongEncoder
  }

  test("TypedAverage outputEncoder is scalaDouble") {
    val agg = TypedAverage[Int](_.toDouble)
    agg.outputEncoder.agnosticEncoder shouldBe PrimitiveDoubleEncoder
  }

  // ---------------------------------------------------------------------------
  // TypedCount
  // ---------------------------------------------------------------------------

  test("TypedCount zero is 0L") {
    val agg = TypedCount[String](identity)
    agg.zero shouldBe 0L
  }

  test("TypedCount reduce increments for non-null") {
    val agg = TypedCount[String](identity)
    val r1 = agg.reduce(0L, "hello")
    r1 shouldBe 1L
    val r2 = agg.reduce(r1, "world")
    r2 shouldBe 2L
  }

  test("TypedCount reduce does not increment for null") {
    val agg = TypedCount[String](identity)
    val r1 = agg.reduce(0L, null)
    r1 shouldBe 0L
  }

  test("TypedCount merge sums counts") {
    val agg = TypedCount[String](identity)
    agg.merge(5L, 3L) shouldBe 8L
  }

  test("TypedCount finish returns the count") {
    val agg = TypedCount[String](identity)
    agg.finish(42L) shouldBe 42L
  }

  test("TypedCount bufferEncoder is scalaLong") {
    val agg = TypedCount[String](identity)
    agg.bufferEncoder.agnosticEncoder shouldBe PrimitiveLongEncoder
  }

  test("TypedCount outputEncoder is scalaLong") {
    val agg = TypedCount[String](identity)
    agg.outputEncoder.agnosticEncoder shouldBe PrimitiveLongEncoder
  }

  // ---------------------------------------------------------------------------
  // TypedSumDouble
  // ---------------------------------------------------------------------------

  test("TypedSumDouble zero is 0.0") {
    val agg = TypedSumDouble[Int](_.toDouble)
    agg.zero shouldBe 0.0
  }

  test("TypedSumDouble reduce accumulates sum") {
    val agg = TypedSumDouble[Int](_.toDouble)
    val r1 = agg.reduce(0.0, 10)
    r1 shouldBe 10.0
    val r2 = agg.reduce(r1, 20)
    r2 shouldBe 30.0
  }

  test("TypedSumDouble merge sums buffers") {
    val agg = TypedSumDouble[Int](_.toDouble)
    agg.merge(10.0, 20.0) shouldBe 30.0
  }

  test("TypedSumDouble finish returns the sum") {
    val agg = TypedSumDouble[Int](_.toDouble)
    agg.finish(42.0) shouldBe 42.0
  }

  test("TypedSumDouble bufferEncoder is scalaDouble") {
    val agg = TypedSumDouble[Int](_.toDouble)
    agg.bufferEncoder.agnosticEncoder shouldBe PrimitiveDoubleEncoder
  }

  test("TypedSumDouble outputEncoder is scalaDouble") {
    val agg = TypedSumDouble[Int](_.toDouble)
    agg.outputEncoder.agnosticEncoder shouldBe PrimitiveDoubleEncoder
  }

  // ---------------------------------------------------------------------------
  // TypedSumLong
  // ---------------------------------------------------------------------------

  test("TypedSumLong zero is 0L") {
    val agg = TypedSumLong[Int](_.toLong)
    agg.zero shouldBe 0L
  }

  test("TypedSumLong reduce accumulates sum") {
    val agg = TypedSumLong[Int](_.toLong)
    val r1 = agg.reduce(0L, 10)
    r1 shouldBe 10L
    val r2 = agg.reduce(r1, 20)
    r2 shouldBe 30L
  }

  test("TypedSumLong merge sums buffers") {
    val agg = TypedSumLong[Int](_.toLong)
    agg.merge(10L, 20L) shouldBe 30L
  }

  test("TypedSumLong finish returns the sum") {
    val agg = TypedSumLong[Int](_.toLong)
    agg.finish(42L) shouldBe 42L
  }

  test("TypedSumLong bufferEncoder is scalaLong") {
    val agg = TypedSumLong[Int](_.toLong)
    agg.bufferEncoder.agnosticEncoder shouldBe PrimitiveLongEncoder
  }

  test("TypedSumLong outputEncoder is scalaLong") {
    val agg = TypedSumLong[Int](_.toLong)
    agg.outputEncoder.agnosticEncoder shouldBe PrimitiveLongEncoder
  }

  // ---------------------------------------------------------------------------
  // End-to-end reduce/merge sequences
  // ---------------------------------------------------------------------------

  test("TypedAverage end-to-end with Doubles") {
    val agg = TypedAverage[Double](identity)
    val values = Seq(1.0, 2.0, 3.0, 4.0, 5.0)
    val result = values.foldLeft(agg.zero)(agg.reduce)
    agg.finish(result) shouldBe 3.0
  }

  test("TypedCount end-to-end with mixed nulls") {
    val agg = TypedCount[String](identity)
    val values = Seq("a", null, "b", null, "c")
    val result = values.foldLeft(agg.zero)(agg.reduce)
    agg.finish(result) shouldBe 3L
  }

  test("TypedSumDouble end-to-end") {
    val agg = TypedSumDouble[Double](identity)
    val values = Seq(1.5, 2.5, 3.0)
    val result = values.foldLeft(agg.zero)(agg.reduce)
    agg.finish(result) shouldBe 7.0
  }

  test("TypedSumLong end-to-end") {
    val agg = TypedSumLong[Long](identity)
    val values = Seq(10L, 20L, 30L)
    val result = values.foldLeft(agg.zero)(agg.reduce)
    agg.finish(result) shouldBe 60L
  }

  // ---------------------------------------------------------------------------
  // Merge with zero
  // ---------------------------------------------------------------------------

  test("TypedAverage merge with zero") {
    val agg = TypedAverage[Int](_.toDouble)
    val buf = (10.0, 2L)
    agg.merge(agg.zero, buf) shouldBe buf
    agg.merge(buf, agg.zero) shouldBe buf
  }

  test("TypedSumDouble merge with zero") {
    val agg = TypedSumDouble[Int](_.toDouble)
    agg.merge(agg.zero, 5.0) shouldBe 5.0
    agg.merge(5.0, agg.zero) shouldBe 5.0
  }

  test("TypedSumLong merge with zero") {
    val agg = TypedSumLong[Int](_.toLong)
    agg.merge(agg.zero, 5L) shouldBe 5L
    agg.merge(5L, agg.zero) shouldBe 5L
  }
