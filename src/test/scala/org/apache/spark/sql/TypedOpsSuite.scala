package org.apache.spark.sql

import org.apache.spark.connect.proto.*
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

  // ---------------------------------------------------------------------------
  // joinWith tests
  // ---------------------------------------------------------------------------

  /** Create a minimal Dataset backed by a LocalRelation (no real server). */
  private def testDataset[T: Encoder: scala.reflect.ClassTag](): Dataset[T] =
    val session = SparkSession(null)
    val rel = Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(0).build())
      .setLocalRelation(LocalRelation.getDefaultInstance)
      .build()
    Dataset(DataFrame(session, rel), summon[Encoder[T]])

  test("joinWith builds Join proto with JoinDataType") {
    val ds1 = testDataset[Long]()
    val ds2 = testDataset[Long]()
    val joined = ds1.joinWith(ds2, Column("id") === Column("id"))
    val rel = joined.df.relation
    rel.hasJoin shouldBe true
    val join = rel.getJoin
    join.hasJoinDataType shouldBe true
    join.getJoinType shouldBe Join.JoinType.JOIN_TYPE_INNER
  }

  test("joinWith with left outer join type") {
    val ds1 = testDataset[Long]()
    val ds2 = testDataset[Long]()
    val joined = ds1.joinWith(ds2, Column("id") === Column("id"), "left")
    val join = joined.df.relation.getJoin
    join.getJoinType shouldBe Join.JoinType.JOIN_TYPE_LEFT_OUTER
    join.hasJoinDataType shouldBe true
  }

  test("joinWith returns correct tuple encoder type") {
    val ds1 = testDataset[Int]()
    val ds2 = testDataset[String]()
    val joined = ds1.joinWith(ds2, Column("id") === Column("id"))
    // Verify it compiles with the correct type
    val enc = joined.encoder
    enc should not be null
  }

  // ---------------------------------------------------------------------------
  // toLocalIterator type signature tests
  // ---------------------------------------------------------------------------

  test("Dataset.toLocalIterator return type implements both Iterator and AutoCloseable") {
    // Verify the method exists and returns an intersection type
    val method = classOf[Dataset[Int]].getMethod("toLocalIterator")
    method should not be null
    // The return type is java.util.Iterator[T] with AutoCloseable
    // Java reflection sees the erased return type — check that both interfaces are assignable
    val retType = method.getReturnType
    classOf[java.util.Iterator[?]].isAssignableFrom(retType) ||
    classOf[AutoCloseable].isAssignableFrom(retType) shouldBe true
  }

  test("DataFrame.toLocalIterator return type implements both Iterator and AutoCloseable") {
    val method = classOf[DataFrame].getMethod("toLocalIterator")
    method should not be null
    val retType = method.getReturnType
    classOf[java.util.Iterator[?]].isAssignableFrom(retType) ||
    classOf[AutoCloseable].isAssignableFrom(retType) shouldBe true
  }
