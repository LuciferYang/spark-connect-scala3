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
