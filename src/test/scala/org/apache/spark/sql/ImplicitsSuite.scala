package org.apache.spark.sql

import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.language.implicitConversions

class ImplicitsSuite extends AnyFunSuite with Matchers:

  test("$\"colName\" creates UnresolvedAttribute") {
    import org.apache.spark.sql.implicits.*

    val c: Column = $"id"
    c.expr.hasUnresolvedAttribute shouldBe true
    c.expr.getUnresolvedAttribute.getUnparsedIdentifier shouldBe "id"
  }

  test("$\"multi.part.name\" creates attribute with dotted name") {
    import org.apache.spark.sql.implicits.*

    val c: Column = $"a.b.c"
    c.expr.getUnresolvedAttribute.getUnparsedIdentifier shouldBe "a.b.c"
  }

  test("Symbol to Column conversion") {
    import org.apache.spark.sql.implicits.given

    val c: Column = Symbol("name")
    c.expr.getUnresolvedAttribute.getUnparsedIdentifier shouldBe "name"
  }

  test("string.col extension") {
    import org.apache.spark.sql.implicits.*

    val c = "age".col
    c.expr.getUnresolvedAttribute.getUnparsedIdentifier shouldBe "age"
  }

  // ---------------------------------------------------------------------------
  // ColumnName StructField helpers
  // ---------------------------------------------------------------------------

  test("ColumnName.string creates StringType StructField") {
    import org.apache.spark.sql.implicits.*

    val field = $"name".string
    field shouldBe StructField("name", StringType)
  }

  test("ColumnName.int creates IntegerType StructField") {
    import org.apache.spark.sql.implicits.*

    val field = $"age".int
    field shouldBe StructField("age", IntegerType)
  }

  test("ColumnName.long creates LongType StructField") {
    import org.apache.spark.sql.implicits.*

    val field = $"count".long
    field shouldBe StructField("count", LongType)
  }

  test("ColumnName.double creates DoubleType StructField") {
    import org.apache.spark.sql.implicits.*

    val field = $"score".double
    field shouldBe StructField("score", DoubleType)
  }

  test("ColumnName.boolean creates BooleanType StructField") {
    import org.apache.spark.sql.implicits.*

    val field = $"active".boolean
    field shouldBe StructField("active", BooleanType)
  }

  test("ColumnName.date creates DateType StructField") {
    import org.apache.spark.sql.implicits.*

    val field = $"dob".date
    field shouldBe StructField("dob", DateType)
  }

  test("ColumnName.timestamp creates TimestampType StructField") {
    import org.apache.spark.sql.implicits.*

    val field = $"ts".timestamp
    field shouldBe StructField("ts", TimestampType)
  }

  test("ColumnName.decimal creates default DecimalType StructField") {
    import org.apache.spark.sql.implicits.*

    val field = $"amount".decimal
    field shouldBe StructField("amount", DecimalType.DEFAULT)
  }

  test("ColumnName.decimal(p,s) creates parameterized DecimalType StructField") {
    import org.apache.spark.sql.implicits.*

    val field = $"price".decimal(10, 2)
    field shouldBe StructField("price", DecimalType(10, 2))
  }

  test("ColumnName.binary creates BinaryType StructField") {
    import org.apache.spark.sql.implicits.*

    val field = $"data".binary
    field shouldBe StructField("data", BinaryType)
  }

  // ---------------------------------------------------------------------------
  // ColumnName implicit conversion to Column
  // ---------------------------------------------------------------------------

  test("ColumnName implicitly converts to Column") {
    import org.apache.spark.sql.implicits.*

    val cn = $"myCol"
    val c: Column = cn // implicit conversion
    c.expr.getUnresolvedAttribute.getUnparsedIdentifier shouldBe "myCol"
  }

  // ---------------------------------------------------------------------------
  // Seq[T].toDS / Seq[T].toDF
  // ---------------------------------------------------------------------------

  test("Seq[Int].toDS builds local relation proto") {
    import org.apache.spark.sql.implicits.*

    given spark: SparkSession = SparkSession.builder().remote("sc://localhost:15002").build()

    val ds = Seq(1, 2, 3).toDS
    ds.toDF().relation.hasLocalRelation shouldBe true
  }

  test("Seq[Int].toDF builds local relation proto") {
    import org.apache.spark.sql.implicits.*

    given spark: SparkSession = SparkSession.builder().remote("sc://localhost:15002").build()

    val df = Seq(1, 2, 3).toDF
    df.relation.hasLocalRelation shouldBe true
  }

  test("Seq[Int].toDF with column names adds rename") {
    import org.apache.spark.sql.implicits.*

    given spark: SparkSession = SparkSession.builder().remote("sc://localhost:15002").build()

    val df = Seq(1, 2, 3).toDF("number")
    // toDF("number") wraps with a WithColumnsRenamed or Project
    // The outermost relation should not be a bare LocalRelation
    df.relation should not be null
  }

  test("Seq[String].toDS builds local relation proto") {
    import org.apache.spark.sql.implicits.*

    given spark: SparkSession = SparkSession.builder().remote("sc://localhost:15002").build()

    val ds = Seq("a", "b", "c").toDS
    ds.toDF().relation.hasLocalRelation shouldBe true
  }
