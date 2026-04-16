package org.apache.spark.sql.types

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataTypeSuite extends AnyFunSuite with Matchers:

  test("primitive type names") {
    BooleanType.typeName shouldBe "boolean"
    ByteType.typeName shouldBe "byte"
    ShortType.typeName shouldBe "short"
    IntegerType.typeName shouldBe "integer"
    LongType.typeName shouldBe "long"
    FloatType.typeName shouldBe "float"
    DoubleType.typeName shouldBe "double"
    StringType.typeName shouldBe "string"
    BinaryType.typeName shouldBe "binary"
    DateType.typeName shouldBe "date"
    TimestampType.typeName shouldBe "timestamp"
    TimestampNTZType.typeName shouldBe "timestamp_ntz"
    NullType.typeName shouldBe "null"
    VariantType.typeName shouldBe "variant"
  }

  test("DecimalType simpleString") {
    DecimalType(10, 2).simpleString shouldBe "decimal(10,2)"
    DecimalType.DEFAULT.simpleString shouldBe "decimal(10,0)"
  }

  test("ArrayType simpleString") {
    ArrayType(IntegerType, containsNull = true).simpleString shouldBe "array<integer>"
    ArrayType(StringType, containsNull = false).simpleString shouldBe "array<string>"
  }

  test("MapType simpleString") {
    MapType(
      StringType,
      IntegerType,
      valueContainsNull = true
    ).simpleString shouldBe "map<string,integer>"
  }

  test("StructType basics") {
    val st = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType, nullable = false)
    ))
    st.fieldNames shouldBe Array("name", "age")
    st("name") shouldBe StructField("name", StringType)
    st("age").nullable shouldBe false
  }

  test("StructType.empty") {
    StructType.empty.fields shouldBe empty
    StructType.empty.fieldNames shouldBe empty
  }

  test("StructType simpleString") {
    val st = StructType(Seq(
      StructField("id", LongType),
      StructField("name", StringType)
    ))
    st.simpleString shouldBe "struct<id:long,name:string>"
  }

  test("StructType treeString") {
    val st = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = true)
    ))
    val tree = st.treeString
    tree should include("root")
    tree should include("|-- id: long (nullable = false)")
    tree should include("|-- name: string (nullable = true)")
  }

  test("StructType apply throws for missing field") {
    val st = StructType(Seq(StructField("x", IntegerType)))
    assertThrows[java.util.NoSuchElementException] {
      st("y")
    }
  }

  test("StructType.fieldIndex returns correct index") {
    val st = StructType(
      Seq(
        StructField("a", IntegerType),
        StructField("b", StringType),
        StructField("c", LongType)
      )
    )
    st.fieldIndex("a") shouldBe 0
    st.fieldIndex("b") shouldBe 1
    st.fieldIndex("c") shouldBe 2
  }

  test("StructType.fieldIndex throws for missing field") {
    val st = StructType(Seq(StructField("x", IntegerType)))
    an[IllegalArgumentException] should be thrownBy st.fieldIndex("y")
  }

  // ---------------------------------------------------------------------------
  // SQL representations
  // ---------------------------------------------------------------------------

  test("primitive type SQL representations") {
    BooleanType.sql shouldBe "BOOLEAN"
    ByteType.sql shouldBe "TINYINT"
    ShortType.sql shouldBe "SMALLINT"
    IntegerType.sql shouldBe "INT"
    LongType.sql shouldBe "BIGINT"
    FloatType.sql shouldBe "FLOAT"
    DoubleType.sql shouldBe "DOUBLE"
    StringType.sql shouldBe "STRING"
    BinaryType.sql shouldBe "BINARY"
    DateType.sql shouldBe "DATE"
    TimestampType.sql shouldBe "TIMESTAMP"
    TimestampNTZType.sql shouldBe "TIMESTAMP_NTZ"
    NullType.sql shouldBe "VOID"
    VariantType.sql shouldBe "VARIANT"
  }

  test("DecimalType SQL representation") {
    DecimalType(10, 2).sql shouldBe "DECIMAL(10,2)"
    DecimalType.DEFAULT.sql shouldBe "DECIMAL(10,0)"
  }

  test("ArrayType SQL representation") {
    ArrayType(IntegerType, containsNull = true).sql shouldBe "ARRAY<INT>"
    ArrayType(StringType, containsNull = false).sql shouldBe "ARRAY<STRING>"
  }

  test("MapType SQL representation") {
    MapType(StringType, IntegerType, valueContainsNull = true).sql shouldBe "MAP<STRING,INT>"
  }

  // ---------------------------------------------------------------------------
  // simpleString for more types
  // ---------------------------------------------------------------------------

  test("primitive types simpleString matches typeName") {
    BooleanType.simpleString shouldBe "boolean"
    ByteType.simpleString shouldBe "byte"
    ShortType.simpleString shouldBe "short"
    IntegerType.simpleString shouldBe "integer"
    LongType.simpleString shouldBe "long"
    FloatType.simpleString shouldBe "float"
    DoubleType.simpleString shouldBe "double"
    StringType.simpleString shouldBe "string"
    BinaryType.simpleString shouldBe "binary"
    DateType.simpleString shouldBe "date"
    TimestampType.simpleString shouldBe "timestamp"
    TimestampNTZType.simpleString shouldBe "timestamp_ntz"
    NullType.simpleString shouldBe "null"
    VariantType.simpleString shouldBe "variant"
  }

  test("nested ArrayType simpleString") {
    val nested = ArrayType(ArrayType(IntegerType, false), true)
    nested.simpleString shouldBe "array<array<integer>>"
  }

  test("nested MapType simpleString") {
    val nested = MapType(StringType, ArrayType(IntegerType, false), true)
    nested.simpleString shouldBe "map<string,array<integer>>"
  }

  // ---------------------------------------------------------------------------
  // StructType toDDL
  // ---------------------------------------------------------------------------

  test("StructType toDDL with nullable and non-nullable fields") {
    val st = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = true)
    ))
    st.toDDL shouldBe "id BIGINT NOT NULL, name STRING"
  }

  test("StructType.empty toDDL is empty string") {
    StructType.empty.toDDL shouldBe ""
  }

  test("StructType toDDL with DecimalType") {
    val st = StructType(Seq(
      StructField("amount", DecimalType(10, 2))
    ))
    st.toDDL shouldBe "amount DECIMAL(10,2)"
  }

  // ---------------------------------------------------------------------------
  // StructType treeString with nesting
  // ---------------------------------------------------------------------------

  test("StructType treeString with nested struct") {
    val inner = StructType(Seq(
      StructField("x", IntegerType, nullable = false)
    ))
    val st = StructType(Seq(
      StructField("name", StringType),
      StructField("nested", inner)
    ))
    val tree = st.treeString
    tree should include("root")
    tree should include("|-- name: string (nullable = true)")
    tree should include("|-- nested: struct<x:integer> (nullable = true)")
    tree should include(" |-- x: integer (nullable = false)")
  }

  test("StructType treeString with maxLevel limits nesting") {
    val inner = StructType(Seq(
      StructField("x", IntegerType, nullable = false)
    ))
    val st = StructType(Seq(
      StructField("nested", inner)
    ))
    // maxLevel=2 renders top-level fields (indent 1 < 2) but not nested (indent 2 < 2 = false)
    val tree = st.treeString(2)
    tree should include("root")
    tree should include("|-- nested")
    // The inner field at level 2 should not be rendered
    tree should not include " |-- x"
  }

  // ---------------------------------------------------------------------------
  // DecimalType DEFAULT
  // ---------------------------------------------------------------------------

  test("DecimalType.DEFAULT is (10, 0)") {
    DecimalType.DEFAULT.precision shouldBe 10
    DecimalType.DEFAULT.scale shouldBe 0
  }

  // ---------------------------------------------------------------------------
  // StructField defaults
  // ---------------------------------------------------------------------------

  test("StructField default nullable is true") {
    val f = StructField("x", IntegerType)
    f.nullable shouldBe true
  }

  test("StructField explicit nullable false") {
    val f = StructField("x", IntegerType, nullable = false)
    f.nullable shouldBe false
  }

  // ---------------------------------------------------------------------------
  // typeName for complex types
  // ---------------------------------------------------------------------------

  test("ArrayType typeName") {
    ArrayType(IntegerType, true).typeName shouldBe "array"
  }

  test("MapType typeName") {
    MapType(StringType, IntegerType, true).typeName shouldBe "map"
  }

  test("StructType typeName") {
    StructType(Seq.empty).typeName shouldBe "struct"
  }

  test("DecimalType typeName") {
    DecimalType(10, 2).typeName shouldBe "decimal"
  }

  // ---------------------------------------------------------------------------
  // Interval types
  // ---------------------------------------------------------------------------

  test("DayTimeIntervalType typeName and sql") {
    DayTimeIntervalType.typeName shouldBe "day_time_interval"
    DayTimeIntervalType.sql shouldBe "INTERVAL DAY TO SECOND"
  }

  test("YearMonthIntervalType typeName and sql") {
    YearMonthIntervalType.typeName shouldBe "year_month_interval"
    YearMonthIntervalType.sql shouldBe "INTERVAL YEAR TO MONTH"
  }

  test("DayTimeIntervalType simpleString matches typeName") {
    DayTimeIntervalType.simpleString shouldBe "day_time_interval"
  }

  test("YearMonthIntervalType simpleString matches typeName") {
    YearMonthIntervalType.simpleString shouldBe "year_month_interval"
  }
