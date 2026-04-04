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
