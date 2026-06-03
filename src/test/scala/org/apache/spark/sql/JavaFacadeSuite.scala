package org.apache.spark.sql

import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JavaFacadeSuite extends AnyFunSuite with Matchers:

  test("RowFactory creates Row values") {
    val row = RowFactory.create("a", Int.box(1), null)

    row.size shouldBe 3
    row.getString(0) shouldBe "a"
    row.getInt(1) shouldBe 1
    row.isNullAt(2) shouldBe true
  }

  test("DataTypes exposes primitive singleton fields") {
    DataTypes.StringType shouldBe StringType
    DataTypes.IntegerType shouldBe IntegerType
    DataTypes.BooleanType shouldBe BooleanType
  }

  test("DataTypes creates composite and decimal types") {
    DataTypes.createArrayType(StringType).containsNull shouldBe true
    DataTypes.createArrayType(StringType, false).containsNull shouldBe false
    DataTypes.createMapType(StringType, IntegerType, false).valueContainsNull shouldBe false
    DataTypes.createDecimalType(12, 2) shouldBe DecimalType(12, 2)
    DataTypes.createDecimalType() shouldBe DecimalType.DEFAULT
  }

  test("DataTypes creates StructField and StructType") {
    val metadata = new MetadataBuilder().putString("comment", "user id").build()
    val field = DataTypes.createStructField("id", LongType, false, metadata)
    val schema = DataTypes.createStructType(Array(field))

    field.metadata shouldBe metadata
    schema("id").dataType shouldBe LongType
    schema("id").nullable shouldBe false
  }

  test("DataTypes rejects duplicate StructType field names") {
    val fields = Array(
      DataTypes.createStructField("id", LongType, false),
      DataTypes.createStructField("id", StringType, true)
    )

    an[IllegalArgumentException] should be thrownBy DataTypes.createStructType(fields)
  }
