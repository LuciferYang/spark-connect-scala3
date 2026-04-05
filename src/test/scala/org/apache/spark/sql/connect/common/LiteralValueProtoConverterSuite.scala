package org.apache.spark.sql.connect.common

import com.google.protobuf.ByteString
import org.apache.spark.connect.proto.{DataType as ProtoDataType, Expression}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class LiteralValueProtoConverterSuite extends AnyFunSuite with Matchers:

  private def lit = Expression.Literal.newBuilder()

  test("null literal") {
    val proto = lit.setNull(ProtoDataType.newBuilder()
      .setNull(ProtoDataType.NULL.getDefaultInstance).build()).build()
    LiteralValueProtoConverter.toScalaValue(proto) shouldBe (null: Any)
  }

  test("boolean literal") {
    val proto = lit.setBoolean(true).build()
    LiteralValueProtoConverter.toScalaValue(proto) shouldBe true
    LiteralValueProtoConverter.toDataType(proto) shouldBe BooleanType
  }

  test("byte literal") {
    val proto = lit.setByte(42).build()
    LiteralValueProtoConverter.toScalaValue(proto) shouldBe 42.toByte
    LiteralValueProtoConverter.toDataType(proto) shouldBe ByteType
  }

  test("short literal") {
    val proto = lit.setShort(1000).build()
    LiteralValueProtoConverter.toScalaValue(proto) shouldBe 1000.toShort
    LiteralValueProtoConverter.toDataType(proto) shouldBe ShortType
  }

  test("integer literal") {
    val proto = lit.setInteger(42).build()
    LiteralValueProtoConverter.toScalaValue(proto) shouldBe 42
    LiteralValueProtoConverter.toDataType(proto) shouldBe IntegerType
  }

  test("long literal") {
    val proto = lit.setLong(123456789L).build()
    LiteralValueProtoConverter.toScalaValue(proto) shouldBe 123456789L
    LiteralValueProtoConverter.toDataType(proto) shouldBe LongType
  }

  test("float literal") {
    val proto = lit.setFloat(3.14f).build()
    LiteralValueProtoConverter.toScalaValue(proto) shouldBe 3.14f
    LiteralValueProtoConverter.toDataType(proto) shouldBe FloatType
  }

  test("double literal") {
    val proto = lit.setDouble(2.718).build()
    LiteralValueProtoConverter.toScalaValue(proto) shouldBe 2.718
    LiteralValueProtoConverter.toDataType(proto) shouldBe DoubleType
  }

  test("string literal") {
    val proto = lit.setString("hello").build()
    LiteralValueProtoConverter.toScalaValue(proto) shouldBe "hello"
    LiteralValueProtoConverter.toDataType(proto) shouldBe StringType
  }

  test("binary literal") {
    val bytes = Array[Byte](1, 2, 3)
    val proto = lit.setBinary(ByteString.copyFrom(bytes)).build()
    LiteralValueProtoConverter.toScalaValue(proto) shouldBe bytes
    LiteralValueProtoConverter.toDataType(proto) shouldBe BinaryType
  }

  test("date literal") {
    val epochDay = 19000 // some date
    val proto = lit.setDate(epochDay).build()
    val result = LiteralValueProtoConverter.toScalaValue(proto)
    result shouldBe java.time.LocalDate.ofEpochDay(epochDay.toLong)
    LiteralValueProtoConverter.toDataType(proto) shouldBe DateType
  }

  test("timestamp literal") {
    val micros = 1000000L // 1 second in micros
    val proto = lit.setTimestamp(micros).build()
    val result = LiteralValueProtoConverter.toScalaValue(proto)
    result shouldBe java.time.Instant.ofEpochSecond(0, micros * 1000)
    LiteralValueProtoConverter.toDataType(proto) shouldBe TimestampType
  }

  test("decimal literal") {
    val proto = lit.setDecimal(
      Expression.Literal.Decimal.newBuilder()
        .setValue("123.45")
        .setPrecision(5)
        .setScale(2)
        .build()
    ).build()
    LiteralValueProtoConverter.toScalaValue(proto) shouldBe BigDecimal("123.45")
    LiteralValueProtoConverter.toDataType(proto) shouldBe DecimalType(5, 2)
  }

  test("array literal") {
    val arrBuilder = Expression.Literal.Array.newBuilder()
      .addElements(Expression.Literal.newBuilder().setInteger(1).build())
      .addElements(Expression.Literal.newBuilder().setInteger(2).build())
      .addElements(Expression.Literal.newBuilder().setInteger(3).build())
    val proto = lit.setArray(arrBuilder.build()).build()
    val result = LiteralValueProtoConverter.toScalaValue(proto).asInstanceOf[Array[?]]
    result.toSeq shouldBe Seq(1, 2, 3)
  }

  test("map literal") {
    val mapBuilder = Expression.Literal.Map.newBuilder()
      .addKeys(Expression.Literal.newBuilder().setString("a").build())
      .addKeys(Expression.Literal.newBuilder().setString("b").build())
      .addValues(Expression.Literal.newBuilder().setInteger(1).build())
      .addValues(Expression.Literal.newBuilder().setInteger(2).build())
    val proto = lit.setMap(mapBuilder.build()).build()
    val result = LiteralValueProtoConverter.toScalaValue(proto).asInstanceOf[Map[?, ?]]
    result shouldBe Map("a" -> 1, "b" -> 2)
  }

  test("struct literal") {
    val structBuilder = Expression.Literal.Struct.newBuilder()
      .addElements(Expression.Literal.newBuilder().setString("hello").build())
      .addElements(Expression.Literal.newBuilder().setInteger(42).build())
    val proto = lit.setStruct(structBuilder.build()).build()
    val result = LiteralValueProtoConverter.toScalaValue(proto).asInstanceOf[Row]
    result.getString(0) shouldBe "hello"
    result.getInt(1) shouldBe 42
  }

  test("toDataType with explicit data_type field") {
    val dataType = ProtoDataType.newBuilder()
      .setLong(ProtoDataType.Long.getDefaultInstance).build()
    val proto = lit.setLong(42L).setDataType(dataType).build()
    LiteralValueProtoConverter.toDataType(proto) shouldBe LongType
  }

  test("literaltype not set returns null") {
    val proto = Expression.Literal.getDefaultInstance
    LiteralValueProtoConverter.toScalaValue(proto) shouldBe (null: Any)
  }
