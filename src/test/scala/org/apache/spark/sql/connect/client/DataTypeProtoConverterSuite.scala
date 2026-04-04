package org.apache.spark.sql.connect.client

import org.apache.spark.connect.proto.{DataType as ProtoDataType}
import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataTypeProtoConverterSuite extends AnyFunSuite with Matchers:

  private def roundTrip(dt: DataType, proto: ProtoDataType): Unit =
    val result = DataTypeProtoConverter.fromProto(proto)
    result shouldBe dt

  test("primitive types round-trip") {
    roundTrip(BooleanType,
      ProtoDataType.newBuilder().setBoolean(ProtoDataType.Boolean.getDefaultInstance).build())
    roundTrip(ByteType,
      ProtoDataType.newBuilder().setByte(ProtoDataType.Byte.getDefaultInstance).build())
    roundTrip(ShortType,
      ProtoDataType.newBuilder().setShort(ProtoDataType.Short.getDefaultInstance).build())
    roundTrip(IntegerType,
      ProtoDataType.newBuilder().setInteger(ProtoDataType.Integer.getDefaultInstance).build())
    roundTrip(LongType,
      ProtoDataType.newBuilder().setLong(ProtoDataType.Long.getDefaultInstance).build())
    roundTrip(FloatType,
      ProtoDataType.newBuilder().setFloat(ProtoDataType.Float.getDefaultInstance).build())
    roundTrip(DoubleType,
      ProtoDataType.newBuilder().setDouble(ProtoDataType.Double.getDefaultInstance).build())
    roundTrip(StringType,
      ProtoDataType.newBuilder().setString(ProtoDataType.String.getDefaultInstance).build())
    roundTrip(BinaryType,
      ProtoDataType.newBuilder().setBinary(ProtoDataType.Binary.getDefaultInstance).build())
    roundTrip(DateType,
      ProtoDataType.newBuilder().setDate(ProtoDataType.Date.getDefaultInstance).build())
    roundTrip(TimestampType,
      ProtoDataType.newBuilder().setTimestamp(ProtoDataType.Timestamp.getDefaultInstance).build())
    roundTrip(TimestampNTZType,
      ProtoDataType.newBuilder().setTimestampNtz(ProtoDataType.TimestampNTZ.getDefaultInstance).build())
    roundTrip(NullType,
      ProtoDataType.newBuilder().setNull(ProtoDataType.NULL.getDefaultInstance).build())
  }

  test("DecimalType") {
    val proto = ProtoDataType.newBuilder().setDecimal(
      ProtoDataType.Decimal.newBuilder().setPrecision(18).setScale(6).build()
    ).build()
    DataTypeProtoConverter.fromProto(proto) shouldBe DecimalType(18, 6)
  }

  test("ArrayType") {
    val elementProto = ProtoDataType.newBuilder()
      .setInteger(ProtoDataType.Integer.getDefaultInstance).build()
    val proto = ProtoDataType.newBuilder().setArray(
      ProtoDataType.Array.newBuilder()
        .setElementType(elementProto)
        .setContainsNull(true)
        .build()
    ).build()
    DataTypeProtoConverter.fromProto(proto) shouldBe ArrayType(IntegerType, containsNull = true)
  }

  test("MapType") {
    val keyProto = ProtoDataType.newBuilder()
      .setString(ProtoDataType.String.getDefaultInstance).build()
    val valProto = ProtoDataType.newBuilder()
      .setLong(ProtoDataType.Long.getDefaultInstance).build()
    val proto = ProtoDataType.newBuilder().setMap(
      ProtoDataType.Map.newBuilder()
        .setKeyType(keyProto)
        .setValueType(valProto)
        .setValueContainsNull(true)
        .build()
    ).build()
    DataTypeProtoConverter.fromProto(proto) shouldBe MapType(StringType, LongType, valueContainsNull = true)
  }

  test("StructType") {
    val f1 = ProtoDataType.StructField.newBuilder()
      .setName("id")
      .setDataType(ProtoDataType.newBuilder()
        .setLong(ProtoDataType.Long.getDefaultInstance).build())
      .setNullable(false)
      .build()
    val f2 = ProtoDataType.StructField.newBuilder()
      .setName("name")
      .setDataType(ProtoDataType.newBuilder()
        .setString(ProtoDataType.String.getDefaultInstance).build())
      .setNullable(true)
      .build()
    val proto = ProtoDataType.newBuilder().setStruct(
      ProtoDataType.Struct.newBuilder()
        .addFields(f1)
        .addFields(f2)
        .build()
    ).build()
    val expected = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = true)
    ))
    DataTypeProtoConverter.fromProto(proto) shouldBe expected
  }
