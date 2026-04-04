package org.apache.spark.sql.connect.client

import org.apache.spark.connect.proto.types.DataType as ProtoDataType
import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataTypeProtoConverterSuite extends AnyFunSuite with Matchers:

  private def roundTrip(dt: DataType, protoKind: ProtoDataType.Kind): Unit =
    val proto = ProtoDataType(kind = protoKind)
    val result = DataTypeProtoConverter.fromProto(proto)
    result shouldBe dt

  test("primitive types round-trip") {
    roundTrip(BooleanType, ProtoDataType.Kind.Boolean(ProtoDataType.Boolean()))
    roundTrip(ByteType, ProtoDataType.Kind.Byte(ProtoDataType.Byte()))
    roundTrip(ShortType, ProtoDataType.Kind.Short(ProtoDataType.Short()))
    roundTrip(IntegerType, ProtoDataType.Kind.Integer(ProtoDataType.Integer()))
    roundTrip(LongType, ProtoDataType.Kind.Long(ProtoDataType.Long()))
    roundTrip(FloatType, ProtoDataType.Kind.Float(ProtoDataType.Float()))
    roundTrip(DoubleType, ProtoDataType.Kind.Double(ProtoDataType.Double()))
    roundTrip(StringType, ProtoDataType.Kind.String(ProtoDataType.String()))
    roundTrip(BinaryType, ProtoDataType.Kind.Binary(ProtoDataType.Binary()))
    roundTrip(DateType, ProtoDataType.Kind.Date(ProtoDataType.Date()))
    roundTrip(TimestampType, ProtoDataType.Kind.Timestamp(ProtoDataType.Timestamp()))
    roundTrip(TimestampNTZType, ProtoDataType.Kind.TimestampNtz(ProtoDataType.TimestampNTZ()))
    roundTrip(NullType, ProtoDataType.Kind.Null(ProtoDataType.NULL()))
  }

  test("DecimalType") {
    val proto = ProtoDataType(kind = ProtoDataType.Kind.Decimal(
      ProtoDataType.Decimal(precision = Some(18), scale = Some(6))
    ))
    DataTypeProtoConverter.fromProto(proto) shouldBe DecimalType(18, 6)
  }

  test("ArrayType") {
    val elementProto = ProtoDataType(kind = ProtoDataType.Kind.Integer(ProtoDataType.Integer()))
    val proto = ProtoDataType(kind = ProtoDataType.Kind.Array(
      ProtoDataType.Array(elementType = Some(elementProto), containsNull = true)
    ))
    DataTypeProtoConverter.fromProto(proto) shouldBe ArrayType(IntegerType, containsNull = true)
  }

  test("MapType") {
    val keyProto = ProtoDataType(kind = ProtoDataType.Kind.String(ProtoDataType.String()))
    val valProto = ProtoDataType(kind = ProtoDataType.Kind.Long(ProtoDataType.Long()))
    val proto = ProtoDataType(kind = ProtoDataType.Kind.Map(
      ProtoDataType.Map(keyType = Some(keyProto), valueType = Some(valProto), valueContainsNull = true)
    ))
    DataTypeProtoConverter.fromProto(proto) shouldBe MapType(StringType, LongType, valueContainsNull = true)
  }

  test("StructType") {
    val f1 = ProtoDataType.StructField(
      name = "id",
      dataType = Some(ProtoDataType(kind = ProtoDataType.Kind.Long(ProtoDataType.Long()))),
      nullable = false
    )
    val f2 = ProtoDataType.StructField(
      name = "name",
      dataType = Some(ProtoDataType(kind = ProtoDataType.Kind.String(ProtoDataType.String()))),
      nullable = true
    )
    val proto = ProtoDataType(kind = ProtoDataType.Kind.Struct(
      ProtoDataType.Struct(fields = Seq(f1, f2))
    ))
    val expected = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = true)
    ))
    DataTypeProtoConverter.fromProto(proto) shouldBe expected
  }
