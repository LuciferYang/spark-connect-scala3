package org.apache.spark.sql.connect.common

import scala.annotation.nowarn

import org.apache.spark.connect.proto.{DataType as ProtoDataType, Expression}
import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for deprecated proto builder methods (setElementType, setKeyType, setStructType). */
@nowarn("msg=deprecated")
class DeprecationLiteralValueProtoConverterSuite extends AnyFunSuite with Matchers:

  private def lit = Expression.Literal.newBuilder()

  test("array toDataType with explicit element type") {
    val elementType = ProtoDataType.newBuilder()
      .setString(ProtoDataType.String.getDefaultInstance).build()
    val arrBuilder = Expression.Literal.Array.newBuilder()
      .setElementType(elementType)
    val proto = lit.setArray(arrBuilder.build()).build()
    LiteralValueProtoConverter.toDataType(proto) shouldBe ArrayType(StringType, containsNull = true)
  }

  test("map toDataType with explicit key/value types") {
    val keyType = ProtoDataType.newBuilder()
      .setString(ProtoDataType.String.getDefaultInstance).build()
    val valType = ProtoDataType.newBuilder()
      .setInteger(ProtoDataType.Integer.getDefaultInstance).build()
    val mapBuilder = Expression.Literal.Map.newBuilder()
      .setKeyType(keyType)
      .setValueType(valType)
    val proto = lit.setMap(mapBuilder.build()).build()
    LiteralValueProtoConverter.toDataType(proto) shouldBe
      MapType(StringType, IntegerType, valueContainsNull = true)
  }

  test("struct toDataType with explicit struct_type") {
    val f1 = ProtoDataType.StructField.newBuilder()
      .setName("a")
      .setDataType(ProtoDataType.newBuilder()
        .setInteger(ProtoDataType.Integer.getDefaultInstance).build())
      .setNullable(true)
      .build()
    val structType = ProtoDataType.newBuilder().setStruct(
      ProtoDataType.Struct.newBuilder().addFields(f1).build()
    ).build()
    val structBuilder = Expression.Literal.Struct.newBuilder()
      .setStructType(structType)
      .addElements(Expression.Literal.newBuilder().setInteger(42).build())
    val proto = lit.setStruct(structBuilder.build()).build()
    val dt = LiteralValueProtoConverter.toDataType(proto)
    dt shouldBe StructType(Seq(StructField("a", IntegerType, nullable = true)))
  }
