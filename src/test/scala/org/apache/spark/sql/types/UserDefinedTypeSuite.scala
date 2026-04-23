package org.apache.spark.sql.types

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.UDTEncoder
import org.apache.spark.sql.connect.client.DataTypeProtoConverter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** A test UDT that wraps an (Int, Int) pair as a StructType with two INT fields. */
class IntPairUDT extends UserDefinedType[IntPair]:
  override def sqlType: DataType = StructType(
    Seq(
      StructField("first", IntegerType, nullable = false),
      StructField("second", IntegerType, nullable = false)
    )
  )
  override def serialize(obj: IntPair): Any = (obj.first, obj.second)
  override def deserialize(datum: Any): IntPair =
    datum match
      case (a: Int, b: Int) => IntPair(a, b)
      case _                => throw IllegalArgumentException(s"Cannot deserialize $datum")
  override def userClass: Class[IntPair] = classOf[IntPair]

case class IntPair(first: Int, second: Int)

class UserDefinedTypeSuite extends AnyFunSuite with Matchers:

  // ---------------------------------------------------------------------------
  // UserDefinedType contract
  // ---------------------------------------------------------------------------

  test("serialize/deserialize round-trip") {
    val udt = IntPairUDT()
    val pair = IntPair(1, 2)
    val serialized = udt.serialize(pair)
    val deserialized = udt.deserialize(serialized)
    deserialized shouldBe pair
  }

  test("UserDefinedType typeName contains class name") {
    val udt = IntPairUDT()
    udt.typeName should include("IntPairUDT")
  }

  test("UserDefinedType sql delegates to sqlType") {
    val udt = IntPairUDT()
    udt.sql shouldBe udt.sqlType.sql
  }

  test("UserDefinedType equality is by class") {
    val udt1 = IntPairUDT()
    val udt2 = IntPairUDT()
    udt1 shouldBe udt2
    udt1.hashCode() shouldBe udt2.hashCode()
  }

  test("UserDefinedType not equal to non-UDT") {
    val udt = IntPairUDT()
    udt.equals("not a UDT") shouldBe false
    udt.equals(IntegerType) shouldBe false
  }

  // ---------------------------------------------------------------------------
  // UDTEncoder properties
  // ---------------------------------------------------------------------------

  test("UDTEncoder dataType is the UDT itself") {
    val udt = IntPairUDT()
    val enc = UDTEncoder(udt)
    enc.dataType shouldBe udt
  }

  test("UDTEncoder isPrimitive is false") {
    val udt = IntPairUDT()
    val enc = UDTEncoder(udt)
    enc.isPrimitive shouldBe false
  }

  test("UDTEncoder nullable is true (non-primitive)") {
    val udt = IntPairUDT()
    val enc = UDTEncoder(udt)
    enc.nullable shouldBe true
  }

  test("UDTEncoder clsTag matches userClass") {
    val udt = IntPairUDT()
    val enc = UDTEncoder(udt)
    enc.clsTag.runtimeClass shouldBe classOf[IntPair]
  }

  test("UDTEncoder companion apply sets udtClass") {
    val udt = IntPairUDT()
    val enc = UDTEncoder(udt)
    enc.udtClass shouldBe classOf[IntPairUDT]
  }

  // ---------------------------------------------------------------------------
  // Encoders.udt creates correct encoder
  // ---------------------------------------------------------------------------

  test("Encoders.udt creates encoder with UDT dataType") {
    val udt = IntPairUDT()
    val encoder = Encoders.udt(udt)
    encoder.agnosticEncoder shouldBe a[UDTEncoder[?]]
    val udtEnc = encoder.agnosticEncoder.asInstanceOf[UDTEncoder[IntPair]]
    udtEnc.dataType shouldBe udt
  }

  test("Encoders.udt encoder schema uses sqlType") {
    val udt = IntPairUDT()
    val encoder = Encoders.udt(udt)
    // The schema wraps the UDT dataType in a StructType with a "value" field
    encoder.schema shouldBe StructType(Seq(StructField("value", udt)))
  }

  // ---------------------------------------------------------------------------
  // DataTypeProtoConverter round-trip
  // ---------------------------------------------------------------------------

  test("DataTypeProtoConverter toProto/fromProto round-trip for UDT") {
    val udt = IntPairUDT()
    val proto = DataTypeProtoConverter.toProto(udt)
    val restored = DataTypeProtoConverter.fromProto(proto)
    restored shouldBe a[IntPairUDT]
    restored.asInstanceOf[IntPairUDT].sqlType shouldBe udt.sqlType
  }

  test("DataTypeProtoConverter toProto sets UDT fields correctly") {
    val udt = IntPairUDT()
    val proto = DataTypeProtoConverter.toProto(udt)
    proto.hasUdt shouldBe true
    val udtProto = proto.getUdt
    udtProto.getType shouldBe "udt"
    udtProto.getJvmClass shouldBe classOf[IntPairUDT].getName
    udtProto.hasSqlType shouldBe true
  }
