package org.apache.spark.sql.catalyst.encoders

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.*
import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.reflect.ClassTag

/** Tests for AgnosticEncoder hierarchy — dataType, isPrimitive, nullable, clsTag. */
class AgnosticEncoderSuite extends AnyFunSuite with Matchers:

  // ---------------------------------------------------------------------------
  // Primitive leaf encoders
  // ---------------------------------------------------------------------------

  test("PrimitiveBooleanEncoder") {
    PrimitiveBooleanEncoder.dataType shouldBe BooleanType
    PrimitiveBooleanEncoder.isPrimitive shouldBe true
    PrimitiveBooleanEncoder.nullable shouldBe false
    PrimitiveBooleanEncoder.encoderName shouldBe "PrimitiveBooleanEncoder"
  }

  test("PrimitiveByteEncoder") {
    PrimitiveByteEncoder.dataType shouldBe ByteType
    PrimitiveByteEncoder.isPrimitive shouldBe true
    PrimitiveByteEncoder.nullable shouldBe false
    PrimitiveByteEncoder.encoderName shouldBe "PrimitiveByteEncoder"
  }

  test("PrimitiveShortEncoder") {
    PrimitiveShortEncoder.dataType shouldBe ShortType
    PrimitiveShortEncoder.isPrimitive shouldBe true
    PrimitiveShortEncoder.nullable shouldBe false
    PrimitiveShortEncoder.encoderName shouldBe "PrimitiveShortEncoder"
  }

  test("PrimitiveIntEncoder") {
    PrimitiveIntEncoder.dataType shouldBe IntegerType
    PrimitiveIntEncoder.isPrimitive shouldBe true
    PrimitiveIntEncoder.nullable shouldBe false
    PrimitiveIntEncoder.encoderName shouldBe "PrimitiveIntEncoder"
  }

  test("PrimitiveLongEncoder") {
    PrimitiveLongEncoder.dataType shouldBe LongType
    PrimitiveLongEncoder.isPrimitive shouldBe true
    PrimitiveLongEncoder.nullable shouldBe false
    PrimitiveLongEncoder.encoderName shouldBe "PrimitiveLongEncoder"
  }

  test("PrimitiveFloatEncoder") {
    PrimitiveFloatEncoder.dataType shouldBe FloatType
    PrimitiveFloatEncoder.isPrimitive shouldBe true
    PrimitiveFloatEncoder.nullable shouldBe false
    PrimitiveFloatEncoder.encoderName shouldBe "PrimitiveFloatEncoder"
  }

  test("PrimitiveDoubleEncoder") {
    PrimitiveDoubleEncoder.dataType shouldBe DoubleType
    PrimitiveDoubleEncoder.isPrimitive shouldBe true
    PrimitiveDoubleEncoder.nullable shouldBe false
    PrimitiveDoubleEncoder.encoderName shouldBe "PrimitiveDoubleEncoder"
  }

  // ---------------------------------------------------------------------------
  // Boxed leaf encoders
  // ---------------------------------------------------------------------------

  test("BoxedBooleanEncoder") {
    BoxedBooleanEncoder.dataType shouldBe BooleanType
    BoxedBooleanEncoder.isPrimitive shouldBe false
    BoxedBooleanEncoder.nullable shouldBe true
    BoxedBooleanEncoder.encoderName shouldBe "BoxedBooleanEncoder"
    BoxedBooleanEncoder.primitive shouldBe PrimitiveBooleanEncoder
  }

  test("BoxedByteEncoder") {
    BoxedByteEncoder.dataType shouldBe ByteType
    BoxedByteEncoder.isPrimitive shouldBe false
    BoxedByteEncoder.nullable shouldBe true
    BoxedByteEncoder.primitive shouldBe PrimitiveByteEncoder
  }

  test("BoxedShortEncoder") {
    BoxedShortEncoder.dataType shouldBe ShortType
    BoxedShortEncoder.isPrimitive shouldBe false
    BoxedShortEncoder.nullable shouldBe true
    BoxedShortEncoder.primitive shouldBe PrimitiveShortEncoder
  }

  test("BoxedIntEncoder") {
    BoxedIntEncoder.dataType shouldBe IntegerType
    BoxedIntEncoder.isPrimitive shouldBe false
    BoxedIntEncoder.nullable shouldBe true
    BoxedIntEncoder.primitive shouldBe PrimitiveIntEncoder
  }

  test("BoxedLongEncoder") {
    BoxedLongEncoder.dataType shouldBe LongType
    BoxedLongEncoder.isPrimitive shouldBe false
    BoxedLongEncoder.nullable shouldBe true
    BoxedLongEncoder.primitive shouldBe PrimitiveLongEncoder
  }

  test("BoxedFloatEncoder") {
    BoxedFloatEncoder.dataType shouldBe FloatType
    BoxedFloatEncoder.isPrimitive shouldBe false
    BoxedFloatEncoder.nullable shouldBe true
    BoxedFloatEncoder.primitive shouldBe PrimitiveFloatEncoder
  }

  test("BoxedDoubleEncoder") {
    BoxedDoubleEncoder.dataType shouldBe DoubleType
    BoxedDoubleEncoder.isPrimitive shouldBe false
    BoxedDoubleEncoder.nullable shouldBe true
    BoxedDoubleEncoder.primitive shouldBe PrimitiveDoubleEncoder
  }

  // ---------------------------------------------------------------------------
  // Special leaf encoders
  // ---------------------------------------------------------------------------

  test("NullEncoder") {
    NullEncoder.dataType shouldBe NullType
    NullEncoder.isPrimitive shouldBe false
    NullEncoder.nullable shouldBe true
    NullEncoder.encoderName shouldBe "NullEncoder"
  }

  test("StringEncoder") {
    StringEncoder.dataType shouldBe StringType
    StringEncoder.isPrimitive shouldBe false
    StringEncoder.nullable shouldBe true
    StringEncoder.encoderName shouldBe "StringEncoder"
  }

  test("BinaryEncoder") {
    BinaryEncoder.dataType shouldBe BinaryType
    BinaryEncoder.isPrimitive shouldBe false
    BinaryEncoder.nullable shouldBe true
    BinaryEncoder.encoderName shouldBe "BinaryEncoder"
  }

  test("UnboundRowEncoder") {
    UnboundRowEncoder.dataType shouldBe StructType(Seq.empty)
    UnboundRowEncoder.isPrimitive shouldBe false
    UnboundRowEncoder.encoderName shouldBe "UnboundRowEncoder"
  }

  // ---------------------------------------------------------------------------
  // Parameterized encoders
  // ---------------------------------------------------------------------------

  test("DateEncoder strict") {
    val enc = DateEncoder(false)
    enc.dataType shouldBe DateType
    enc.isPrimitive shouldBe false
    enc.lenientSerialization shouldBe false
    enc.encoderName shouldBe "DateEncoder"
  }

  test("DateEncoder lenient") {
    val enc = DateEncoder(true)
    enc.lenientSerialization shouldBe true
  }

  test("LocalDateEncoder strict") {
    val enc = LocalDateEncoder(false)
    enc.dataType shouldBe DateType
    enc.lenientSerialization shouldBe false
    enc.encoderName shouldBe "LocalDateEncoder"
  }

  test("LocalDateEncoder lenient") {
    val enc = LocalDateEncoder(true)
    enc.lenientSerialization shouldBe true
  }

  test("TimestampEncoder strict") {
    val enc = TimestampEncoder(false)
    enc.dataType shouldBe TimestampType
    enc.isPrimitive shouldBe false
    enc.lenientSerialization shouldBe false
    enc.encoderName shouldBe "TimestampEncoder"
  }

  test("TimestampEncoder lenient") {
    val enc = TimestampEncoder(true)
    enc.lenientSerialization shouldBe true
  }

  test("InstantEncoder strict") {
    val enc = InstantEncoder(false)
    enc.dataType shouldBe TimestampType
    enc.lenientSerialization shouldBe false
    enc.encoderName shouldBe "InstantEncoder"
  }

  test("InstantEncoder lenient") {
    val enc = InstantEncoder(true)
    enc.lenientSerialization shouldBe true
  }

  test("LocalDateTimeEncoder") {
    LocalDateTimeEncoder.dataType shouldBe TimestampNTZType
    LocalDateTimeEncoder.isPrimitive shouldBe false
    LocalDateTimeEncoder.encoderName shouldBe "LocalDateTimeEncoder"
  }

  test("ScalaDecimalEncoder") {
    val enc = ScalaDecimalEncoder(DecimalType(18, 6))
    enc.dataType shouldBe DecimalType(18, 6)
    enc.isPrimitive shouldBe false
    enc.encoderName shouldBe "ScalaDecimalEncoder"
  }

  test("JavaDecimalEncoder") {
    val enc = JavaDecimalEncoder(DecimalType(10, 2), false)
    enc.dataType shouldBe DecimalType(10, 2)
    enc.lenientSerialization shouldBe false
    enc.encoderName shouldBe "JavaDecimalEncoder"
  }

  test("JavaDecimalEncoder lenient") {
    val enc = JavaDecimalEncoder(DecimalType(10, 2), true)
    enc.lenientSerialization shouldBe true
  }

  test("ScalaBigIntEncoder") {
    ScalaBigIntEncoder.dataType shouldBe DecimalType.DEFAULT
    ScalaBigIntEncoder.isPrimitive shouldBe false
    ScalaBigIntEncoder.encoderName shouldBe "ScalaBigIntEncoder"
  }

  // ---------------------------------------------------------------------------
  // Convenience constants
  // ---------------------------------------------------------------------------

  test("STRICT_DATE_ENCODER") {
    STRICT_DATE_ENCODER shouldBe DateEncoder(false)
    STRICT_DATE_ENCODER.dataType shouldBe DateType
  }

  test("STRICT_LOCAL_DATE_ENCODER") {
    STRICT_LOCAL_DATE_ENCODER shouldBe LocalDateEncoder(false)
    STRICT_LOCAL_DATE_ENCODER.dataType shouldBe DateType
  }

  test("STRICT_TIMESTAMP_ENCODER") {
    STRICT_TIMESTAMP_ENCODER shouldBe TimestampEncoder(false)
    STRICT_TIMESTAMP_ENCODER.dataType shouldBe TimestampType
  }

  test("STRICT_INSTANT_ENCODER") {
    STRICT_INSTANT_ENCODER shouldBe InstantEncoder(false)
    STRICT_INSTANT_ENCODER.dataType shouldBe TimestampType
  }

  test("DEFAULT_SCALA_DECIMAL_ENCODER") {
    DEFAULT_SCALA_DECIMAL_ENCODER shouldBe ScalaDecimalEncoder(DecimalType.DEFAULT)
    DEFAULT_SCALA_DECIMAL_ENCODER.dataType shouldBe DecimalType.DEFAULT
  }

  test("DEFAULT_JAVA_DECIMAL_ENCODER") {
    DEFAULT_JAVA_DECIMAL_ENCODER shouldBe JavaDecimalEncoder(DecimalType.DEFAULT, false)
    DEFAULT_JAVA_DECIMAL_ENCODER.dataType shouldBe DecimalType.DEFAULT
    DEFAULT_JAVA_DECIMAL_ENCODER.lenientSerialization shouldBe false
  }

  // ---------------------------------------------------------------------------
  // Collection type encoders
  // ---------------------------------------------------------------------------

  test("OptionEncoder wraps inner encoder") {
    val enc = OptionEncoder(PrimitiveIntEncoder)
    enc.dataType shouldBe IntegerType
    enc.nullable shouldBe true
    enc.isPrimitive shouldBe false
    enc.element shouldBe PrimitiveIntEncoder
  }

  test("ArrayEncoder has correct ArrayType") {
    val enc = ArrayEncoder(StringEncoder, containsNull = true)
    enc.dataType shouldBe ArrayType(StringType, true)
    enc.isPrimitive shouldBe false
  }

  test("ArrayEncoder containsNull=false") {
    val enc = ArrayEncoder(PrimitiveIntEncoder, containsNull = false)
    enc.dataType shouldBe ArrayType(IntegerType, false)
  }

  test("IterableEncoder has correct ArrayType") {
    val enc = IterableEncoder[Seq[Int], Int](
      ClassTag(classOf[Seq[?]]),
      PrimitiveIntEncoder,
      containsNull = false
    )
    enc.dataType shouldBe ArrayType(IntegerType, false)
    enc.isPrimitive shouldBe false
  }

  test("MapEncoder has correct MapType") {
    val enc = MapEncoder[Map[String, Int], String, Int](
      ClassTag(classOf[Map[?, ?]]),
      StringEncoder,
      PrimitiveIntEncoder,
      valueContainsNull = true
    )
    enc.dataType shouldBe MapType(StringType, IntegerType, true)
    enc.isPrimitive shouldBe false
  }

  test("MapEncoder valueContainsNull=false") {
    val enc = MapEncoder[Map[String, Long], String, Long](
      ClassTag(classOf[Map[?, ?]]),
      StringEncoder,
      PrimitiveLongEncoder,
      valueContainsNull = false
    )
    enc.dataType shouldBe MapType(StringType, LongType, false)
  }

  // ---------------------------------------------------------------------------
  // ProductEncoder
  // ---------------------------------------------------------------------------

  test("ProductEncoder has correct StructType") {
    val enc = ProductEncoder[Any](
      ClassTag(classOf[Any]),
      fields = Seq(
        EncoderField("name", StringEncoder, nullable = true, Metadata.empty),
        EncoderField("age", PrimitiveIntEncoder, nullable = false, Metadata.empty)
      )
    )
    enc.dataType shouldBe StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = false)
    ))
    enc.isPrimitive shouldBe false
  }

  test("ProductEncoder with empty fields") {
    val enc = ProductEncoder[Any](
      ClassTag(classOf[Any]),
      fields = Seq.empty
    )
    enc.dataType shouldBe StructType(Seq.empty)
  }

  // ---------------------------------------------------------------------------
  // EncoderField
  // ---------------------------------------------------------------------------

  test("EncoderField default values") {
    val field = EncoderField("x", PrimitiveIntEncoder, nullable = false, Metadata.empty)
    field.readMethod shouldBe None
    field.writeMethod shouldBe None
  }

  test("EncoderField with readMethod and writeMethod") {
    val field = EncoderField(
      "x",
      PrimitiveIntEncoder,
      nullable = false,
      Metadata.empty,
      readMethod = Some("getX"),
      writeMethod = Some("setX")
    )
    field.readMethod shouldBe Some("getX")
    field.writeMethod shouldBe Some("setX")
  }

  // ---------------------------------------------------------------------------
  // Metadata
  // ---------------------------------------------------------------------------

  test("Metadata.empty has default json") {
    Metadata.empty.json shouldBe "{}"
  }

  test("Metadata with custom json") {
    val m = Metadata("""{"key":"value"}""")
    m.json shouldBe """{"key":"value"}"""
  }

  // ---------------------------------------------------------------------------
  // clsTag
  // ---------------------------------------------------------------------------

  test("primitive encoder clsTag") {
    PrimitiveBooleanEncoder.clsTag shouldBe ClassTag.Boolean
    PrimitiveIntEncoder.clsTag shouldBe ClassTag.Int
    PrimitiveLongEncoder.clsTag shouldBe ClassTag.Long
    PrimitiveDoubleEncoder.clsTag shouldBe ClassTag.Double
    PrimitiveFloatEncoder.clsTag shouldBe ClassTag.Float
    PrimitiveByteEncoder.clsTag shouldBe ClassTag.Byte
    PrimitiveShortEncoder.clsTag shouldBe ClassTag.Short
  }

  test("StringEncoder clsTag") {
    StringEncoder.clsTag.runtimeClass shouldBe classOf[String]
  }

  test("BinaryEncoder clsTag") {
    BinaryEncoder.clsTag.runtimeClass shouldBe classOf[Array[Byte]]
  }

  // ---------------------------------------------------------------------------
  // Serialization proxies
  // ---------------------------------------------------------------------------

  test("EncoderSerializationProxy stores encoder name") {
    val proxy = EncoderSerializationProxy("PrimitiveIntEncoder")
    proxy.encoderName shouldBe "PrimitiveIntEncoder"
  }

  test("ParameterizedEncoderProxy stores name and args") {
    val proxy = ParameterizedEncoderProxy(
      "DateEncoder",
      Array(java.lang.Boolean.FALSE),
      Array(classOf[Boolean])
    )
    proxy.encoderName shouldBe "DateEncoder"
    proxy.args should have length 1
    proxy.argTypes should have length 1
  }

  test("DecimalEncoderProxy stores precision and scale") {
    val proxy = DecimalEncoderProxy(
      "ScalaDecimalEncoder",
      18,
      6,
      Array.empty,
      Array.empty
    )
    proxy.encoderName shouldBe "ScalaDecimalEncoder"
    proxy.precision shouldBe 18
    proxy.scale shouldBe 6
    proxy.extraArgs shouldBe empty
    proxy.extraArgTypes shouldBe empty
  }

  test("DecimalEncoderProxy with extra args") {
    val proxy = DecimalEncoderProxy(
      "JavaDecimalEncoder",
      10,
      2,
      Array(java.lang.Boolean.TRUE),
      Array(classOf[Boolean])
    )
    proxy.extraArgs should have length 1
    proxy.extraArgs(0) shouldBe java.lang.Boolean.TRUE
    proxy.extraArgTypes should have length 1
  }

  // ---------------------------------------------------------------------------
  // lenientSerialization default
  // ---------------------------------------------------------------------------

  test("default lenientSerialization is false for trait") {
    // Leaf encoders that don't override
    PrimitiveIntEncoder.lenientSerialization shouldBe false
    StringEncoder.lenientSerialization shouldBe false
    BinaryEncoder.lenientSerialization shouldBe false
  }

  test("OptionEncoder with StringEncoder") {
    val enc = OptionEncoder(StringEncoder)
    enc.dataType shouldBe StringType
    enc.nullable shouldBe true
    enc.isPrimitive shouldBe false
    enc.element shouldBe StringEncoder
  }

  test("OptionEncoder with nested OptionEncoder") {
    val inner = OptionEncoder(PrimitiveIntEncoder)
    val outer = OptionEncoder(inner)
    outer.dataType shouldBe IntegerType
    outer.nullable shouldBe true
    outer.element shouldBe inner
  }

  // --- ArrayEncoder edge cases ---

  test("ArrayEncoder with nested encoder") {
    val enc = ArrayEncoder(ArrayEncoder(PrimitiveIntEncoder, false), true)
    enc.dataType shouldBe ArrayType(ArrayType(IntegerType, false), true)
  }

  test("ArrayEncoder isPrimitive is always false") {
    ArrayEncoder(PrimitiveBooleanEncoder, false).isPrimitive shouldBe false
  }

  // --- IterableEncoder edge cases ---

  test("IterableEncoder with containsNull=true") {
    val enc = IterableEncoder[Seq[String], String](
      ClassTag(classOf[Seq[?]]),
      StringEncoder,
      containsNull = true
    )
    enc.dataType shouldBe ArrayType(StringType, true)
    enc.isPrimitive shouldBe false
  }

  test("IterableEncoder with List type") {
    val enc = IterableEncoder[List[Int], Int](
      ClassTag(classOf[List[?]]),
      PrimitiveIntEncoder,
      containsNull = false
    )
    enc.dataType shouldBe ArrayType(IntegerType, false)
    enc.clsTag.runtimeClass shouldBe classOf[List[?]]
  }

  // --- MapEncoder edge cases ---

  test("MapEncoder with complex key/value types") {
    val keyEnc = StringEncoder
    val valEnc = ArrayEncoder(PrimitiveIntEncoder, false)
    val enc = MapEncoder[Map[String, Array[Int]], String, Array[Int]](
      ClassTag(classOf[Map[?, ?]]),
      keyEnc,
      valEnc,
      valueContainsNull = false
    )
    enc.dataType shouldBe MapType(StringType, ArrayType(IntegerType, false), false)
    enc.isPrimitive shouldBe false
  }

  // --- ProductEncoder with various field combinations ---

  test("ProductEncoder with single field") {
    val enc = ProductEncoder[Any](
      ClassTag(classOf[Any]),
      fields = Seq(
        EncoderField("value", PrimitiveLongEncoder, nullable = false, Metadata.empty)
      )
    )
    enc.dataType shouldBe StructType(Seq(
      StructField("value", LongType, nullable = false)
    ))
  }

  test("ProductEncoder with nullable and non-nullable fields") {
    val enc = ProductEncoder[Any](
      ClassTag(classOf[Any]),
      fields = Seq(
        EncoderField("id", PrimitiveIntEncoder, nullable = false, Metadata.empty),
        EncoderField("name", StringEncoder, nullable = true, Metadata.empty),
        EncoderField("score", PrimitiveDoubleEncoder, nullable = false, Metadata.empty)
      )
    )
    val dt = enc.dataType.asInstanceOf[StructType]
    dt.fields should have size 3
    dt.fields(0).nullable shouldBe false
    dt.fields(1).nullable shouldBe true
    dt.fields(2).nullable shouldBe false
  }

  test("ProductEncoder with outerPointerGetter") {
    val enc = ProductEncoder[Any](
      ClassTag(classOf[Any]),
      fields = Seq.empty,
      outerPointerGetter = Some(() => "outer")
    )
    enc.outerPointerGetter should not be None
    enc.outerPointerGetter.get() shouldBe "outer"
  }

  // --- clsTag for collection encoders ---

  test("OptionEncoder clsTag") {
    val enc = OptionEncoder(PrimitiveIntEncoder)
    enc.clsTag.runtimeClass shouldBe classOf[Option[?]]
  }

  test("ArrayEncoder clsTag") {
    val enc = ArrayEncoder(StringEncoder, true)
    enc.clsTag.runtimeClass shouldBe classOf[Array[?]]
  }

  // --- nullable defaults ---

  test("nullable returns true for non-primitive encoders") {
    StringEncoder.nullable shouldBe true
    BinaryEncoder.nullable shouldBe true
    NullEncoder.nullable shouldBe true
    BoxedIntEncoder.nullable shouldBe true
  }

  test("nullable returns false for primitive encoders") {
    PrimitiveIntEncoder.nullable shouldBe false
    PrimitiveLongEncoder.nullable shouldBe false
    PrimitiveDoubleEncoder.nullable shouldBe false
    PrimitiveBooleanEncoder.nullable shouldBe false
    PrimitiveFloatEncoder.nullable shouldBe false
    PrimitiveByteEncoder.nullable shouldBe false
    PrimitiveShortEncoder.nullable shouldBe false
  }

  // --- ParameterizedEncoder base class ---

  test("ParameterizedEncoder isPrimitive is false") {
    DateEncoder(false).isPrimitive shouldBe false
    LocalDateEncoder(false).isPrimitive shouldBe false
    TimestampEncoder(false).isPrimitive shouldBe false
    InstantEncoder(false).isPrimitive shouldBe false
    ScalaDecimalEncoder(DecimalType.DEFAULT).isPrimitive shouldBe false
    JavaDecimalEncoder(DecimalType.DEFAULT, false).isPrimitive shouldBe false
  }

  test("ParameterizedEncoder clsTag is correct") {
    DateEncoder(false).clsTag.runtimeClass shouldBe classOf[java.sql.Date]
    LocalDateEncoder(false).clsTag.runtimeClass shouldBe classOf[java.time.LocalDate]
    TimestampEncoder(false).clsTag.runtimeClass shouldBe classOf[java.sql.Timestamp]
    InstantEncoder(false).clsTag.runtimeClass shouldBe classOf[java.time.Instant]
  }

  test("ParameterizedEncoder nullable is true (non-primitive)") {
    DateEncoder(false).nullable shouldBe true
    TimestampEncoder(false).nullable shouldBe true
    InstantEncoder(false).nullable shouldBe true
    ScalaDecimalEncoder(DecimalType.DEFAULT).nullable shouldBe true
  }

  // --- Boxed encoders clsTag ---

  test("BoxedBooleanEncoder clsTag") {
    BoxedBooleanEncoder.clsTag.runtimeClass shouldBe classOf[java.lang.Boolean]
  }

  test("BoxedByteEncoder clsTag") {
    BoxedByteEncoder.clsTag.runtimeClass shouldBe classOf[java.lang.Byte]
  }

  test("BoxedShortEncoder clsTag") {
    BoxedShortEncoder.clsTag.runtimeClass shouldBe classOf[java.lang.Short]
  }

  test("BoxedIntEncoder clsTag") {
    BoxedIntEncoder.clsTag.runtimeClass shouldBe classOf[java.lang.Integer]
  }

  test("BoxedLongEncoder clsTag") {
    BoxedLongEncoder.clsTag.runtimeClass shouldBe classOf[java.lang.Long]
  }

  test("BoxedFloatEncoder clsTag") {
    BoxedFloatEncoder.clsTag.runtimeClass shouldBe classOf[java.lang.Float]
  }

  test("BoxedDoubleEncoder clsTag") {
    BoxedDoubleEncoder.clsTag.runtimeClass shouldBe classOf[java.lang.Double]
  }

  // --- Boxed encoders encoderName ---

  test("BoxedByteEncoder encoderName") {
    BoxedByteEncoder.encoderName shouldBe "BoxedByteEncoder"
  }

  test("BoxedShortEncoder encoderName") {
    BoxedShortEncoder.encoderName shouldBe "BoxedShortEncoder"
  }

  test("BoxedIntEncoder encoderName") {
    BoxedIntEncoder.encoderName shouldBe "BoxedIntEncoder"
  }

  test("BoxedLongEncoder encoderName") {
    BoxedLongEncoder.encoderName shouldBe "BoxedLongEncoder"
  }

  test("BoxedFloatEncoder encoderName") {
    BoxedFloatEncoder.encoderName shouldBe "BoxedFloatEncoder"
  }

  test("BoxedDoubleEncoder encoderName") {
    BoxedDoubleEncoder.encoderName shouldBe "BoxedDoubleEncoder"
  }

  // --- EncoderSerializationProxy equality ---

  test("EncoderSerializationProxy with different names are different") {
    val p1 = EncoderSerializationProxy("PrimitiveIntEncoder")
    val p2 = EncoderSerializationProxy("PrimitiveLongEncoder")
    p1.encoderName should not be p2.encoderName
  }

  // --- DecimalEncoderProxy with various precisions ---

  test("DecimalEncoderProxy for DEFAULT decimal") {
    val proxy = DecimalEncoderProxy(
      "ScalaDecimalEncoder",
      DecimalType.DEFAULT.precision,
      DecimalType.DEFAULT.scale,
      Array.empty,
      Array.empty
    )
    proxy.precision shouldBe DecimalType.DEFAULT.precision
    proxy.scale shouldBe DecimalType.DEFAULT.scale
  }

  // --- Metadata ---

  test("Metadata default constructor") {
    val m = Metadata()
    m.json shouldBe "{}"
  }

  test("Metadata equality") {
    Metadata.empty shouldBe Metadata("{}")
    Metadata("a") shouldBe Metadata("a")
    Metadata("a") should not be Metadata("b")
  }

  // --- EncoderField ---

  test("EncoderField stores all parameters") {
    val field = EncoderField(
      "field1",
      PrimitiveLongEncoder,
      nullable = true,
      Metadata("{\"key\":\"val\"}"),
      readMethod = Some("getField1"),
      writeMethod = Some("setField1")
    )
    field.name shouldBe "field1"
    field.enc shouldBe PrimitiveLongEncoder
    field.nullable shouldBe true
    field.metadata.json shouldBe "{\"key\":\"val\"}"
    field.readMethod shouldBe Some("getField1")
    field.writeMethod shouldBe Some("setField1")
  }

  // --- lenientSerialization for all convenience constants ---

  test("convenience constants lenientSerialization is false") {
    STRICT_DATE_ENCODER.lenientSerialization shouldBe false
    STRICT_LOCAL_DATE_ENCODER.lenientSerialization shouldBe false
    STRICT_TIMESTAMP_ENCODER.lenientSerialization shouldBe false
    STRICT_INSTANT_ENCODER.lenientSerialization shouldBe false
    DEFAULT_JAVA_DECIMAL_ENCODER.lenientSerialization shouldBe false
  }
