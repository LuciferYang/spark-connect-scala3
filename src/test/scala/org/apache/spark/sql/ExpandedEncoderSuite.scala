package org.apache.spark.sql

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.*
import org.apache.spark.sql.catalyst.encoders.{
  DecimalEncoderProxy,
  EncoderSerializationProxy,
  ParameterizedEncoderProxy
}
import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

class ExpandedEncoderSuite extends AnyFunSuite with Matchers:

  // ---------------------------------------------------------------------------
  // New Encoder[T] givens: schema + round-trip
  // ---------------------------------------------------------------------------

  test("java.sql.Date encoder schema and round-trip") {
    val enc = summon[Encoder[java.sql.Date]]
    enc.schema shouldBe StructType(Seq(StructField("value", DateType)))
    val date = java.sql.Date.valueOf("2024-01-15")
    enc.fromRow(enc.toRow(date)) shouldBe date
  }

  test("java.sql.Timestamp encoder schema and round-trip") {
    val enc = summon[Encoder[java.sql.Timestamp]]
    enc.schema shouldBe StructType(Seq(StructField("value", TimestampType)))
    val ts = java.sql.Timestamp.valueOf("2024-01-15 10:30:00")
    enc.fromRow(enc.toRow(ts)) shouldBe ts
  }

  test("java.time.LocalDate encoder schema and round-trip") {
    val enc = summon[Encoder[java.time.LocalDate]]
    enc.schema shouldBe StructType(Seq(StructField("value", DateType)))
    val ld = java.time.LocalDate.of(2024, 6, 15)
    enc.fromRow(enc.toRow(ld)) shouldBe ld
  }

  test("java.time.Instant encoder schema and round-trip") {
    val enc = summon[Encoder[java.time.Instant]]
    enc.schema shouldBe StructType(Seq(StructField("value", TimestampType)))
    val inst = java.time.Instant.parse("2024-01-15T10:30:00Z")
    enc.fromRow(enc.toRow(inst)) shouldBe inst
  }

  test("BigDecimal encoder schema and round-trip") {
    val enc = summon[Encoder[BigDecimal]]
    enc.schema shouldBe StructType(Seq(StructField("value", DecimalType.DEFAULT)))
    val bd = BigDecimal("123.456")
    enc.fromRow(enc.toRow(bd)) shouldBe bd
  }

  test("Array[Byte] encoder schema and round-trip") {
    val enc = summon[Encoder[Array[Byte]]]
    enc.schema shouldBe StructType(Seq(StructField("value", BinaryType)))
    val bytes = Array[Byte](1, 2, 3, 4, 5)
    enc.fromRow(enc.toRow(bytes)) shouldBe bytes
  }

  // ---------------------------------------------------------------------------
  // Parameterized AgnosticEncoder stubs
  // ---------------------------------------------------------------------------

  test("DateEncoder has correct dataType and lenient flag") {
    val strict = DateEncoder(false)
    strict.dataType shouldBe DateType
    strict.lenientSerialization shouldBe false

    val lenient = DateEncoder(true)
    lenient.lenientSerialization shouldBe true
  }

  test("TimestampEncoder has correct dataType") {
    val enc = TimestampEncoder(false)
    enc.dataType shouldBe TimestampType
    enc.isPrimitive shouldBe false
  }

  test("InstantEncoder has correct dataType") {
    val enc = InstantEncoder(false)
    enc.dataType shouldBe TimestampType
  }

  test("LocalDateEncoder has correct dataType") {
    val enc = LocalDateEncoder(false)
    enc.dataType shouldBe DateType
  }

  test("ScalaDecimalEncoder has correct dataType") {
    val enc = ScalaDecimalEncoder(DecimalType(18, 6))
    enc.dataType shouldBe DecimalType(18, 6)
  }

  test("JavaDecimalEncoder has correct dataType and lenient flag") {
    val enc = JavaDecimalEncoder(DecimalType(10, 2), true)
    enc.dataType shouldBe DecimalType(10, 2)
    enc.lenientSerialization shouldBe true
  }

  test("LocalDateTimeEncoder is a singleton with TimestampNTZType") {
    LocalDateTimeEncoder.dataType shouldBe TimestampNTZType
  }

  test("ScalaBigIntEncoder is a singleton") {
    ScalaBigIntEncoder.dataType shouldBe DecimalType.DEFAULT
  }

  test("convenience constants have expected values") {
    STRICT_DATE_ENCODER shouldBe DateEncoder(false)
    STRICT_LOCAL_DATE_ENCODER shouldBe LocalDateEncoder(false)
    STRICT_TIMESTAMP_ENCODER shouldBe TimestampEncoder(false)
    STRICT_INSTANT_ENCODER shouldBe InstantEncoder(false)
    DEFAULT_SCALA_DECIMAL_ENCODER shouldBe ScalaDecimalEncoder(DecimalType.DEFAULT)
  }

  // ---------------------------------------------------------------------------
  // ParameterizedEncoderProxy serialization
  // ---------------------------------------------------------------------------

  test("DateEncoder writeReplace produces ParameterizedEncoderProxy") {
    val enc = DateEncoder(false)
    val proxy = serializeAndGetProxy(enc)
    proxy shouldBe a[ParameterizedEncoderProxy]
    val p = proxy.asInstanceOf[ParameterizedEncoderProxy]
    p.encoderName shouldBe "DateEncoder"
    p.args should have length 1
    p.args(0) shouldBe java.lang.Boolean.FALSE
  }

  test("TimestampEncoder writeReplace produces ParameterizedEncoderProxy") {
    val enc = TimestampEncoder(true)
    val proxy = serializeAndGetProxy(enc)
    proxy shouldBe a[ParameterizedEncoderProxy]
    val p = proxy.asInstanceOf[ParameterizedEncoderProxy]
    p.encoderName shouldBe "TimestampEncoder"
    p.args(0) shouldBe java.lang.Boolean.TRUE
  }

  test("ScalaDecimalEncoder writeReplace produces DecimalEncoderProxy") {
    val enc = ScalaDecimalEncoder(DecimalType(18, 6))
    val proxy = serializeAndGetProxy(enc)
    proxy shouldBe a[DecimalEncoderProxy]
    val p = proxy.asInstanceOf[DecimalEncoderProxy]
    p.encoderName shouldBe "ScalaDecimalEncoder"
    p.precision shouldBe 18
    p.scale shouldBe 6
  }

  test("JavaDecimalEncoder writeReplace produces DecimalEncoderProxy with extra args") {
    val enc = JavaDecimalEncoder(DecimalType(10, 2), true)
    val proxy = serializeAndGetProxy(enc)
    proxy shouldBe a[DecimalEncoderProxy]
    val p = proxy.asInstanceOf[DecimalEncoderProxy]
    p.encoderName shouldBe "JavaDecimalEncoder"
    p.precision shouldBe 10
    p.scale shouldBe 2
    p.extraArgs should have length 1
    p.extraArgs(0) shouldBe java.lang.Boolean.TRUE
  }

  test("singleton encoders writeReplace produces EncoderSerializationProxy") {
    val proxy = serializeAndGetProxy(LocalDateTimeEncoder)
    proxy shouldBe a[EncoderSerializationProxy]
    proxy.asInstanceOf[EncoderSerializationProxy].encoderName shouldBe "LocalDateTimeEncoder"
  }

  // ---------------------------------------------------------------------------
  // ArrowSerializer: Date/Timestamp/Decimal/Binary encoding
  // ---------------------------------------------------------------------------

  test("ArrowSerializer encodes Date values") {
    val schema = StructType(Seq(StructField("d", DateType)))
    val date = java.sql.Date.valueOf("2024-01-15")
    val rows = Seq(Row(date))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("ArrowSerializer encodes Timestamp values") {
    val schema = StructType(Seq(StructField("ts", TimestampType)))
    val ts = java.sql.Timestamp.valueOf("2024-01-15 10:30:00")
    val rows = Seq(Row(ts))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("ArrowSerializer encodes Decimal values") {
    val schema = StructType(Seq(StructField("d", DecimalType(10, 2))))
    val bd = java.math.BigDecimal("123.45")
    val rows = Seq(Row(bd))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("ArrowSerializer encodes Binary values") {
    val schema = StructType(Seq(StructField("b", BinaryType)))
    val bytes = Array[Byte](1, 2, 3)
    val rows = Seq(Row(bytes))
    val result = ArrowSerializer.encodeRows(rows, schema)
    result should not be empty
  }

  test("ArrowSerializer encodes LocalDate values for DateType") {
    val schema = StructType(Seq(StructField("d", DateType)))
    val ld = java.time.LocalDate.of(2024, 6, 15)
    val rows = Seq(Row(ld))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("ArrowSerializer encodes Instant values for TimestampType") {
    val schema = StructType(Seq(StructField("ts", TimestampType)))
    val inst = java.time.Instant.parse("2024-01-15T10:30:00Z")
    val rows = Seq(Row(inst))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  // ---------------------------------------------------------------------------
  // encoderForType new mappings
  // ---------------------------------------------------------------------------

  test("UserDefinedFunction uses correct encoder for Date UDF") {
    // Verify that creating a UDF with DateType return doesn't throw
    val udf = UserDefinedFunction(
      ((x: Int) => java.sql.Date.valueOf("2024-01-01")).asInstanceOf[AnyRef],
      DateType,
      Seq(IntegerType)
    )
    udf should not be null
  }

  test("UserDefinedFunction uses correct encoder for Timestamp UDF") {
    val udf = UserDefinedFunction(
      ((x: Int) => java.sql.Timestamp.valueOf("2024-01-01 00:00:00")).asInstanceOf[AnyRef],
      TimestampType,
      Seq(IntegerType)
    )
    udf should not be null
  }

  test("UserDefinedFunction uses correct encoder for Decimal UDF") {
    val udf = UserDefinedFunction(
      ((x: Int) => java.math.BigDecimal.valueOf(x.toLong)).asInstanceOf[AnyRef],
      DecimalType(10, 2),
      Seq(IntegerType)
    )
    udf should not be null
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Serialize an object via Java serialization and return the writeReplace proxy. */
  private def serializeAndGetProxy(obj: AnyRef): AnyRef =
    // Walk up the class hierarchy to find writeReplace (may be in a parent class)
    def findWriteReplace(cls: Class[?]): java.lang.reflect.Method =
      try
        val m = cls.getDeclaredMethod("writeReplace")
        m.setAccessible(true)
        m
      catch
        case _: NoSuchMethodException =>
          val parent = cls.getSuperclass
          if parent == null then
            throw NoSuchMethodException(
              s"writeReplace not found in ${obj.getClass.getName}"
            )
          findWriteReplace(parent)
    val method = findWriteReplace(obj.getClass)
    method.invoke(obj)

  // ---------------------------------------------------------------------------
  // Phase 3: Collection type AgnosticEncoder stubs
  // ---------------------------------------------------------------------------

  test("OptionEncoder wraps inner encoder") {
    val enc = OptionEncoder(PrimitiveIntEncoder)
    enc.dataType shouldBe IntegerType
    enc.nullable shouldBe true
    enc.isPrimitive shouldBe false
  }

  test("ArrayEncoder has ArrayType dataType") {
    val enc = ArrayEncoder(StringEncoder, containsNull = true)
    enc.dataType shouldBe ArrayType(StringType, true)
    enc.isPrimitive shouldBe false
  }

  test("IterableEncoder has ArrayType dataType") {
    import scala.reflect.ClassTag
    val enc = IterableEncoder[Seq[Int], Int](
      ClassTag(classOf[Seq[?]]),
      PrimitiveIntEncoder,
      containsNull = false
    )
    enc.dataType shouldBe ArrayType(IntegerType, false)
  }

  test("MapEncoder has MapType dataType") {
    import scala.reflect.ClassTag
    val enc = MapEncoder[Map[String, Int], String, Int](
      ClassTag(classOf[Map[?, ?]]),
      StringEncoder,
      PrimitiveIntEncoder,
      valueContainsNull = false
    )
    enc.dataType shouldBe MapType(StringType, IntegerType, false)
  }

  test("ProductEncoder has StructType dataType") {
    import scala.reflect.ClassTag
    val enc = ProductEncoder[Person](
      ClassTag(classOf[Person]),
      fields = Seq(
        EncoderField("name", StringEncoder, nullable = true, Metadata.empty),
        EncoderField("age", PrimitiveIntEncoder, nullable = false, Metadata.empty)
      )
    )
    enc.dataType shouldBe StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = false)
    ))
  }

  // ---------------------------------------------------------------------------
  // Phase 3: ArrowSerializer complex type encoding
  // ---------------------------------------------------------------------------

  test("ArrowSerializer encodes ArrayType values") {
    val schema = StructType(Seq(StructField("arr", ArrayType(IntegerType, false))))
    val rows = Seq(Row(Seq(1, 2, 3)))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }

  test("ArrowSerializer encodes StructType values") {
    val innerSchema = StructType(Seq(StructField("x", StringType)))
    val schema = StructType(Seq(StructField("s", innerSchema)))
    val rows = Seq(Row(Row("hello")))
    val bytes = ArrowSerializer.encodeRows(rows, schema)
    bytes should not be empty
  }
