package org.apache.spark.sql.expressions

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import scala.jdk.CollectionConverters.*

import org.apache.spark.sql.{Encoder, Encoders, functions}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.*
import org.apache.spark.sql.connect.client.DataTypeProtoConverter
import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class AggregatorSuite extends AnyFunSuite with Matchers:

  /** A simple Long-sum aggregator for testing. */
  object LongSumAggregator extends Aggregator[Long, Long, Long]:
    def zero: Long = 0L
    def reduce(b: Long, a: Long): Long = b + a
    def merge(b1: Long, b2: Long): Long = b1 + b2
    def finish(reduction: Long): Long = reduction
    def bufferEncoder: Encoder[Long] = Encoders.scalaLong
    def outputEncoder: Encoder[Long] = Encoders.scalaLong

  /** An Int-to-Double average aggregator for testing. */
  object IntAvgAggregator extends Aggregator[Int, (Long, Long), Double]:
    def zero: (Long, Long) = (0L, 0L)
    def reduce(b: (Long, Long), a: Int): (Long, Long) = (b._1 + a, b._2 + 1)
    def merge(b1: (Long, Long), b2: (Long, Long)): (Long, Long) = (b1._1 + b2._1, b1._2 + b2._2)
    def finish(reduction: (Long, Long)): Double = reduction._1.toDouble / reduction._2
    def bufferEncoder: Encoder[(Long, Long)] =
      Encoders.tuple(Encoders.scalaLong, Encoders.scalaLong)
    def outputEncoder: Encoder[Double] = Encoders.scalaDouble

  /** Helper to extract the AgnosticEncoder from an Encoder. */
  private def agnostic[T](enc: Encoder[T]): AgnosticEncoder[?] =
    Encoders.asAgnostic(enc)

  // ---------------------------------------------------------------------------
  // Aggregator serialization tests
  // ---------------------------------------------------------------------------

  test("Aggregator serializes and deserializes via Java serialization") {
    val bos = ByteArrayOutputStream()
    val oos = ObjectOutputStream(bos)
    oos.writeObject(LongSumAggregator)
    oos.close()

    val bytes = bos.toByteArray
    bytes.length should be > 0

    val bis = ByteArrayInputStream(bytes)
    val ois = ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[Aggregator[Long, Long, Long]]
    ois.close()

    restored.zero shouldBe 0L
    restored.reduce(10L, 5L) shouldBe 15L
    restored.merge(3L, 7L) shouldBe 10L
    restored.finish(42L) shouldBe 42L
  }

  test("Aggregator has correct @SerialVersionUID") {
    val clazz = classOf[Aggregator[?, ?, ?]]
    val field = clazz.getDeclaredField("serialVersionUID")
    field.setAccessible(true)
    field.getLong(null) shouldBe 2093413866369130093L
  }

  // ---------------------------------------------------------------------------
  // Encoders factory tests
  // ---------------------------------------------------------------------------

  test("Encoders.scalaXxx returns Encoder backed by correct AgnosticEncoder") {
    agnostic(Encoders.scalaBoolean) shouldBe PrimitiveBooleanEncoder
    agnostic(Encoders.scalaByte) shouldBe PrimitiveByteEncoder
    agnostic(Encoders.scalaShort) shouldBe PrimitiveShortEncoder
    agnostic(Encoders.scalaInt) shouldBe PrimitiveIntEncoder
    agnostic(Encoders.scalaLong) shouldBe PrimitiveLongEncoder
    agnostic(Encoders.scalaFloat) shouldBe PrimitiveFloatEncoder
    agnostic(Encoders.scalaDouble) shouldBe PrimitiveDoubleEncoder
  }

  test("Encoders boxed types return correct AgnosticEncoder instances") {
    agnostic(Encoders.BOOLEAN) shouldBe BoxedBooleanEncoder
    agnostic(Encoders.BYTE) shouldBe BoxedByteEncoder
    agnostic(Encoders.SHORT) shouldBe BoxedShortEncoder
    agnostic(Encoders.INT) shouldBe BoxedIntEncoder
    agnostic(Encoders.LONG) shouldBe BoxedLongEncoder
    agnostic(Encoders.FLOAT) shouldBe BoxedFloatEncoder
    agnostic(Encoders.DOUBLE) shouldBe BoxedDoubleEncoder
  }

  test("Encoders.STRING and BINARY return correct instances") {
    agnostic(Encoders.STRING) shouldBe StringEncoder
    agnostic(Encoders.BINARY) shouldBe BinaryEncoder
  }

  test("Encoders.DATE, TIMESTAMP, DECIMAL return correct instances") {
    agnostic(Encoders.DATE).dataType shouldBe DateType
    agnostic(Encoders.TIMESTAMP).dataType shouldBe TimestampType
    agnostic(Encoders.DECIMAL).dataType shouldBe DecimalType.DEFAULT
  }

  test("Encoders.tuple(e1, e2) returns ProductEncoder with correct fields") {
    val enc = Encoders.tuple(Encoders.scalaInt, Encoders.scalaLong)
    val ae = agnostic(enc)
    ae shouldBe a[ProductEncoder[?]]
    val pe = ae.asInstanceOf[ProductEncoder[?]]
    pe.fields should have size 2
    pe.fields(0).name shouldBe "_1"
    pe.fields(0).enc shouldBe PrimitiveIntEncoder
    pe.fields(1).name shouldBe "_2"
    pe.fields(1).enc shouldBe PrimitiveLongEncoder
    pe.dataType shouldBe StructType(
      Seq(
        StructField("_1", IntegerType, nullable = false),
        StructField("_2", LongType, nullable = false)
      )
    )
  }

  test("Encoders.tuple with 3 elements") {
    val enc = Encoders.tuple(Encoders.scalaInt, Encoders.STRING, Encoders.scalaDouble)
    val pe = agnostic(enc).asInstanceOf[ProductEncoder[?]]
    pe.fields should have size 3
    pe.fields(0).enc shouldBe PrimitiveIntEncoder
    pe.fields(1).enc shouldBe StringEncoder
    pe.fields(2).enc shouldBe PrimitiveDoubleEncoder
  }

  // ---------------------------------------------------------------------------
  // udaf() factory + proto tests
  // ---------------------------------------------------------------------------

  test("udaf with explicit encoder creates UserDefinedFunction with aggregate=true") {
    val myUdaf = functions.udaf(LongSumAggregator, Encoders.scalaLong)
    myUdaf._aggregate shouldBe true
    myUdaf.returnType shouldBe LongType
    myUdaf.inputTypes shouldBe Seq(LongType)
  }

  test("udaf with implicit encoder creates UserDefinedFunction with aggregate=true") {
    val myUdaf = functions.udaf(LongSumAggregator)
    myUdaf._aggregate shouldBe true
    myUdaf.returnType shouldBe LongType
    myUdaf.inputTypes shouldBe Seq(LongType)
  }

  test("udaf proto has aggregate=true") {
    val myUdaf = functions.udaf(LongSumAggregator, Encoders.scalaLong)
    val proto = myUdaf.toProto(functionName = "longSum")
    val scalaUdf = proto.getScalarScalaUdf
    scalaUdf.getAggregate shouldBe true
    scalaUdf.getPayload.size() should be > 0
  }

  test("udaf proto has correct input and output types") {
    val myUdaf = functions.udaf(LongSumAggregator, Encoders.scalaLong)
    val proto = myUdaf.toProto(functionName = "longSum")
    val scalaUdf = proto.getScalarScalaUdf

    val inputTypes =
      scalaUdf.getInputTypesList.asScala.toSeq.map(DataTypeProtoConverter.fromProto)
    inputTypes shouldBe Seq(LongType)

    val outputType = DataTypeProtoConverter.fromProto(scalaUdf.getOutputType)
    outputType shouldBe LongType
  }

  test("udaf proto for IntAvgAggregator has correct types") {
    val myUdaf = functions.udaf(IntAvgAggregator, Encoders.scalaInt)
    myUdaf._aggregate shouldBe true
    myUdaf.returnType shouldBe DoubleType
    myUdaf.inputTypes shouldBe Seq(IntegerType)

    val proto = myUdaf.toProto(functionName = "intAvg")
    val scalaUdf = proto.getScalarScalaUdf
    scalaUdf.getAggregate shouldBe true
  }

  test("regular UDF proto has aggregate=false") {
    val f = functions.udf((x: Int) => x + 1)
    val proto = f.toProto(functionName = "inc")
    val scalaUdf = proto.getScalarScalaUdf
    scalaUdf.getAggregate shouldBe false
  }

  test("UserDefinedFunction.forAggregator builds correct proto expression") {
    val myUdaf = functions.udaf(LongSumAggregator, Encoders.scalaLong).withName("mySum")
    val col1 = functions.col("value")
    val result = myUdaf(col1)

    result.expr.hasCommonInlineUserDefinedFunction shouldBe true

    val udfExpr = result.expr.getCommonInlineUserDefinedFunction
    udfExpr.getFunctionName shouldBe "mySum"
    udfExpr.getDeterministic shouldBe true
    udfExpr.getArgumentsList should have size 1
    udfExpr.hasScalarScalaUdf shouldBe true

    val scalaUdf = udfExpr.getScalarScalaUdf
    scalaUdf.getAggregate shouldBe true
    scalaUdf.getPayload.size() should be > 0
    scalaUdf.getInputTypesList should have size 1
    scalaUdf.hasOutputType shouldBe true
  }

  test("udaf withName/asNonNullable/asNondeterministic preserve aggregate flag") {
    val base = functions.udaf(LongSumAggregator, Encoders.scalaLong)

    val named = base.withName("test")
    named._aggregate shouldBe true
    named.name shouldBe Some("test")

    val nonNull = base.asNonNullable()
    nonNull._aggregate shouldBe true

    val nonDet = base.asNondeterministic()
    nonDet._aggregate shouldBe true
  }
