package org.apache.spark.sql

import scala.jdk.CollectionConverters.*

import org.apache.spark.connect.proto.{Expression, CommonInlineUserDefinedFunction, ScalarScalaUDF}
import org.apache.spark.sql.connect.client.DataTypeProtoConverter
import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class UserDefinedFunctionSuite extends AnyFunSuite with Matchers:

  // ---------------------------------------------------------------------------
  // DataTypeProtoConverter.toProto round-trip tests
  // ---------------------------------------------------------------------------

  test("toProto round-trip for primitive types") {
    val types = Seq(
      BooleanType,
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      StringType,
      BinaryType,
      DateType,
      TimestampType,
      TimestampNTZType,
      NullType
    )
    for dt <- types do
      assert(
        DataTypeProtoConverter.fromProto(DataTypeProtoConverter.toProto(dt)) == dt,
        s"Round-trip failed for $dt"
      )
  }

  test("toProto round-trip for DecimalType") {
    val dt = DecimalType(18, 5)
    assert(DataTypeProtoConverter.fromProto(DataTypeProtoConverter.toProto(dt)) == dt)
  }

  test("toProto round-trip for ArrayType") {
    val dt = ArrayType(IntegerType, containsNull = true)
    assert(DataTypeProtoConverter.fromProto(DataTypeProtoConverter.toProto(dt)) == dt)
  }

  test("toProto round-trip for MapType") {
    val dt = MapType(StringType, DoubleType, valueContainsNull = false)
    assert(DataTypeProtoConverter.fromProto(DataTypeProtoConverter.toProto(dt)) == dt)
  }

  test("toProto round-trip for StructType") {
    val dt = StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("age", IntegerType, nullable = false)
    ))
    assert(DataTypeProtoConverter.fromProto(DataTypeProtoConverter.toProto(dt)) == dt)
  }

  test("toProto round-trip for nested StructType") {
    val dt = StructType(Seq(
      StructField("name", StringType),
      StructField("scores", ArrayType(DoubleType, containsNull = false)),
      StructField("meta", MapType(StringType, IntegerType, valueContainsNull = true))
    ))
    val roundTripped = DataTypeProtoConverter.fromProto(DataTypeProtoConverter.toProto(dt))
    assert(roundTripped == dt)
  }

  // ---------------------------------------------------------------------------
  // udf() factory method tests
  // ---------------------------------------------------------------------------

  test("udf with Function1[Int, Int] has correct types") {
    val f = functions.udf((x: Int) => x + 1)
    f.returnType shouldBe IntegerType
    f.inputTypes shouldBe Seq(IntegerType)
  }

  test("udf with Function1[String, Int] has correct types") {
    val f = functions.udf((s: String) => s.length)
    f.returnType shouldBe IntegerType
    f.inputTypes shouldBe Seq(StringType)
  }

  test("udf with Function2[Int, Int, Double] has correct types") {
    val f = functions.udf((a: Int, b: Int) => (a + b).toDouble)
    f.returnType shouldBe DoubleType
    f.inputTypes shouldBe Seq(IntegerType, IntegerType)
  }

  test("udf with Function3[String, Int, Double, Boolean] has correct types") {
    val f = functions.udf((s: String, i: Int, d: Double) => s.length > i && d > 0.0)
    f.returnType shouldBe BooleanType
    f.inputTypes shouldBe Seq(StringType, IntegerType, DoubleType)
  }

  // ---------------------------------------------------------------------------
  // UserDefinedFunction API tests
  // ---------------------------------------------------------------------------

  test("withName sets the function name") {
    val f = functions.udf((x: Int) => x * 2).withName("doubler")
    f.name shouldBe Some("doubler")
  }

  test("asNonNullable creates non-nullable UDF") {
    val f = functions.udf((x: Int) => x * 2).asNonNullable()
    // Verify it compiles and returns a UserDefinedFunction
    f shouldBe a[UserDefinedFunction]
  }

  test("asNondeterministic creates non-deterministic UDF") {
    val f = functions.udf((x: Int) => x * 2).asNondeterministic()
    f shouldBe a[UserDefinedFunction]
  }

  // ---------------------------------------------------------------------------
  // UDF apply produces correct proto expression
  // ---------------------------------------------------------------------------

  test("UDF apply produces CommonInlineUserDefinedFunction expression") {
    val f = functions.udf((x: Int) => x + 1).withName("inc")
    val col1 = Column("age")
    val result = f(col1)

    result.expr.hasCommonInlineUserDefinedFunction shouldBe true

    val udfExpr = result.expr.getCommonInlineUserDefinedFunction
    udfExpr.getFunctionName shouldBe "inc"
    udfExpr.getDeterministic shouldBe true
    udfExpr.getArgumentsList should have size 1
    udfExpr.hasScalarScalaUdf shouldBe true

    val scalaUdf = udfExpr.getScalarScalaUdf
    scalaUdf.getNullable shouldBe true
    scalaUdf.getPayload.size() should be > 0
    scalaUdf.getInputTypesList should have size 1
    scalaUdf.hasOutputType shouldBe true
  }

  test("UDF apply with multiple arguments") {
    val f = functions.udf((a: Int, b: String) => s"$a-$b")
    val result = f(Column("id"), Column("name"))

    val udfExpr = result.expr.getCommonInlineUserDefinedFunction
    udfExpr.getArgumentsList should have size 2

    val scalaUdf = udfExpr.getScalarScalaUdf
    scalaUdf.getInputTypesList should have size 2
  }

  test("UDF serialization produces non-empty payload") {
    val f = functions.udf((x: Int) => x * 2)
    val proto = f.toProto(functionName = "test")
    val scalaUdf = proto.getScalarScalaUdf
    scalaUdf.getPayload.size() should be > 0
  }

  test("UDF output type is correct in proto") {
    val f = functions.udf((x: String) => x.length.toLong)
    val proto = f.toProto(functionName = "strlen")
    val scalaUdf = proto.getScalarScalaUdf
    val outputType = DataTypeProtoConverter.fromProto(scalaUdf.getOutputType)
    assert(outputType == LongType)
  }

  test("UDF input types are correct in proto") {
    val f = functions.udf((a: Double, b: Long) => a + b)
    val proto = f.toProto(functionName = "add")
    val scalaUdf = proto.getScalarScalaUdf
    val inputTypes = scalaUdf.getInputTypesList.asScala.toSeq.map(DataTypeProtoConverter.fromProto)
    assert(inputTypes == Seq(DoubleType, LongType))
  }

  // ---------------------------------------------------------------------------
  // UDFRegistration construction test
  // ---------------------------------------------------------------------------

  test("UDFRegistration can be instantiated") {
    // Just verify it compiles — actual registration requires a live server
    // The class constructor is private[sql] so this tests visibility
    assertCompiles("new UDFRegistration(null)")
  }
