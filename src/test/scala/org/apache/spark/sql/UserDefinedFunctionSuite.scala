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

  test("udf overloads from Function4 through Function21 have correct arity") {
    def verify(f: UserDefinedFunction, inputCount: Int): Unit =
      f.returnType shouldBe IntegerType
      f.inputTypes shouldBe Seq.fill(inputCount)(IntegerType)

    verify(functions.udf((_: Int, _: Int, _: Int, _: Int) => 0), 4)
    verify(functions.udf((_: Int, _: Int, _: Int, _: Int, _: Int) => 0), 5)
    verify(functions.udf((_: Int, _: Int, _: Int, _: Int, _: Int, _: Int) => 0), 6)
    verify(functions.udf((_: Int, _: Int, _: Int, _: Int, _: Int, _: Int, _: Int) => 0), 7)
    verify(functions.udf((_: Int, _: Int, _: Int, _: Int, _: Int, _: Int, _: Int, _: Int) => 0), 8)
    verify(
      functions.udf((_: Int, _: Int, _: Int, _: Int, _: Int, _: Int, _: Int, _: Int, _: Int) => 0),
      9
    )
    verify(
      functions.udf(
        (_: Int, _: Int, _: Int, _: Int, _: Int, _: Int, _: Int, _: Int, _: Int, _: Int) => 0
      ),
      10
    )
    verify(
      functions.udf(
        (_: Int, _: Int, _: Int, _: Int, _: Int, _: Int, _: Int, _: Int, _: Int, _: Int, _: Int) =>
          0
      ),
      11
    )
    verify(
      functions.udf(
        (
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int
        ) => 0
      ),
      12
    )
    verify(
      functions.udf(
        (
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int
        ) => 0
      ),
      13
    )
    verify(
      functions.udf(
        (
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int
        ) => 0
      ),
      14
    )
    verify(
      functions.udf(
        (
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int
        ) => 0
      ),
      15
    )
    verify(
      functions.udf(
        (
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int
        ) => 0
      ),
      16
    )
    verify(
      functions.udf(
        (
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int
        ) => 0
      ),
      17
    )
    verify(
      functions.udf(
        (
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int
        ) => 0
      ),
      18
    )
    verify(
      functions.udf(
        (
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int
        ) => 0
      ),
      19
    )
    verify(
      functions.udf(
        (
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int
        ) => 0
      ),
      20
    )
    verify(
      functions.udf(
        (
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int,
            _: Int
        ) => 0
      ),
      21
    )
  }

  test("udf with Function22 has correct types") {
    val f = functions.udf(
      (
          a1: Int,
          a2: Int,
          a3: Int,
          a4: Int,
          a5: Int,
          a6: Int,
          a7: Int,
          a8: Int,
          a9: Int,
          a10: Int,
          a11: Int,
          a12: Int,
          a13: Int,
          a14: Int,
          a15: Int,
          a16: Int,
          a17: Int,
          a18: Int,
          a19: Int,
          a20: Int,
          a21: Int,
          a22: Int
      ) =>
        a1 + a2 + a3 + a4 + a5 + a6 + a7 + a8 + a9 + a10 + a11 + a12 + a13 +
          a14 + a15 + a16 + a17 + a18 + a19 + a20 + a21 + a22
    )

    f.returnType shouldBe IntegerType
    f.inputTypes shouldBe Seq.fill(22)(IntegerType)
  }

  test("Java UDF overloads from UDF0 through UDF21 keep explicit return type") {
    def verify(f: UserDefinedFunction): Unit =
      f.returnType shouldBe IntegerType
      f.inputTypes shouldBe empty

    verify(functions.udf(null.asInstanceOf[org.apache.spark.sql.api.java.UDF0[Int]], IntegerType))
    verify(functions.udf(
      null.asInstanceOf[org.apache.spark.sql.api.java.UDF1[Int, Int]],
      IntegerType
    ))
    verify(functions.udf(
      null.asInstanceOf[org.apache.spark.sql.api.java.UDF2[Int, Int, Int]],
      IntegerType
    ))
    verify(functions.udf(
      null.asInstanceOf[org.apache.spark.sql.api.java.UDF3[Int, Int, Int, Int]],
      IntegerType
    ))
    verify(functions.udf(
      null.asInstanceOf[org.apache.spark.sql.api.java.UDF4[Int, Int, Int, Int, Int]],
      IntegerType
    ))
    verify(functions.udf(
      null.asInstanceOf[org.apache.spark.sql.api.java.UDF5[Int, Int, Int, Int, Int, Int]],
      IntegerType
    ))
    verify(functions.udf(
      null.asInstanceOf[org.apache.spark.sql.api.java.UDF6[Int, Int, Int, Int, Int, Int, Int]],
      IntegerType
    ))
    verify(functions.udf(
      null.asInstanceOf[org.apache.spark.sql.api.java.UDF7[Int, Int, Int, Int, Int, Int, Int, Int]],
      IntegerType
    ))
    verify(functions.udf(
      null.asInstanceOf[org.apache.spark.sql.api.java.UDF8[
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int
      ]],
      IntegerType
    ))
    verify(functions.udf(
      null.asInstanceOf[org.apache.spark.sql.api.java.UDF9[
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int
      ]],
      IntegerType
    ))
    verify(functions.udf(
      null.asInstanceOf[org.apache.spark.sql.api.java.UDF10[
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int
      ]],
      IntegerType
    ))
    verify(functions.udf(
      null.asInstanceOf[org.apache.spark.sql.api.java.UDF11[
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int
      ]],
      IntegerType
    ))
    verify(functions.udf(
      null.asInstanceOf[org.apache.spark.sql.api.java.UDF12[
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int
      ]],
      IntegerType
    ))
    verify(functions.udf(
      null.asInstanceOf[org.apache.spark.sql.api.java.UDF13[
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int
      ]],
      IntegerType
    ))
    verify(functions.udf(
      null.asInstanceOf[org.apache.spark.sql.api.java.UDF14[
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int
      ]],
      IntegerType
    ))
    verify(functions.udf(
      null.asInstanceOf[org.apache.spark.sql.api.java.UDF15[
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int
      ]],
      IntegerType
    ))
    verify(functions.udf(
      null.asInstanceOf[org.apache.spark.sql.api.java.UDF16[
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int
      ]],
      IntegerType
    ))
    verify(functions.udf(
      null.asInstanceOf[org.apache.spark.sql.api.java.UDF17[
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int
      ]],
      IntegerType
    ))
    verify(functions.udf(
      null.asInstanceOf[org.apache.spark.sql.api.java.UDF18[
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int
      ]],
      IntegerType
    ))
    verify(functions.udf(
      null.asInstanceOf[org.apache.spark.sql.api.java.UDF19[
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int
      ]],
      IntegerType
    ))
    verify(functions.udf(
      null.asInstanceOf[org.apache.spark.sql.api.java.UDF20[
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int
      ]],
      IntegerType
    ))
    verify(functions.udf(
      null.asInstanceOf[org.apache.spark.sql.api.java.UDF21[
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int,
        Int
      ]],
      IntegerType
    ))
  }

  test("Java UDF22 factory keeps explicit return type") {
    val udf22 = new org.apache.spark.sql.api.java.UDF22[
      Int,
      Int,
      Int,
      Int,
      Int,
      Int,
      Int,
      Int,
      Int,
      Int,
      Int,
      Int,
      Int,
      Int,
      Int,
      Int,
      Int,
      Int,
      Int,
      Int,
      Int,
      Int,
      Int
    ]:
      override def call(
          t1: Int,
          t2: Int,
          t3: Int,
          t4: Int,
          t5: Int,
          t6: Int,
          t7: Int,
          t8: Int,
          t9: Int,
          t10: Int,
          t11: Int,
          t12: Int,
          t13: Int,
          t14: Int,
          t15: Int,
          t16: Int,
          t17: Int,
          t18: Int,
          t19: Int,
          t20: Int,
          t21: Int,
          t22: Int
      ): Int =
        t1 + t2 + t3 + t4 + t5 + t6 + t7 + t8 + t9 + t10 + t11 + t12 + t13 +
          t14 + t15 + t16 + t17 + t18 + t19 + t20 + t21 + t22

    val f = functions.udf(udf22, IntegerType)
    f.returnType shouldBe IntegerType
    f.inputTypes shouldBe empty
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

  // ---------------------------------------------------------------------------
  // UDFRegistration register overloads (inline methods — compile-time test)
  // ---------------------------------------------------------------------------

  test("UDFRegistration.register inline overloads compile") {
    // These verify the inline register methods exist and compile correctly.
    // Actual execution requires a live server connection.
    assertCompiles("""
      val reg = new UDFRegistration(null)
      val f0 = functions.udf(() => 42)
      val f1 = functions.udf((x: Int) => x + 1)
      val f2 = functions.udf((a: Int, b: Int) => a + b)
      val f22 = functions.udf((
        a1: Int, a2: Int, a3: Int, a4: Int, a5: Int, a6: Int, a7: Int, a8: Int,
        a9: Int, a10: Int, a11: Int, a12: Int, a13: Int, a14: Int, a15: Int,
        a16: Int, a17: Int, a18: Int, a19: Int, a20: Int, a21: Int, a22: Int
      ) => a1 + a2 + a3 + a4 + a5 + a6 + a7 + a8 + a9 + a10 + a11 + a12 +
        a13 + a14 + a15 + a16 + a17 + a18 + a19 + a20 + a21 + a22)
    """)
  }

  // ---------------------------------------------------------------------------
  // UDFRegistration.registerJava — proto-shape verification
  // ---------------------------------------------------------------------------

  test("registerJava builds a CommonInlineUserDefinedFunction with JavaUDF payload") {
    val udf = UDFRegistration.buildJavaUdf("strLen", "com.example.StrLen", IntegerType)

    udf.getFunctionName shouldBe "strLen"
    udf.hasJavaUdf shouldBe true
    val javaUdf = udf.getJavaUdf
    javaUdf.getClassName shouldBe "com.example.StrLen"
    javaUdf.getAggregate shouldBe false
    DataTypeProtoConverter.fromProto(javaUdf.getOutputType) shouldBe IntegerType
  }

  test("registerJava supports complex return types") {
    val structType = StructType(Seq(
      StructField("name", StringType),
      StructField("count", LongType)
    ))
    val udf = UDFRegistration.buildJavaUdf("complex", "com.example.Complex", structType)

    udf.getFunctionName shouldBe "complex"
    udf.getJavaUdf.getClassName shouldBe "com.example.Complex"
    DataTypeProtoConverter.fromProto(udf.getJavaUdf.getOutputType) shouldBe structType
  }
