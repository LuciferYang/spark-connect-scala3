package org.apache.spark.sql.expressions.javalang

import scala.annotation.nowarn

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.TypedColumn
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

@nowarn("msg=object typed in package org.apache.spark.sql.expressions.javalang is deprecated")
class JavaTypedSuite extends AnyFunSuite with Matchers:

  test("typed Java aggregate functions return TypedColumns") {
    typed.avg[java.lang.Long]((value: java.lang.Long) => value.doubleValue())
      .shouldBe(a[TypedColumn[?, ?]])
    typed.count[java.lang.Long]((value: java.lang.Long) => value)
      .shouldBe(a[TypedColumn[?, ?]])
    typed.sum[java.lang.Long]((value: java.lang.Long) => value.doubleValue())
      .shouldBe(a[TypedColumn[?, ?]])
    typed.sumLong[java.lang.Long]((value: java.lang.Long) => value)
      .shouldBe(a[TypedColumn[?, ?]])
  }

  test("typed.count accepts null extractor results") {
    val extractor: MapFunction[java.lang.Long, Object] = _ => null
    typed.count(extractor) shouldBe a[TypedColumn[?, ?]]
  }
