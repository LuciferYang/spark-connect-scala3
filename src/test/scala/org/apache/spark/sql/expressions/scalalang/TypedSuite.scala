package org.apache.spark.sql.expressions.scalalang

import org.apache.spark.sql.TypedColumn
import org.scalatest.funsuite.AnyFunSuite

@annotation.nowarn("cat=deprecation")
class TypedSuite extends AnyFunSuite:

  test("typed.avg returns TypedColumn") {
    val col = typed.avg[Long](_.toDouble)
    assert(col.isInstanceOf[TypedColumn[Long, Double]])
    assert(col.expr.hasTypedAggregateExpression)
  }

  test("typed.count returns TypedColumn") {
    val col = typed.count[String](identity)
    assert(col.isInstanceOf[TypedColumn[String, Long]])
    assert(col.expr.hasTypedAggregateExpression)
  }

  test("typed.sum returns TypedColumn") {
    val col = typed.sum[Int](_.toDouble)
    assert(col.isInstanceOf[TypedColumn[Int, Double]])
    assert(col.expr.hasTypedAggregateExpression)
  }

  test("typed.sumLong returns TypedColumn") {
    val col = typed.sumLong[Long](identity)
    assert(col.isInstanceOf[TypedColumn[Long, Long]])
    assert(col.expr.hasTypedAggregateExpression)
  }
