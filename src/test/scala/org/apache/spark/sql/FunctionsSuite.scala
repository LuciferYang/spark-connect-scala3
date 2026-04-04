package org.apache.spark.sql

import org.apache.spark.connect.proto.expressions.Expression
import org.apache.spark.connect.proto.expressions.Expression.ExprType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FunctionsSuite extends AnyFunSuite with Matchers:

  private def assertFn(c: Column, expectedName: String, expectedArgCount: Int): Unit =
    c.expr.exprType shouldBe a[ExprType.UnresolvedFunction]
    val fn = c.expr.exprType.asInstanceOf[ExprType.UnresolvedFunction].value
    fn.functionName shouldBe expectedName
    fn.arguments should have size expectedArgCount

  test("col and column create UnresolvedAttribute") {
    functions.col("x").expr.exprType shouldBe a[ExprType.UnresolvedAttribute]
    functions.column("y").expr.exprType shouldBe a[ExprType.UnresolvedAttribute]
  }

  test("lit delegates to Column.lit") {
    val c = functions.lit(42)
    c.expr.exprType shouldBe a[ExprType.Literal]
  }

  test("expr creates ExpressionString") {
    val c = functions.expr("a + b")
    c.expr.exprType shouldBe a[ExprType.ExpressionString]
    c.expr.exprType.asInstanceOf[ExprType.ExpressionString].value.expression shouldBe "a + b"
  }

  test("aggregate functions") {
    assertFn(functions.count(Column("x")), "count", 1)
    assertFn(functions.sum(Column("x")), "sum", 1)
    assertFn(functions.avg(Column("x")), "avg", 1)
    assertFn(functions.min(Column("x")), "min", 1)
    assertFn(functions.max(Column("x")), "max", 1)
    assertFn(functions.first(Column("x")), "first", 1)
    assertFn(functions.last(Column("x")), "last", 1)
  }

  test("countDistinct sets isDistinct") {
    val c = functions.countDistinct(Column("a"), Column("b"))
    val fn = c.expr.exprType.asInstanceOf[ExprType.UnresolvedFunction].value
    fn.functionName shouldBe "count"
    fn.isDistinct shouldBe true
    fn.arguments should have size 2
  }

  test("math functions") {
    assertFn(functions.abs(Column("x")), "abs", 1)
    assertFn(functions.sqrt(Column("x")), "sqrt", 1)
    assertFn(functions.floor(Column("x")), "floor", 1)
    assertFn(functions.ceil(Column("x")), "ceil", 1)
    assertFn(functions.log(Column("x")), "ln", 1)
    assertFn(functions.exp(Column("x")), "exp", 1)
    assertFn(functions.pow(Column("a"), Column("b")), "power", 2)
    assertFn(functions.round(Column("x"), 2), "round", 2)
  }

  test("string functions") {
    assertFn(functions.upper(Column("x")), "upper", 1)
    assertFn(functions.lower(Column("x")), "lower", 1)
    assertFn(functions.trim(Column("x")), "trim", 1)
    assertFn(functions.length(Column("x")), "length", 1)
    assertFn(functions.concat(Column("a"), Column("b")), "concat", 2)
    assertFn(functions.substring(Column("x"), 1, 3), "substring", 3)
  }

  test("date functions") {
    assertFn(functions.current_date(), "current_date", 0)
    assertFn(functions.current_timestamp(), "current_timestamp", 0)
    assertFn(functions.year(Column("d")), "year", 1)
    assertFn(functions.month(Column("d")), "month", 1)
  }

  test("window functions") {
    assertFn(functions.row_number(), "row_number", 0)
    assertFn(functions.rank(), "rank", 0)
    assertFn(functions.dense_rank(), "dense_rank", 0)
    assertFn(functions.lead(Column("x"), 2), "lead", 2)
    assertFn(functions.lag(Column("x"), 1), "lag", 2)
  }

  test("collection functions") {
    assertFn(functions.array(Column("a"), Column("b")), "array", 2)
    assertFn(functions.struct(Column("a")), "struct", 1)
    assertFn(functions.explode(Column("arr")), "explode", 1)
    assertFn(functions.size(Column("arr")), "size", 1)
  }

  test("string overloads: count(String), sum(String)") {
    assertFn(functions.count("x"), "count", 1)
    assertFn(functions.sum("x"), "sum", 1)
    assertFn(functions.avg("x"), "avg", 1)
    assertFn(functions.min("x"), "min", 1)
    assertFn(functions.max("x"), "max", 1)
  }
