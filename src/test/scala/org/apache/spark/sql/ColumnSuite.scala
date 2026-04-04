package org.apache.spark.sql

import org.apache.spark.connect.proto.expressions.Expression
import org.apache.spark.connect.proto.expressions.Expression.ExprType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ColumnSuite extends AnyFunSuite with Matchers:

  test("Column from name creates UnresolvedAttribute") {
    val c = Column("id")
    c.expr.exprType shouldBe a[ExprType.UnresolvedAttribute]
    c.expr.exprType.asInstanceOf[ExprType.UnresolvedAttribute]
      .value.unparsedIdentifier shouldBe "id"
  }

  test("Column.lit creates Literal for primitives") {
    val intCol = Column.lit(42)
    intCol.expr.exprType shouldBe a[ExprType.Literal]
    val lit = intCol.expr.exprType.asInstanceOf[ExprType.Literal].value
    lit.literalType shouldBe a[Expression.Literal.LiteralType.Integer]

    val strCol = Column.lit("hello")
    val slit = strCol.expr.exprType.asInstanceOf[ExprType.Literal].value
    slit.literalType shouldBe a[Expression.Literal.LiteralType.String]

    val boolCol = Column.lit(true)
    val blit = boolCol.expr.exprType.asInstanceOf[ExprType.Literal].value
    blit.literalType shouldBe a[Expression.Literal.LiteralType.Boolean]
  }

  test("Column.lit(null) creates Null literal") {
    val c = Column.lit(null)
    val lit = c.expr.exprType.asInstanceOf[ExprType.Literal].value
    lit.literalType shouldBe a[Expression.Literal.LiteralType.Null]
  }

  test("Column.lit passes through Column") {
    val original = Column("x")
    Column.lit(original) should be theSameInstanceAs original
  }

  test("comparison operators create UnresolvedFunction") {
    val c = Column("a") === Column("b")
    c.expr.exprType shouldBe a[ExprType.UnresolvedFunction]
    val fn = c.expr.exprType.asInstanceOf[ExprType.UnresolvedFunction].value
    fn.functionName shouldBe "=="
    fn.arguments should have size 2
  }

  test("arithmetic operators") {
    val c = Column("x") + Column("y")
    val fn = c.expr.exprType.asInstanceOf[ExprType.UnresolvedFunction].value
    fn.functionName shouldBe "+"
  }

  test("logical operators") {
    val c = Column("a") && Column("b")
    val fn = c.expr.exprType.asInstanceOf[ExprType.UnresolvedFunction].value
    fn.functionName shouldBe "and"
  }

  test("unary not") {
    val c = !Column("flag")
    val fn = c.expr.exprType.asInstanceOf[ExprType.UnresolvedFunction].value
    fn.functionName shouldBe "not"
    fn.arguments should have size 1
  }

  test("cast creates Cast expression") {
    val c = Column("x").cast("string")
    c.expr.exprType shouldBe a[ExprType.Cast]
  }

  test("alias creates Alias expression") {
    val c = Column("x").as("renamed")
    c.expr.exprType shouldBe a[ExprType.Alias]
    val alias = c.expr.exprType.asInstanceOf[ExprType.Alias].value
    alias.name shouldBe Seq("renamed")
  }

  test("asc / desc create SortOrder") {
    val asc = Column("x").asc
    asc.expr.exprType shouldBe a[ExprType.SortOrder]
    val so = asc.expr.exprType.asInstanceOf[ExprType.SortOrder].value
    so.direction shouldBe Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING

    val desc = Column("x").desc
    val dso = desc.expr.exprType.asInstanceOf[ExprType.SortOrder].value
    dso.direction shouldBe Expression.SortOrder.SortDirection.SORT_DIRECTION_DESCENDING
  }

  test("isNull / isNotNull / isNaN") {
    val c1 = Column("x").isNull
    c1.expr.exprType.asInstanceOf[ExprType.UnresolvedFunction].value.functionName shouldBe "isnull"

    val c2 = Column("x").isNotNull
    c2.expr.exprType.asInstanceOf[ExprType.UnresolvedFunction].value.functionName shouldBe "isnotnull"

    val c3 = Column("x").isNaN
    c3.expr.exprType.asInstanceOf[ExprType.UnresolvedFunction].value.functionName shouldBe "isnan"
  }

  test("isin creates 'in' function") {
    val c = Column("x").isin(1, 2, 3)
    val fn = c.expr.exprType.asInstanceOf[ExprType.UnresolvedFunction].value
    fn.functionName shouldBe "in"
    fn.arguments should have size 4 // column + 3 literals
  }

  test("like / rlike") {
    val c = Column("name").like("%test%")
    c.expr.exprType.asInstanceOf[ExprType.UnresolvedFunction].value.functionName shouldBe "like"

    val c2 = Column("name").rlike("^test.*")
    c2.expr.exprType.asInstanceOf[ExprType.UnresolvedFunction].value.functionName shouldBe "rlike"
  }

  test("contains / startsWith / endsWith") {
    Column("x").contains("a").expr.exprType
      .asInstanceOf[ExprType.UnresolvedFunction].value.functionName shouldBe "contains"
    Column("x").startsWith("a").expr.exprType
      .asInstanceOf[ExprType.UnresolvedFunction].value.functionName shouldBe "startswith"
    Column("x").endsWith("a").expr.exprType
      .asInstanceOf[ExprType.UnresolvedFunction].value.functionName shouldBe "endswith"
  }
