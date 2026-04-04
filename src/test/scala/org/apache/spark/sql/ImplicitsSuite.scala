package org.apache.spark.sql

import org.apache.spark.connect.proto.expressions.Expression.ExprType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ImplicitsSuite extends AnyFunSuite with Matchers:

  test("$\"colName\" creates UnresolvedAttribute") {
    import org.apache.spark.sql.implicits.*

    val c = $"id"
    c.expr.exprType shouldBe a[ExprType.UnresolvedAttribute]
    c.expr.exprType.asInstanceOf[ExprType.UnresolvedAttribute]
      .value.unparsedIdentifier shouldBe "id"
  }

  test("$\"multi.part.name\" creates attribute with dotted name") {
    import org.apache.spark.sql.implicits.*

    val c = $"a.b.c"
    c.expr.exprType.asInstanceOf[ExprType.UnresolvedAttribute]
      .value.unparsedIdentifier shouldBe "a.b.c"
  }

  test("Symbol to Column conversion") {
    import org.apache.spark.sql.implicits.given

    val c: Column = Symbol("name")
    c.expr.exprType.asInstanceOf[ExprType.UnresolvedAttribute]
      .value.unparsedIdentifier shouldBe "name"
  }

  test("string.col extension") {
    import org.apache.spark.sql.implicits.*

    val c = "age".col
    c.expr.exprType.asInstanceOf[ExprType.UnresolvedAttribute]
      .value.unparsedIdentifier shouldBe "age"
  }
