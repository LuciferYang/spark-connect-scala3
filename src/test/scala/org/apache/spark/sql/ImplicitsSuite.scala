package org.apache.spark.sql

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ImplicitsSuite extends AnyFunSuite with Matchers:

  test("$\"colName\" creates UnresolvedAttribute") {
    import org.apache.spark.sql.implicits.*

    val c = $"id"
    c.expr.hasUnresolvedAttribute shouldBe true
    c.expr.getUnresolvedAttribute.getUnparsedIdentifier shouldBe "id"
  }

  test("$\"multi.part.name\" creates attribute with dotted name") {
    import org.apache.spark.sql.implicits.*

    val c = $"a.b.c"
    c.expr.getUnresolvedAttribute.getUnparsedIdentifier shouldBe "a.b.c"
  }

  test("Symbol to Column conversion") {
    import org.apache.spark.sql.implicits.given

    val c: Column = Symbol("name")
    c.expr.getUnresolvedAttribute.getUnparsedIdentifier shouldBe "name"
  }

  test("string.col extension") {
    import org.apache.spark.sql.implicits.*

    val c = "age".col
    c.expr.getUnresolvedAttribute.getUnparsedIdentifier shouldBe "age"
  }
