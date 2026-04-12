package org.apache.spark.sql

import scala.annotation.nowarn

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for deprecated function aliases (approxCountDistinct, monotonicallyIncreasingId). */
@nowarn("msg=deprecated")
class DeprecationFunctionsSuite extends AnyFunSuite with Matchers:

  private def assertFn(c: Column, expectedName: String, expectedArgCount: Int): Unit =
    c.expr.hasUnresolvedFunction shouldBe true
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe expectedName
    fn.getArgumentsList should have size expectedArgCount

  test("approxCountDistinct deprecated alias") {
    assertFn(functions.approxCountDistinct(Column("x")), "approx_count_distinct", 1)
    assertFn(functions.approxCountDistinct(Column("x"), 0.05), "approx_count_distinct", 2)
  }

  test("monotonicallyIncreasingId deprecated alias") {
    assertFn(functions.monotonicallyIncreasingId(), "monotonically_increasing_id", 0)
  }
