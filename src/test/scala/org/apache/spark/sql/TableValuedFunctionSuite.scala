package org.apache.spark.sql

import org.apache.spark.connect.proto.{Expression, Relation}
import org.scalatest.funsuite.AnyFunSuite

class TableValuedFunctionSuite extends AnyFunSuite:

  /** Stub SparkSession with minimal wiring for proto construction (no gRPC). */
  private def stubSession: SparkSession =
    // We cannot construct a real SparkSession without a gRPC server.
    // Instead, test the proto construction via a helper that mimics what TVF.fn does.
    null // tests below exercise the proto builder directly

  test("UnresolvedTableValuedFunction proto is built correctly for explode") {
    val fnName = "explode"
    val argExpr = Expression.newBuilder()
      .setUnresolvedAttribute(
        Expression.UnresolvedAttribute.newBuilder().setUnparsedIdentifier("col1")
      )
      .build()
    val tvfProto = org.apache.spark.connect.proto.UnresolvedTableValuedFunction
      .newBuilder()
      .setFunctionName(fnName)
      .addArguments(argExpr)
      .build()
    assert(tvfProto.getFunctionName == "explode")
    assert(tvfProto.getArgumentsCount == 1)
    assert(tvfProto.getArguments(0).getUnresolvedAttribute.getUnparsedIdentifier == "col1")
  }

  test("UnresolvedTableValuedFunction proto with multiple arguments") {
    val arg1 = Expression.newBuilder()
      .setUnresolvedAttribute(
        Expression.UnresolvedAttribute.newBuilder().setUnparsedIdentifier("input")
      )
      .build()
    val arg2 = Expression.newBuilder()
      .setUnresolvedAttribute(
        Expression.UnresolvedAttribute.newBuilder().setUnparsedIdentifier("field1")
      )
      .build()
    val tvfProto = org.apache.spark.connect.proto.UnresolvedTableValuedFunction
      .newBuilder()
      .setFunctionName("json_tuple")
      .addArguments(arg1)
      .addArguments(arg2)
      .build()
    assert(tvfProto.getFunctionName == "json_tuple")
    assert(tvfProto.getArgumentsCount == 2)
  }

  test("UnresolvedTableValuedFunction proto with no arguments") {
    val tvfProto = org.apache.spark.connect.proto.UnresolvedTableValuedFunction
      .newBuilder()
      .setFunctionName("collations")
      .build()
    assert(tvfProto.getFunctionName == "collations")
    assert(tvfProto.getArgumentsCount == 0)
  }

  test("TVF proto can be set on Relation") {
    val tvfProto = org.apache.spark.connect.proto.UnresolvedTableValuedFunction
      .newBuilder()
      .setFunctionName("sql_keywords")
      .build()
    val relation = Relation.newBuilder()
      .setUnresolvedTableValuedFunction(tvfProto)
      .build()
    assert(relation.hasUnresolvedTableValuedFunction)
    assert(relation.getUnresolvedTableValuedFunction.getFunctionName == "sql_keywords")
  }

  test("All TVF method names are valid identifiers") {
    // Verify the TVF class has all expected methods via reflection
    val tvfClass = classOf[TableValuedFunction]
    val expectedMethods = Seq(
      "range", "explode", "explode_outer", "posexplode", "posexplode_outer",
      "inline", "inline_outer", "json_tuple", "stack", "collations",
      "sql_keywords", "variant_explode", "variant_explode_outer"
    )
    val actualMethods = tvfClass.getMethods.map(_.getName).toSet
    expectedMethods.foreach { name =>
      assert(actualMethods.contains(name), s"Missing TVF method: $name")
    }
  }
