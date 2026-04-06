package org.apache.spark.sql

import org.apache.spark.connect.proto.{Expression, Relation}
import org.scalatest.funsuite.AnyFunSuite

class TableValuedFunctionSuite extends AnyFunSuite:

  /** Stub SparkSession using null client — proto-only methods never touch gRPC. */
  private def stubSession: SparkSession = SparkSession(null)

  private def tvf: TableValuedFunction = TableValuedFunction(stubSession)

  /** Helper to check TVF relation structure. */
  private def assertTvf(df: DataFrame, expectedName: String, expectedArgCount: Int): Unit =
    val rel = df.relation
    assert(rel.hasUnresolvedTableValuedFunction, s"Expected UnresolvedTableValuedFunction")
    val proto = rel.getUnresolvedTableValuedFunction
    assert(proto.getFunctionName == expectedName, s"Expected function name $expectedName")
    assert(
      proto.getArgumentsCount == expectedArgCount,
      s"Expected $expectedArgCount args, got ${proto.getArgumentsCount}"
    )

  // ---------------------------------------------------------------------------
  // Proto builder tests (existing)
  // ---------------------------------------------------------------------------

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
    val tvfClass = classOf[TableValuedFunction]
    val expectedMethods = Seq(
      "range",
      "explode",
      "explode_outer",
      "posexplode",
      "posexplode_outer",
      "inline",
      "inline_outer",
      "json_tuple",
      "stack",
      "collations",
      "sql_keywords",
      "variant_explode",
      "variant_explode_outer"
    )
    val actualMethods = tvfClass.getMethods.map(_.getName).toSet
    expectedMethods.foreach { name =>
      assert(actualMethods.contains(name), s"Missing TVF method: $name")
    }
  }

  // ---------------------------------------------------------------------------
  // Actual method invocation tests (proto-only, no gRPC)
  // ---------------------------------------------------------------------------

  test("explode builds correct TVF relation") {
    assertTvf(tvf.explode(Column("arr")), "explode", 1)
  }

  test("explode_outer builds correct TVF relation") {
    assertTvf(tvf.explode_outer(Column("arr")), "explode_outer", 1)
  }

  test("posexplode builds correct TVF relation") {
    assertTvf(tvf.posexplode(Column("arr")), "posexplode", 1)
  }

  test("posexplode_outer builds correct TVF relation") {
    assertTvf(tvf.posexplode_outer(Column("arr")), "posexplode_outer", 1)
  }

  test("inline builds correct TVF relation") {
    assertTvf(tvf.inline(Column("structs")), "inline", 1)
  }

  test("inline_outer builds correct TVF relation") {
    assertTvf(tvf.inline_outer(Column("structs")), "inline_outer", 1)
  }

  test("json_tuple builds correct TVF relation") {
    assertTvf(tvf.json_tuple(Column("json"), Column("f1"), Column("f2")), "json_tuple", 3)
  }

  test("stack builds correct TVF relation") {
    assertTvf(tvf.stack(Column.lit(2), Column("a"), Column("b")), "stack", 3)
  }

  test("collations builds correct TVF relation") {
    assertTvf(tvf.collations(), "collations", 0)
  }

  test("sql_keywords builds correct TVF relation") {
    assertTvf(tvf.sql_keywords(), "sql_keywords", 0)
  }

  test("variant_explode builds correct TVF relation") {
    assertTvf(tvf.variant_explode(Column("v")), "variant_explode", 1)
  }

  test("variant_explode_outer builds correct TVF relation") {
    assertTvf(tvf.variant_explode_outer(Column("v")), "variant_explode_outer", 1)
  }

  test("TVF relation has a plan ID") {
    val df = tvf.explode(Column("arr"))
    assert(df.relation.hasCommon)
    assert(df.relation.getCommon.getPlanId >= 0)
  }

  test("TVF argument expression matches input column") {
    val df = tvf.explode(Column("my_array"))
    val arg = df.relation.getUnresolvedTableValuedFunction.getArguments(0)
    assert(arg.hasUnresolvedAttribute)
    assert(arg.getUnresolvedAttribute.getUnparsedIdentifier == "my_array")
  }
