package org.apache.spark.sql

import org.apache.spark.connect.proto.Expression
import org.scalatest.funsuite.AnyFunSuite

class TypedColumnSuite extends AnyFunSuite:

  test("TypedColumn wraps expr and encoder") {
    val expr = Expression.newBuilder()
      .setUnresolvedAttribute(
        Expression.UnresolvedAttribute.newBuilder().setUnparsedIdentifier("x")
      )
      .build()
    val tc = TypedColumn[Any, Long](expr, summon[Encoder[Long]])
    assert(tc.expr == expr)
    assert(tc.encoder == summon[Encoder[Long]])
  }

  test("TypedColumn extends Column") {
    val expr = Expression.newBuilder()
      .setUnresolvedAttribute(
        Expression.UnresolvedAttribute.newBuilder().setUnparsedIdentifier("y")
      )
      .build()
    val tc = TypedColumn[Any, Int](expr, summon[Encoder[Int]])
    val col: Column = tc // should compile — TypedColumn is a Column
    assert(col.expr == expr)
  }

  test("TypedColumn.name returns a new TypedColumn with alias") {
    val expr = Expression.newBuilder()
      .setUnresolvedAttribute(
        Expression.UnresolvedAttribute.newBuilder().setUnparsedIdentifier("z")
      )
      .build()
    val tc = TypedColumn[Any, String](expr, summon[Encoder[String]])
    val aliased = tc.name("my_alias")
    assert(aliased.isInstanceOf[TypedColumn[?, ?]])
    assert(aliased.encoder == summon[Encoder[String]])
    // The aliased expression should contain an Alias proto
    assert(aliased.expr.hasAlias)
    assert(aliased.expr.getAlias.getName(0) == "my_alias")
  }
