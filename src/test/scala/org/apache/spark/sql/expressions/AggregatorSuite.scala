package org.apache.spark.sql.expressions

import org.apache.spark.connect.proto.Expression
import org.apache.spark.sql.{Encoder, Encoders, TypedColumn}
import org.scalatest.funsuite.AnyFunSuite

class AggregatorSuite extends AnyFunSuite:

  private val summer = new Aggregator[Long, Long, Long]:
    def zero: Long = 0L
    def reduce(b: Long, a: Long): Long = b + a
    def merge(b1: Long, b2: Long): Long = b1 + b2
    def finish(reduction: Long): Long = reduction
    def bufferEncoder: Encoder[Long] = Encoders.scalaLong
    def outputEncoder: Encoder[Long] = Encoders.scalaLong

  test("Aggregator.toColumn returns a TypedColumn") {
    val tc = summer.toColumn
    assert(tc.isInstanceOf[TypedColumn[Long, Long]])
  }

  test("Aggregator.toColumn builds a TypedAggregateExpression proto") {
    val tc = summer.toColumn
    assert(tc.expr.hasTypedAggregateExpression)
    val tae = tc.expr.getTypedAggregateExpression
    assert(tae.hasScalarScalaUdf)
    assert(tae.getScalarScalaUdf.getPayload.size() > 0)
    assert(tae.getScalarScalaUdf.getAggregate == true)
  }

  test("Aggregator.toColumn carries the output encoder") {
    val tc = summer.toColumn
    // Encoders.scalaLong creates a new wrapper each time, so compare the underlying AgnosticEncoder
    assert(tc.encoder.agnosticEncoder == Encoders.scalaLong.agnosticEncoder)
  }

  test("TypedColumn from toColumn can be named") {
    val tc = summer.toColumn.name("sum_result")
    assert(tc.expr.hasAlias)
    assert(tc.expr.getAlias.getName(0) == "sum_result")
    assert(tc.encoder.agnosticEncoder == Encoders.scalaLong.agnosticEncoder)
  }

class ReduceAggregatorSuite extends AnyFunSuite:

  test("ReduceAggregator reduce combines values") {
    val reducer = ReduceAggregator[Int]((a, b) => a + b)(using summon[Encoder[Int]])
    val z = reducer.zero
    assert(z._1 == false)
    val r1 = reducer.reduce(z, 10)
    assert(r1 == (true, 10))
    val r2 = reducer.reduce(r1, 20)
    assert(r2 == (true, 30))
  }

  test("ReduceAggregator merge combines buffers") {
    val reducer = ReduceAggregator[Int]((a, b) => a + b)(using summon[Encoder[Int]])
    val b1 = (true, 10)
    val b2 = (true, 20)
    assert(reducer.merge(b1, b2) == (true, 30))
  }

  test("ReduceAggregator merge with empty buffer") {
    val reducer = ReduceAggregator[Int]((a, b) => a + b)(using summon[Encoder[Int]])
    val empty = reducer.zero
    val full = (true, 42)
    assert(reducer.merge(empty, full) == (true, 42))
    assert(reducer.merge(full, empty) == (true, 42))
    assert(reducer.merge(empty, empty) == empty)
  }

  test("ReduceAggregator finish extracts result") {
    val reducer = ReduceAggregator[String]((a, b) => a + b)(using summon[Encoder[String]])
    assert(reducer.finish((true, "hello")) == "hello")
  }

  test("ReduceAggregator finish throws on empty") {
    val reducer = ReduceAggregator[Int]((a, b) => a + b)(using summon[Encoder[Int]])
    assertThrows[IllegalStateException] {
      reducer.finish(reducer.zero)
    }
  }

  test("ReduceAggregator.toColumn returns TypedColumn") {
    val reducer = ReduceAggregator[Long]((a, b) => a + b)(using summon[Encoder[Long]])
    val tc = reducer.toColumn
    assert(tc.isInstanceOf[TypedColumn[Long, Long]])
    assert(tc.expr.hasTypedAggregateExpression)
  }
