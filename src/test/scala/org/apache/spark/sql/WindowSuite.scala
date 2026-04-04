package org.apache.spark.sql

import org.apache.spark.connect.proto.expressions.Expression
import org.apache.spark.connect.proto.expressions.Expression.ExprType
import org.apache.spark.connect.proto.relations.Relation
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class WindowSuite extends AnyFunSuite with Matchers:

  test("Window.partitionBy creates WindowSpec with partition expressions") {
    val ws = Window.partitionBy(Column("dept"))
    ws.partitionExprs should have size 1
    ws.orderExprs shouldBe empty
    ws.frameSpec shouldBe None
  }

  test("Window.orderBy creates WindowSpec with order expressions") {
    val ws = Window.orderBy(Column("salary").desc)
    ws.partitionExprs shouldBe empty
    ws.orderExprs should have size 1
    ws.frameSpec shouldBe None
  }

  test("WindowSpec.rowsBetween creates ROW frame") {
    val ws = Window.partitionBy(Column("dept"))
      .orderBy(Column("salary"))
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    ws.frameSpec shouldBe defined
    val frame = ws.frameSpec.get
    frame.frameType shouldBe Expression.Window.WindowFrame.FrameType.FRAME_TYPE_ROW
    frame.lower shouldBe defined
    frame.upper shouldBe defined
    // lower = unboundedPreceding
    frame.lower.get.boundary shouldBe a[Expression.Window.WindowFrame.FrameBoundary.Boundary.Unbounded]
    // upper = currentRow
    frame.upper.get.boundary shouldBe a[Expression.Window.WindowFrame.FrameBoundary.Boundary.CurrentRow]
  }

  test("WindowSpec.rangeBetween creates RANGE frame") {
    val ws = Window.orderBy(Column("ts"))
      .rangeBetween(-100, 100)
    ws.frameSpec shouldBe defined
    val frame = ws.frameSpec.get
    frame.frameType shouldBe Expression.Window.WindowFrame.FrameType.FRAME_TYPE_RANGE
    // lower = literal value
    frame.lower.get.boundary shouldBe a[Expression.Window.WindowFrame.FrameBoundary.Boundary.Value]
    frame.upper.get.boundary shouldBe a[Expression.Window.WindowFrame.FrameBoundary.Boundary.Value]
  }

  test("Window.rowsBetween factory method") {
    val ws = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    ws.frameSpec shouldBe defined
    val frame = ws.frameSpec.get
    frame.frameType shouldBe Expression.Window.WindowFrame.FrameType.FRAME_TYPE_ROW
    frame.lower.get.boundary shouldBe a[Expression.Window.WindowFrame.FrameBoundary.Boundary.Unbounded]
    frame.upper.get.boundary shouldBe a[Expression.Window.WindowFrame.FrameBoundary.Boundary.Unbounded]
  }

  test("Window constants") {
    Window.unboundedPreceding shouldBe Long.MinValue
    Window.unboundedFollowing shouldBe Long.MaxValue
    Window.currentRow shouldBe 0L
  }

  test("Column.over passes frame spec to Window proto") {
    val ws = Window.partitionBy(Column("dept"))
      .orderBy(Column("salary"))
      .rowsBetween(-1, 1)
    val c = functions.row_number().over(ws)
    c.expr.exprType shouldBe a[ExprType.Window]
    val w = c.expr.exprType.asInstanceOf[ExprType.Window].value
    w.partitionSpec should have size 1
    w.orderSpec should have size 1
    w.frameSpec shouldBe defined
    w.frameSpec.get.frameType shouldBe Expression.Window.WindowFrame.FrameType.FRAME_TYPE_ROW
  }

  test("WindowSpec chaining preserves frame through orderBy") {
    val ws = Window.partitionBy(Column("a"))
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
      .orderBy(Column("b"))
    // orderBy preserves both partition and frame from the existing WindowSpec
    ws.frameSpec shouldBe defined
    ws.orderExprs should have size 1
    ws.partitionExprs should have size 1

    // Typical usage: partition -> order -> frame
    val ws2 = Window.partitionBy(Column("a"))
      .orderBy(Column("b"))
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    ws2.frameSpec shouldBe defined
    ws2.orderExprs should have size 1
    ws2.partitionExprs should have size 1
  }
