package org.apache.spark.sql

import org.apache.spark.connect.proto.Expression
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
    frame.getFrameType shouldBe Expression.Window.WindowFrame.FrameType.FRAME_TYPE_ROW
    frame.hasLower shouldBe true
    frame.hasUpper shouldBe true
    // lower = unboundedPreceding
    frame.getLower.hasUnbounded shouldBe true
    // upper = currentRow
    frame.getUpper.hasCurrentRow shouldBe true
  }

  test("WindowSpec.rangeBetween creates RANGE frame") {
    val ws = Window.orderBy(Column("ts"))
      .rangeBetween(-100, 100)
    ws.frameSpec shouldBe defined
    val frame = ws.frameSpec.get
    frame.getFrameType shouldBe Expression.Window.WindowFrame.FrameType.FRAME_TYPE_RANGE
    // lower = literal value
    frame.getLower.hasValue shouldBe true
    frame.getUpper.hasValue shouldBe true
  }

  test("Window.rowsBetween factory method") {
    val ws = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    ws.frameSpec shouldBe defined
    val frame = ws.frameSpec.get
    frame.getFrameType shouldBe Expression.Window.WindowFrame.FrameType.FRAME_TYPE_ROW
    frame.getLower.hasUnbounded shouldBe true
    frame.getUpper.hasUnbounded shouldBe true
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
    c.expr.hasWindow shouldBe true
    val w = c.expr.getWindow
    w.getPartitionSpecList should have size 1
    w.getOrderSpecList should have size 1
    w.hasFrameSpec shouldBe true
    w.getFrameSpec.getFrameType shouldBe Expression.Window.WindowFrame.FrameType.FRAME_TYPE_ROW
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
