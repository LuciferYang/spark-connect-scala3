package org.apache.spark.sql.expressions

import org.apache.spark.connect.proto.Expression
import org.apache.spark.sql.functions
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ExpressionsWindowSuite extends AnyFunSuite with Matchers:

  test("expressions Window builds a window spec accepted by Column.over") {
    val spec = Window.partitionBy("dept")
      .orderBy("salary")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val column = functions.row_number().over(spec)
    column.expr.hasWindow shouldBe true
    val window = column.expr.getWindow
    window.getPartitionSpecCount shouldBe 1
    window.getOrderSpecCount shouldBe 1
    window.hasFrameSpec shouldBe true
    window.getFrameSpec.getFrameType shouldBe
      Expression.Window.WindowFrame.FrameType.FRAME_TYPE_ROW
  }

  test("expressions Window range factory delegates to existing window implementation") {
    val spec = Window.rangeBetween(-2L, 2L)
    val column = functions.rank().over(spec)

    column.expr.getWindow.getFrameSpec.getFrameType shouldBe
      Expression.Window.WindowFrame.FrameType.FRAME_TYPE_RANGE
  }
