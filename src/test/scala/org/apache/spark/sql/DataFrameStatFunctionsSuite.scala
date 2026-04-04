package org.apache.spark.sql

import scala.jdk.CollectionConverters.*

import org.apache.spark.connect.proto.Expression
import org.apache.spark.connect.proto.StatApproxQuantile
import org.apache.spark.connect.proto.StatCorr
import org.apache.spark.connect.proto.StatCrosstab
import org.apache.spark.connect.proto.StatFreqItems
import org.apache.spark.connect.proto.StatSampleBy
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataFrameStatFunctionsSuite extends AnyFunSuite with Matchers:

  /** Helper: create a minimal DataFrame stub for testing relation building. */
  private def stubDf: DataFrame =
    // We can't create a real SparkSession in unit tests, so we test
    // the proto construction indirectly via DataFrameStatFunctions.
    // For now, we just test that the class compiles and the API surface is correct.
    // Integration tests will verify actual execution.
    null.asInstanceOf[DataFrame]

  test("DataFrameStatFunctions class has expected methods") {
    // Verify the API surface exists by checking method signatures compile
    val clazz = classOf[DataFrameStatFunctions]
    clazz.getMethod("crosstab", classOf[String], classOf[String]) should not be null
    clazz.getMethod("cov", classOf[String], classOf[String]) should not be null
    clazz.getMethod("corr", classOf[String], classOf[String]) should not be null
    clazz.getMethod("corr", classOf[String], classOf[String], classOf[String]) should not be null
    clazz.getMethod("freqItems", classOf[Seq[String]], classOf[Double]) should not be null
    clazz.getMethod("freqItems", classOf[Seq[String]]) should not be null
  }

  test("StatCrosstab proto construction") {
    val proto = StatCrosstab.newBuilder()
      .setCol1("a")
      .setCol2("b")
      .build()
    proto.getCol1 shouldBe "a"
    proto.getCol2 shouldBe "b"
  }

  test("StatCorr proto construction") {
    val proto = StatCorr.newBuilder()
      .setCol1("x")
      .setCol2("y")
      .setMethod("pearson")
      .build()
    proto.getCol1 shouldBe "x"
    proto.getCol2 shouldBe "y"
    proto.getMethod shouldBe "pearson"
  }

  test("StatApproxQuantile proto construction") {
    val proto = StatApproxQuantile.newBuilder()
      .addCols("a")
      .addCols("b")
      .addProbabilities(0.25)
      .addProbabilities(0.5)
      .addProbabilities(0.75)
      .setRelativeError(0.01)
      .build()
    proto.getColsList.asScala.toSeq shouldBe Seq("a", "b")
    proto.getProbabilitiesList.asScala.toSeq.map(_.doubleValue) shouldBe Seq(0.25, 0.5, 0.75)
    proto.getRelativeError shouldBe 0.01
  }

  test("StatFreqItems proto construction") {
    val proto = StatFreqItems.newBuilder()
      .addCols("col1")
      .addCols("col2")
      .setSupport(0.05)
      .build()
    proto.getColsList.asScala.toSeq shouldBe Seq("col1", "col2")
    proto.getSupport shouldBe 0.05
  }

  test("StatSampleBy.Fraction proto construction") {
    val litExpr = Column.lit("stratum_a").expr
    assert(litExpr.hasLiteral)
    val lit = litExpr.getLiteral
    val fraction = StatSampleBy.Fraction.newBuilder()
      .setStratum(lit)
      .setFraction(0.5)
      .build()
    fraction.getFraction shouldBe 0.5
    fraction.hasStratum shouldBe true
  }
