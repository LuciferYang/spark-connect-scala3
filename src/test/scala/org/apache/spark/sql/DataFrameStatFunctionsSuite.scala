package org.apache.spark.sql

import scala.jdk.CollectionConverters.*

import org.apache.spark.connect.proto.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataFrameStatFunctionsSuite extends AnyFunSuite with Matchers:

  /** Stub SparkSession using null client — proto-only methods never touch gRPC. */
  private def stubSession: SparkSession = SparkSession(null)

  /** Stub DataFrame backed by an empty LocalRelation. */
  private def stubDf: DataFrame =
    val session = stubSession
    val rel = Relation
      .newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(session.nextPlanId()).build())
      .setLocalRelation(LocalRelation.getDefaultInstance)
      .build()
    DataFrame(session, rel)

  private def statFunctions: DataFrameStatFunctions = stubDf.stat

  // ---------------------------------------------------------------------------
  // API surface
  // ---------------------------------------------------------------------------

  test("DataFrameStatFunctions class has expected methods") {
    val clazz = classOf[DataFrameStatFunctions]
    clazz.getMethod("crosstab", classOf[String], classOf[String]) should not be null
    clazz.getMethod("cov", classOf[String], classOf[String]) should not be null
    clazz.getMethod("corr", classOf[String], classOf[String]) should not be null
    clazz.getMethod("corr", classOf[String], classOf[String], classOf[String]) should not be null
    clazz.getMethod("freqItems", classOf[Seq[String]], classOf[Double]) should not be null
    clazz.getMethod("freqItems", classOf[Seq[String]]) should not be null
  }

  // ---------------------------------------------------------------------------
  // Proto builder tests (existing)
  // ---------------------------------------------------------------------------

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

  // ---------------------------------------------------------------------------
  // crosstab (proto-only)
  // ---------------------------------------------------------------------------

  test("crosstab builds StatCrosstab relation") {
    val result = statFunctions.crosstab("col_a", "col_b")
    val rel = result.relation
    assert(rel.hasCrosstab)
    val ct = rel.getCrosstab
    ct.getCol1 shouldBe "col_a"
    ct.getCol2 shouldBe "col_b"
    ct.hasInput shouldBe true
  }

  // ---------------------------------------------------------------------------
  // freqItems (proto-only)
  // ---------------------------------------------------------------------------

  test("freqItems with support builds StatFreqItems relation") {
    val result = statFunctions.freqItems(Seq("x", "y"), 0.1)
    val rel = result.relation
    assert(rel.hasFreqItems)
    val fi = rel.getFreqItems
    fi.getColsList.asScala.toSeq shouldBe Seq("x", "y")
    fi.getSupport shouldBe 0.1
    fi.hasInput shouldBe true
  }

  test("freqItems without support builds StatFreqItems without support set") {
    val result = statFunctions.freqItems(Seq("a"))
    val fi = result.relation.getFreqItems
    fi.getColsList.asScala.toSeq shouldBe Seq("a")
    fi.hasInput shouldBe true
  }

  // ---------------------------------------------------------------------------
  // sampleBy (proto-only)
  // ---------------------------------------------------------------------------

  test("sampleBy(Column, fractions, seed) builds StatSampleBy relation") {
    val result = statFunctions.sampleBy(Column("grp"), Map("a" -> 0.5, "b" -> 0.3), 42L)
    val rel = result.relation
    assert(rel.hasSampleBy)
    val sb = rel.getSampleBy
    sb.getSeed shouldBe 42L
    sb.getFractionsCount shouldBe 2
    sb.hasInput shouldBe true
  }

  test("sampleBy(String, fractions, seed) delegates to Column variant") {
    val result = statFunctions.sampleBy("grp", Map(1 -> 0.5, 2 -> 0.3), 99L)
    val sb = result.relation.getSampleBy
    sb.getSeed shouldBe 99L
    sb.getFractionsCount shouldBe 2
  }

  // ---------------------------------------------------------------------------
  // Relation wiring
  // ---------------------------------------------------------------------------

  test("crosstab result's input is the original DataFrame relation") {
    val df = stubDf
    val result = df.stat.crosstab("a", "b")
    result.relation.getCrosstab.getInput shouldBe df.relation
  }

  test("freqItems result's input is the original DataFrame relation") {
    val df = stubDf
    val result = df.stat.freqItems(Seq("a"), 0.5)
    result.relation.getFreqItems.getInput shouldBe df.relation
  }
