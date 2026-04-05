package org.apache.spark.sql

import org.apache.spark.connect.proto.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.*

/** Tests for DataFrame transformation methods that build proto Relations.
  *
  * These tests verify proto construction only — no live Spark Connect server needed.
  */
class DataFrameSuite extends AnyFunSuite with Matchers:

  /** Create a minimal DataFrame backed by a LocalRelation (no real server). */
  private def testDf(): DataFrame =
    val session = SparkSession(null) // null client — only nextPlanId() is used
    val rel = Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(0).build())
      .setLocalRelation(LocalRelation.getDefaultInstance)
      .build()
    DataFrame(session, rel)

  // ---------- unpivot / melt ----------

  test("unpivot with explicit values builds Unpivot proto") {
    val df = testDf()
    val result = df.unpivot(
      ids = Array(Column("id")),
      values = Array(Column("v1"), Column("v2")),
      variableColumnName = "var",
      valueColumnName = "val"
    )
    val rel = result.relation
    rel.hasUnpivot shouldBe true
    val unpivot = rel.getUnpivot
    unpivot.getIdsList should have size 1
    unpivot.hasValues shouldBe true
    unpivot.getValues.getValuesList should have size 2
    unpivot.getVariableColumnName shouldBe "var"
    unpivot.getValueColumnName shouldBe "val"
  }

  test("unpivot without values builds Unpivot proto without values field") {
    val df = testDf()
    val result = df.unpivot(
      ids = Array(Column("id")),
      variableColumnName = "var",
      valueColumnName = "val"
    )
    val unpivot = result.relation.getUnpivot
    unpivot.getIdsList should have size 1
    unpivot.hasValues shouldBe false
    unpivot.getVariableColumnName shouldBe "var"
  }

  test("melt is an alias for unpivot") {
    val df = testDf()
    val result = df.melt(
      ids = Array(Column("id")),
      values = Array(Column("v1")),
      variableColumnName = "var",
      valueColumnName = "val"
    )
    result.relation.hasUnpivot shouldBe true
  }

  // ---------- withColumnsRenamed ----------

  test("withColumnsRenamed builds WithColumnsRenamed proto") {
    val df = testDf()
    val result = df.withColumnsRenamed(Map("a" -> "x", "b" -> "y"))
    val rel = result.relation
    rel.hasWithColumnsRenamed shouldBe true
    val renames = rel.getWithColumnsRenamed.getRenamesList.asScala
    renames should have size 2
    renames.map(r => r.getColName -> r.getNewColName).toSet shouldBe
      Set("a" -> "x", "b" -> "y")
  }

  // ---------- withColumns ----------

  test("withColumns builds WithColumns proto") {
    val df = testDf()
    val result = df.withColumns(Map(
      "new1" -> functions.lit(1),
      "new2" -> functions.lit("hello")
    ))
    val rel = result.relation
    rel.hasWithColumns shouldBe true
    val aliases = rel.getWithColumns.getAliasesList.asScala
    aliases should have size 2
    aliases.flatMap(_.getNameList.asScala).toSet shouldBe Set("new1", "new2")
  }

  // ---------- drop(Column*) ----------

  test("drop(Column*) builds Drop proto with expressions") {
    val df = testDf()
    val result = df.drop(Column("a"), Column("b"))(using summon[DummyImplicit])
    val rel = result.relation
    rel.hasDrop shouldBe true
    rel.getDrop.getColumnsList should have size 2
  }

  // ---------- observe ----------

  test("observe builds CollectMetrics proto") {
    val df = testDf()
    val result = df.observe("metrics", functions.count(Column("x")), functions.sum(Column("y")))
    val rel = result.relation
    rel.hasCollectMetrics shouldBe true
    val cm = rel.getCollectMetrics
    cm.getName shouldBe "metrics"
    cm.getMetricsList should have size 2
  }

  test("observe with Observation builds CollectMetrics proto and registers") {
    val df = testDf()
    val obs = Observation("obs-test")
    val result = df.observe(obs, functions.count(Column("x")))
    result.relation.hasCollectMetrics shouldBe true
    result.relation.getCollectMetrics.getName shouldBe "obs-test"
    obs.planId should be >= 0L
  }

  test("observe with Observation enforces single use") {
    val df = testDf()
    val obs = Observation("single-use")
    df.observe(obs, functions.count(Column("x")))
    an[IllegalArgumentException] should be thrownBy
      df.observe(obs, functions.count(Column("x")))
  }

  // ---------- randomSplit ----------

  test("randomSplit returns correct number of DataFrames") {
    val df = testDf()
    val splits = df.randomSplit(Array(0.6, 0.3, 0.1))
    splits should have size 3
    splits.foreach { s =>
      s.relation.hasSample shouldBe true
    }
  }

  test("randomSplit bounds are correct") {
    val df = testDf()
    val splits = df.randomSplit(Array(1.0, 1.0))
    val s0 = splits(0).relation.getSample
    val s1 = splits(1).relation.getSample
    s0.getLowerBound shouldBe 0.0
    s0.getUpperBound shouldBe 0.5 +- 0.01
    s1.getLowerBound shouldBe 0.5 +- 0.01
    s1.getUpperBound shouldBe 1.0 +- 0.01
  }
