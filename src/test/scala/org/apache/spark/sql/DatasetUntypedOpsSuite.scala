package org.apache.spark.sql

import org.apache.spark.connect.proto.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Unit tests for Dataset untyped operation overloads (P0 API gap). */
class DatasetUntypedOpsSuite extends AnyFunSuite with Matchers:

  /** Create a minimal typed Dataset backed by a LocalRelation (no real server). */
  private def testDs(): Dataset[String] =
    val session = SparkSession(null)
    val rel = Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(0).build())
      .setLocalRelation(LocalRelation.getDefaultInstance)
      .build()
    val df = DataFrame(session, rel)
    Dataset(df, Encoders.STRING)

  test("filter(String) wraps DataFrame filter") {
    val ds = testDs()
    val result = ds.filter("value = 'hello'")
    val rel = result.df.relation
    rel.hasFilter shouldBe true
    val cond = rel.getFilter.getCondition
    cond.hasExpressionString shouldBe true
    cond.getExpressionString.getExpression shouldBe "value = 'hello'"
  }

  test("where(String) delegates to filter(String)") {
    val ds = testDs()
    val result = ds.where("value = 'world'")
    val rel = result.df.relation
    rel.hasFilter shouldBe true
    rel.getFilter.getCondition.getExpressionString.getExpression shouldBe "value = 'world'"
  }

  test("select(String, String*) delegates to DataFrame") {
    val ds = testDs()
    val result = ds.select("value")
    result shouldBe a[DataFrame]
    val rel = result.relation
    rel.hasProject shouldBe true
  }

  test("selectExpr(String*) delegates to DataFrame") {
    val ds = testDs()
    val result = ds.selectExpr("value as v")
    result shouldBe a[DataFrame]
  }

  test("groupBy(String, String*) delegates to DataFrame") {
    val ds = testDs()
    val gdf = ds.groupBy("value")
    gdf shouldBe a[GroupedDataFrame]
  }

  test("agg(Column, Column*) delegates to DataFrame") {
    val ds = testDs()
    val result = ds.agg(functions.count(Column("value")))
    result shouldBe a[DataFrame]
  }

  test("agg(Map) delegates to DataFrame groupBy().agg") {
    val ds = testDs()
    val result = ds.agg(Map("value" -> "count"))
    result shouldBe a[DataFrame]
  }

  test("rollup(Column*) delegates to DataFrame") {
    val ds = testDs()
    val gdf = ds.rollup(Column("value"))
    gdf shouldBe a[GroupedDataFrame]
  }

  test("rollup(String, String*) delegates to DataFrame") {
    val ds = testDs()
    val gdf = ds.rollup("value")
    gdf shouldBe a[GroupedDataFrame]
  }

  test("cube(Column*) delegates to DataFrame") {
    val ds = testDs()
    val gdf = ds.cube(Column("value"))
    gdf shouldBe a[GroupedDataFrame]
  }

  test("cube(String, String*) delegates to DataFrame") {
    val ds = testDs()
    val gdf = ds.cube("value")
    gdf shouldBe a[GroupedDataFrame]
  }

  test("offset delegates to DataFrame") {
    val ds = testDs()
    val result = ds.offset(5)
    val rel = result.df.relation
    rel.hasOffset shouldBe true
    rel.getOffset.getOffset shouldBe 5
  }

  test("head() method exists on Dataset") {
    val ds = testDs()
    val method = ds.getClass.getMethod("head")
    method should not be null
  }

  test("show(truncate: Boolean) method exists on Dataset") {
    val ds = testDs()
    val method = ds.getClass.getMethod("show", classOf[Boolean])
    method should not be null
  }

  test("show(numRows: Int, truncate: Boolean) method exists on Dataset") {
    val ds = testDs()
    val method = ds.getClass.getMethod("show", classOf[Int], classOf[Boolean])
    method should not be null
  }
