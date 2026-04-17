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

  test("agg((String, String)*) delegates to agg(Map)") {
    val ds = testDs()
    val result = ds.agg("value" -> "count")
    result shouldBe a[DataFrame]
    result.relation.hasAggregate shouldBe true
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

  // ---------------------------------------------------------------------------
  // P1: dropDuplicates varargs
  // ---------------------------------------------------------------------------

  test("dropDuplicates(col1, cols*) delegates to DataFrame") {
    val ds = testDs()
    val result = ds.dropDuplicates("a", "b")
    val rel = result.df.relation
    rel.hasDeduplicate shouldBe true
    import scala.jdk.CollectionConverters.*
    rel.getDeduplicate.getColumnNamesList.asScala shouldBe Seq("a", "b")
  }

  // ---------------------------------------------------------------------------
  // P1: unionAll
  // ---------------------------------------------------------------------------

  test("unionAll is alias for union") {
    val ds1 = testDs()
    val ds2 = testDs()
    val unionResult = ds1.union(ds2)
    val unionAllResult = ds1.unionAll(ds2)
    // Both should produce SetOp with UNION type
    unionResult.df.relation.hasSetOp shouldBe true
    unionAllResult.df.relation.hasSetOp shouldBe true
    unionResult.df.relation.getSetOp.getSetOpType shouldBe
      unionAllResult.df.relation.getSetOp.getSetOpType
  }

  // ---------------------------------------------------------------------------
  // P2 Batch 5: Dataset delegates + Java interop
  // ---------------------------------------------------------------------------

  test("unpivot(4-arg) delegates to DataFrame") {
    val ds = testDs()
    val result = ds.unpivot(
      Array(Column("id")),
      Array(Column("v1")),
      "var",
      "val"
    )
    result.relation.hasUnpivot shouldBe true
  }

  test("unpivot(3-arg) delegates to DataFrame") {
    val ds = testDs()
    val result = ds.unpivot(Array(Column("id")), "var", "val")
    result.relation.hasUnpivot shouldBe true
  }

  test("melt(4-arg) delegates to DataFrame") {
    val ds = testDs()
    val result = ds.melt(
      Array(Column("id")),
      Array(Column("v1")),
      "var",
      "val"
    )
    result.relation.hasUnpivot shouldBe true
  }

  test("melt(3-arg) delegates to DataFrame") {
    val ds = testDs()
    val result = ds.melt(Array(Column("id")), "var", "val")
    result.relation.hasUnpivot shouldBe true
  }

  test("transpose(indexColumn) delegates to DataFrame") {
    val ds = testDs()
    val result = ds.transpose(Column("id"))
    result.relation.hasTranspose shouldBe true
  }

  test("transpose() delegates to DataFrame") {
    val ds = testDs()
    val result = ds.transpose()
    result.relation.hasTranspose shouldBe true
  }

  test("lateralJoin(right) delegates to DataFrame") {
    val ds = testDs()
    val right = ds.toDF()
    val result = ds.lateralJoin(right)
    result.relation.hasLateralJoin shouldBe true
  }

  test("lateralJoin(right, joinType) delegates to DataFrame") {
    val ds = testDs()
    val right = ds.toDF()
    val result = ds.lateralJoin(right, "left")
    result.relation.hasLateralJoin shouldBe true
    result.relation.getLateralJoin.getJoinType shouldBe Join.JoinType.JOIN_TYPE_LEFT_OUTER
  }

  test("lateralJoin(right, condition, joinType) delegates to DataFrame") {
    val ds = testDs()
    val right = ds.toDF()
    val result = ds.lateralJoin(right, Column.lit(true), "cross")
    result.relation.hasLateralJoin shouldBe true
  }

  test("agg(java.util.Map) delegates to Scala Map agg") {
    val ds = testDs()
    val jMap = new java.util.HashMap[String, String]()
    jMap.put("value", "count")
    val result = ds.agg(jMap)
    result shouldBe a[DataFrame]
    result.relation.hasAggregate shouldBe true
  }

  test("withColumns(java.util.Map) delegates to Scala Map withColumns") {
    val ds = testDs()
    val jMap = new java.util.HashMap[String, Column]()
    jMap.put("a", Column.lit(1))
    val result = ds.withColumns(jMap)
    result.relation.hasWithColumns shouldBe true
  }

  test("withColumnsRenamed(java.util.Map) delegates to Scala Map withColumnsRenamed") {
    val ds = testDs()
    val jMap = new java.util.HashMap[String, String]()
    jMap.put("value", "v")
    val result = ds.withColumnsRenamed(jMap)
    result.relation.hasWithColumnsRenamed shouldBe true
  }

  test("dropDuplicates(Array) delegates to dropDuplicates(Seq)") {
    val ds = testDs()
    val result = ds.dropDuplicates(Array("value"))
    val rel = result.df.relation
    rel.hasDeduplicate shouldBe true
    import scala.jdk.CollectionConverters.*
    rel.getDeduplicate.getColumnNamesList.asScala shouldBe Seq("value")
  }

  test("randomSplit(Array[Double]) uses seed=0") {
    val ds = testDs()
    val splits = ds.randomSplit(Array(0.5, 0.5))
    splits.length shouldBe 2
  }
