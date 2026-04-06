package org.apache.spark.sql

import org.apache.spark.connect.proto.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SubquerySuite extends AnyFunSuite with Matchers:

  /** Create a minimal Dataset backed by a LocalRelation (no real server). */
  private def testDataset[T: Encoder: scala.reflect.ClassTag](planId: Long = 0): Dataset[T] =
    val session = SparkSession(null)
    val rel = Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(planId).build())
      .setLocalRelation(LocalRelation.getDefaultInstance)
      .build()
    Dataset(DataFrame(session, rel), summon[Encoder[T]])

  private def testDataFrame(planId: Long = 0): DataFrame =
    val session = SparkSession(null)
    val rel = Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(planId).build())
      .setLocalRelation(LocalRelation.getDefaultInstance)
      .build()
    DataFrame(session, rel)

  // ---------------------------------------------------------------------------
  // Dataset.scalar()
  // ---------------------------------------------------------------------------

  test("Dataset.scalar() produces SubqueryExpression with SCALAR type") {
    val ds = testDataset[Long](planId = 42)
    val col = ds.scalar()

    col.expr.hasSubqueryExpression shouldBe true
    val sub = col.expr.getSubqueryExpression
    sub.getPlanId shouldBe 42L
    sub.getSubqueryType shouldBe SubqueryExpression.SubqueryType.SUBQUERY_TYPE_SCALAR
  }

  test("Dataset.scalar() carries the subquery relation") {
    val ds = testDataset[Long](planId = 42)
    val col = ds.scalar()
    col.subqueryRelations should have size 1
    col.subqueryRelations.head.getCommon.getPlanId shouldBe 42L
  }

  // ---------------------------------------------------------------------------
  // Dataset.exists()
  // ---------------------------------------------------------------------------

  test("Dataset.exists() produces SubqueryExpression with EXISTS type") {
    val ds = testDataset[Long](planId = 99)
    val col = ds.exists()

    col.expr.hasSubqueryExpression shouldBe true
    val sub = col.expr.getSubqueryExpression
    sub.getPlanId shouldBe 99L
    sub.getSubqueryType shouldBe SubqueryExpression.SubqueryType.SUBQUERY_TYPE_EXISTS
  }

  test("Dataset.exists() carries the subquery relation") {
    val ds = testDataset[Long](planId = 99)
    val col = ds.exists()
    col.subqueryRelations should have size 1
    col.subqueryRelations.head.getCommon.getPlanId shouldBe 99L
  }

  // ---------------------------------------------------------------------------
  // Column.isin(Dataset)
  // ---------------------------------------------------------------------------

  test("Column.isin(Dataset) produces SubqueryExpression with IN type") {
    val ds = testDataset[Long](planId = 77)
    val col = Column("id").isin(ds)

    col.expr.hasSubqueryExpression shouldBe true
    val sub = col.expr.getSubqueryExpression
    sub.getPlanId shouldBe 77L
    sub.getSubqueryType shouldBe SubqueryExpression.SubqueryType.SUBQUERY_TYPE_IN
    sub.getInSubqueryValuesCount shouldBe 1
  }

  test("Column.isin(Dataset) carries the subquery relation") {
    val ds = testDataset[Long](planId = 77)
    val col = Column("id").isin(ds)
    col.subqueryRelations should have size 1
    col.subqueryRelations.head.getCommon.getPlanId shouldBe 77L
  }

  // ---------------------------------------------------------------------------
  // Column combination propagation
  // ---------------------------------------------------------------------------

  test("scalar Column combined with comparison preserves subqueryRelations") {
    val ds = testDataset[Long](planId = 10)
    val scalarCol = ds.scalar()
    val combined = Column("id") === scalarCol
    combined.subqueryRelations should have size 1
    combined.subqueryRelations.head.getCommon.getPlanId shouldBe 10L
  }

  test("multiple subquery Columns merged via binary op") {
    val ds1 = testDataset[Long](planId = 10)
    val ds2 = testDataset[Long](planId = 20)
    val combined = ds1.scalar() === ds2.scalar()
    combined.subqueryRelations should have size 2
    combined.subqueryRelations.map(_.getCommon.getPlanId).toSet shouldBe Set(10L, 20L)
  }

  // ---------------------------------------------------------------------------
  // filter produces WithRelations when subquery Column is used
  // ---------------------------------------------------------------------------

  test("filter with subquery Column produces WithRelations") {
    val df = testDataFrame(planId = 1)
    val sub = testDataset[Long](planId = 50)
    val result = df.filter(Column("id") === sub.scalar())

    result.relation.hasWithRelations shouldBe true
    val wr = result.relation.getWithRelations
    wr.getRoot.hasFilter shouldBe true
    wr.getReferencesCount shouldBe 1
    wr.getReferences(0).getCommon.getPlanId shouldBe 50L
  }

  // ---------------------------------------------------------------------------
  // select produces WithRelations when subquery Column is used
  // ---------------------------------------------------------------------------

  test("select with subquery Column produces WithRelations") {
    val df = testDataFrame(planId = 1)
    val sub = testDataset[Long](planId = 60)
    val result = df.select(sub.scalar().as("max_val"))

    result.relation.hasWithRelations shouldBe true
    val wr = result.relation.getWithRelations
    wr.getRoot.hasProject shouldBe true
    wr.getReferencesCount shouldBe 1
    wr.getReferences(0).getCommon.getPlanId shouldBe 60L
  }

  // ---------------------------------------------------------------------------
  // No subquery = no WithRelations wrapper
  // ---------------------------------------------------------------------------

  test("filter without subquery does not produce WithRelations") {
    val df = testDataFrame(planId = 1)
    val result = df.filter(Column("id") > Column.lit(0))

    result.relation.hasWithRelations shouldBe false
    result.relation.hasFilter shouldBe true
  }

  test("select without subquery does not produce WithRelations") {
    val df = testDataFrame(planId = 1)
    val result = df.select(Column("id"), Column("name"))

    result.relation.hasWithRelations shouldBe false
    result.relation.hasProject shouldBe true
  }

  // ---------------------------------------------------------------------------
  // plan_id dedup
  // ---------------------------------------------------------------------------

  test("same subquery used twice: WithRelations.references deduplicated by plan_id") {
    val df = testDataFrame(planId = 1)
    val sub = testDataset[Long](planId = 88)
    val scalarCol = sub.scalar()
    // Use the same subquery Column twice in a filter expression
    val result = df.filter(scalarCol > Column.lit(0) && scalarCol < Column.lit(100))

    result.relation.hasWithRelations shouldBe true
    val wr = result.relation.getWithRelations
    // Despite the Column carrying two references (from && merging), they are deduplicated
    wr.getReferencesCount shouldBe 1
    wr.getReferences(0).getCommon.getPlanId shouldBe 88L
  }
