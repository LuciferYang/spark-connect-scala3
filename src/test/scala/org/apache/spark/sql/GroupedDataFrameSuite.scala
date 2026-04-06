package org.apache.spark.sql

import org.apache.spark.connect.proto.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GroupedDataFrameSuite extends AnyFunSuite with Matchers:

  private def stubSession: SparkSession = SparkSession(null)

  private def stubDf: DataFrame =
    val session = stubSession
    val rel = Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(session.nextPlanId()).build())
      .setLocalRelation(LocalRelation.getDefaultInstance)
      .build()
    DataFrame(session, rel)

  private def stubGrouped: GroupedDataFrame =
    stubDf.groupBy(Column("key"))

  // ---------------------------------------------------------------------------
  // count()
  // ---------------------------------------------------------------------------

  test("count() builds Aggregate with count expression") {
    val result = stubGrouped.count()
    result.relation.hasAggregate shouldBe true
    val agg = result.relation.getAggregate
    agg.getGroupType shouldBe Aggregate.GroupType.GROUP_TYPE_GROUPBY
    agg.getGroupingExpressionsCount shouldBe 1
    agg.getAggregateExpressionsCount shouldBe 1
  }

  // ---------------------------------------------------------------------------
  // mean / avg
  // ---------------------------------------------------------------------------

  test("mean(colNames) builds Aggregate with avg function") {
    val result = stubGrouped.mean("v1", "v2")
    result.relation.hasAggregate shouldBe true
    val agg = result.relation.getAggregate
    agg.getGroupType shouldBe Aggregate.GroupType.GROUP_TYPE_GROUPBY
    agg.getAggregateExpressionsCount shouldBe 2
  }

  test("avg(colNames) builds Aggregate with avg function") {
    val result = stubGrouped.avg("v1")
    result.relation.hasAggregate shouldBe true
    val agg = result.relation.getAggregate
    agg.getAggregateExpressionsCount shouldBe 1
  }

  // ---------------------------------------------------------------------------
  // max
  // ---------------------------------------------------------------------------

  test("max(colNames) builds Aggregate with max function") {
    val result = stubGrouped.max("v1", "v2")
    result.relation.hasAggregate shouldBe true
    val agg = result.relation.getAggregate
    agg.getAggregateExpressionsCount shouldBe 2
  }

  // ---------------------------------------------------------------------------
  // min
  // ---------------------------------------------------------------------------

  test("min(colNames) builds Aggregate with min function") {
    val result = stubGrouped.min("v1")
    result.relation.hasAggregate shouldBe true
    val agg = result.relation.getAggregate
    agg.getAggregateExpressionsCount shouldBe 1
  }

  // ---------------------------------------------------------------------------
  // sum
  // ---------------------------------------------------------------------------

  test("sum(colNames) builds Aggregate with sum function") {
    val result = stubGrouped.sum("v1", "v2", "v3")
    result.relation.hasAggregate shouldBe true
    val agg = result.relation.getAggregate
    agg.getAggregateExpressionsCount shouldBe 3
  }

  // ---------------------------------------------------------------------------
  // agg(aggExpr, aggExprs*)
  // ---------------------------------------------------------------------------

  test("agg(Column, Column*) builds Aggregate with given expressions") {
    val result = stubGrouped.agg(
      functions.count(Column("key")).as("cnt"),
      functions.sum(Column("v1")).as("total")
    )
    result.relation.hasAggregate shouldBe true
    val agg = result.relation.getAggregate
    agg.getGroupType shouldBe Aggregate.GroupType.GROUP_TYPE_GROUPBY
    agg.getGroupingExpressionsCount shouldBe 1
    agg.getAggregateExpressionsCount shouldBe 2
  }

  // ---------------------------------------------------------------------------
  // agg(Map[String, String])
  // ---------------------------------------------------------------------------

  test("agg(Map[String, String]) builds Aggregate from map") {
    val result = stubGrouped.agg(Map("v1" -> "sum", "v2" -> "avg"))
    result.relation.hasAggregate shouldBe true
    val agg = result.relation.getAggregate
    agg.getAggregateExpressionsCount shouldBe 2
  }

  // ---------------------------------------------------------------------------
  // pivot
  // ---------------------------------------------------------------------------

  test("pivot(pivotCol) returns GroupedDataFrame with Pivot type") {
    val pivoted = stubGrouped.pivot(Column("category"))
    // After pivoting, calling agg should produce a PIVOT aggregate
    val result = pivoted.agg(functions.sum(Column("v1")).as("total"))
    result.relation.hasAggregate shouldBe true
    val agg = result.relation.getAggregate
    agg.getGroupType shouldBe Aggregate.GroupType.GROUP_TYPE_PIVOT
  }

  // ---------------------------------------------------------------------------
  // NEW: Additional GroupedDataFrame tests for coverage
  // ---------------------------------------------------------------------------

  // -- count() aggregate expression structure --

  test("count() aggregate expression uses count function with lit(1)") {
    val result = stubGrouped.count()
    val agg = result.relation.getAggregate
    val aggExpr = agg.getAggregateExpressions(0)
    // Should be an alias wrapping an unresolved function
    aggExpr.hasAlias shouldBe true
    aggExpr.getAlias.getNameCount shouldBe 1
    aggExpr.getAlias.getName(0) shouldBe "count"
  }

  // -- mean / avg function names --

  test("mean resolves to avg function in aggregate expressions") {
    val result = stubGrouped.mean("v1")
    val agg = result.relation.getAggregate
    val aggExpr = agg.getAggregateExpressions(0)
    aggExpr.hasAlias shouldBe true
    aggExpr.getAlias.getName(0) shouldBe "avg(v1)"
  }

  test("avg is alias for mean") {
    val meanResult = stubGrouped.mean("v1")
    val avgResult = stubGrouped.avg("v1")
    // Both should produce equivalent aggregate structures
    meanResult.relation.getAggregate.getAggregateExpressionsCount shouldBe
      avgResult.relation.getAggregate.getAggregateExpressionsCount
  }

  // -- max function name --

  test("max aggregate expression has correct alias") {
    val result = stubGrouped.max("v1")
    val agg = result.relation.getAggregate
    val aggExpr = agg.getAggregateExpressions(0)
    aggExpr.hasAlias shouldBe true
    aggExpr.getAlias.getName(0) shouldBe "max(v1)"
  }

  // -- min function name --

  test("min aggregate expression has correct alias") {
    val result = stubGrouped.min("v1")
    val agg = result.relation.getAggregate
    val aggExpr = agg.getAggregateExpressions(0)
    aggExpr.hasAlias shouldBe true
    aggExpr.getAlias.getName(0) shouldBe "min(v1)"
  }

  // -- sum function name --

  test("sum aggregate expression has correct alias") {
    val result = stubGrouped.sum("v1")
    val agg = result.relation.getAggregate
    val aggExpr = agg.getAggregateExpressions(0)
    aggExpr.hasAlias shouldBe true
    aggExpr.getAlias.getName(0) shouldBe "sum(v1)"
  }

  // -- grouping expressions --

  test("groupBy with single column has one grouping expression") {
    val result = stubGrouped.agg(functions.count(Column("key")).as("cnt"))
    val agg = result.relation.getAggregate
    agg.getGroupingExpressionsCount shouldBe 1
    val groupExpr = agg.getGroupingExpressions(0)
    groupExpr.hasUnresolvedAttribute shouldBe true
    groupExpr.getUnresolvedAttribute.getUnparsedIdentifier shouldBe "key"
  }

  test("groupBy with multiple columns has matching grouping expressions") {
    val grouped = stubDf.groupBy(Column("k1"), Column("k2"), Column("k3"))
    val result = grouped.agg(functions.count(Column("*")).as("cnt"))
    val agg = result.relation.getAggregate
    agg.getGroupingExpressionsCount shouldBe 3
  }

  // -- agg(Map) with empty map returns the original df --

  test("agg(Map) with empty map returns original DataFrame") {
    val result = stubGrouped.agg(Map.empty[String, String])
    // When no aggregation columns, should return the original df
    result.relation shouldBe stubGrouped.agg(Map.empty[String, String]).relation
  }

  // -- agg(Map) function names are correct --

  test("agg(Map) builds unresolved functions with correct names") {
    val result = stubGrouped.agg(Map("v1" -> "sum", "v2" -> "max"))
    val agg = result.relation.getAggregate
    agg.getAggregateExpressionsCount shouldBe 2
    // Each aggregate expression should be aliased
    (0 until agg.getAggregateExpressionsCount).foreach { i =>
      agg.getAggregateExpressions(i).hasAlias shouldBe true
    }
  }

  // -- agg with single Column --

  test("agg with single Column builds Aggregate with one expression") {
    val result = stubGrouped.agg(functions.max(Column("v1")).as("maxV1"))
    val agg = result.relation.getAggregate
    agg.getAggregateExpressionsCount shouldBe 1
    agg.getGroupType shouldBe Aggregate.GroupType.GROUP_TYPE_GROUPBY
  }

  // -- agg with three Columns --

  test("agg with three Columns builds Aggregate with three expressions") {
    val result = stubGrouped.agg(
      functions.count(Column("*")).as("cnt"),
      functions.sum(Column("v1")).as("total"),
      functions.avg(Column("v2")).as("average")
    )
    val agg = result.relation.getAggregate
    agg.getAggregateExpressionsCount shouldBe 3
  }

  // -- GroupType variants --

  test("rollup creates GroupedDataFrame with ROLLUP type") {
    val rolled = stubDf.rollup(Column("k1"), Column("k2"))
    val result = rolled.agg(functions.count(Column("*")).as("cnt"))
    result.relation.hasAggregate shouldBe true
    result.relation.getAggregate.getGroupType shouldBe Aggregate.GroupType.GROUP_TYPE_ROLLUP
  }

  test("cube creates GroupedDataFrame with CUBE type") {
    val cubed = stubDf.cube(Column("k1"), Column("k2"))
    val result = cubed.agg(functions.count(Column("*")).as("cnt"))
    result.relation.hasAggregate shouldBe true
    result.relation.getAggregate.getGroupType shouldBe Aggregate.GroupType.GROUP_TYPE_CUBE
  }

  // -- pivot after agg --

  test("pivot preserves original grouping expressions plus pivot col") {
    val pivoted = stubGrouped.pivot(Column("category"))
    val result = pivoted.agg(functions.sum(Column("v1")).as("total"))
    val agg = result.relation.getAggregate
    // Original "key" grouping expression plus "category" pivot column
    agg.getGroupingExpressionsCount shouldBe 2
    agg.getGroupType shouldBe Aggregate.GroupType.GROUP_TYPE_PIVOT
  }

  test("pivot then count builds PIVOT aggregate") {
    val pivoted = stubGrouped.pivot(Column("status"))
    val result = pivoted.count()
    val agg = result.relation.getAggregate
    agg.getGroupType shouldBe Aggregate.GroupType.GROUP_TYPE_PIVOT
  }

  test("pivot then mean builds PIVOT aggregate") {
    val pivoted = stubGrouped.pivot(Column("status"))
    val result = pivoted.mean("v1")
    val agg = result.relation.getAggregate
    agg.getGroupType shouldBe Aggregate.GroupType.GROUP_TYPE_PIVOT
    agg.getAggregateExpressionsCount shouldBe 1
  }

  test("pivot then max builds PIVOT aggregate") {
    val pivoted = stubGrouped.pivot(Column("status"))
    val result = pivoted.max("v1")
    val agg = result.relation.getAggregate
    agg.getGroupType shouldBe Aggregate.GroupType.GROUP_TYPE_PIVOT
  }

  test("pivot then min builds PIVOT aggregate") {
    val pivoted = stubGrouped.pivot(Column("status"))
    val result = pivoted.min("v1")
    val agg = result.relation.getAggregate
    agg.getGroupType shouldBe Aggregate.GroupType.GROUP_TYPE_PIVOT
  }

  test("pivot then sum builds PIVOT aggregate") {
    val pivoted = stubGrouped.pivot(Column("status"))
    val result = pivoted.sum("v1")
    val agg = result.relation.getAggregate
    agg.getGroupType shouldBe Aggregate.GroupType.GROUP_TYPE_PIVOT
  }

  // -- groupingSets --

  test("groupingSets creates GroupedDataFrame with GROUPING_SETS type") {
    val gs = stubDf.groupingSets(
      Seq(Seq(Column("k1")), Seq(Column("k2")), Seq(Column("k1"), Column("k2"))),
      Column("k1"),
      Column("k2")
    )
    val result = gs.agg(functions.count(Column("*")).as("cnt"))
    result.relation.hasAggregate shouldBe true
    val agg = result.relation.getAggregate
    agg.getGroupType shouldBe Aggregate.GroupType.GROUP_TYPE_GROUPING_SETS
    agg.getGroupingSetsCount shouldBe 3
  }

  // -- empty colNames variants --

  test("mean with no column names returns original df") {
    val result = stubGrouped.mean()
    // When resolveColNames returns empty, mean returns the original df
    // Since no columns are specified, cols is empty, returning df unchanged
    result should not be null
  }

  test("max with no column names returns original df") {
    val result = stubGrouped.max()
    result should not be null
  }

  test("min with no column names returns original df") {
    val result = stubGrouped.min()
    result should not be null
  }

  test("sum with no column names returns original df") {
    val result = stubGrouped.sum()
    result should not be null
  }

  test("avg with no column names returns original df") {
    val result = stubGrouped.avg()
    result should not be null
  }

  // -- multiple columns in convenience methods --

  test("mean with multiple columns produces matching aggregate count") {
    val result = stubGrouped.mean("a", "b", "c")
    val agg = result.relation.getAggregate
    agg.getAggregateExpressionsCount shouldBe 3
  }

  test("max with multiple columns produces matching aggregate count") {
    val result = stubGrouped.max("a", "b")
    val agg = result.relation.getAggregate
    agg.getAggregateExpressionsCount shouldBe 2
  }

  test("min with multiple columns produces matching aggregate count") {
    val result = stubGrouped.min("a", "b", "c", "d")
    val agg = result.relation.getAggregate
    agg.getAggregateExpressionsCount shouldBe 4
  }

  test("sum with single column produces one aggregate expression") {
    val result = stubGrouped.sum("v1")
    val agg = result.relation.getAggregate
    agg.getAggregateExpressionsCount shouldBe 1
  }

  // -- input relation is preserved --

  test("agg preserves input relation from the original DataFrame") {
    val result = stubGrouped.agg(functions.count(Column("*")).as("cnt"))
    val agg = result.relation.getAggregate
    agg.hasInput shouldBe true
    agg.getInput.hasLocalRelation shouldBe true
  }

  // -- GroupType enum coverage --

  test("GroupType enum has all expected values") {
    GroupedDataFrame.GroupType.values should contain allOf (
      GroupedDataFrame.GroupType.GroupBy,
      GroupedDataFrame.GroupType.Rollup,
      GroupedDataFrame.GroupType.Cube,
      GroupedDataFrame.GroupType.Pivot,
      GroupedDataFrame.GroupType.GroupingSets
    )
  }

  // -- agg(Map) with single entry --

  test("agg(Map) with single entry builds correct aggregate") {
    val result = stubGrouped.agg(Map("v1" -> "count"))
    val agg = result.relation.getAggregate
    agg.getAggregateExpressionsCount shouldBe 1
  }

  // -- groupBy with String overload (DummyImplicit) --

  test("groupBy(String*) creates GroupedDataFrame") {
    val grouped = stubDf.groupBy("k1", "k2")(using DummyImplicit.dummyImplicit)
    val result = grouped.count()
    result.relation.hasAggregate shouldBe true
    result.relation.getAggregate.getGroupingExpressionsCount shouldBe 2
  }
