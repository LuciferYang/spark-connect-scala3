package org.apache.spark.sql

import org.apache.spark.connect.proto.{Expression, Relation, SubqueryExpression}

/** Helper for constructing subquery Column expressions.
  *
  * Consolidates the shared proto-construction pattern used by:
  *   - `Column.isin(Dataset[?])` / `Column.isin(DataFrame)` — IN-subqueries
  *   - `DataFrame.scalar()` / `Dataset.scalar()` — scalar subqueries
  *   - `DataFrame.exists()` / `Dataset.exists()` — EXISTS subqueries
  *
  * Each of those call sites previously duplicated the same `require(rel.hasCommon, ...)` guard,
  * plan-id extraction, and `SubqueryExpression` builder wiring. All share identical proto shape;
  * only the `SubqueryType` enum value, the optional in-subquery values list, and the base
  * `subqueryRelations` to propagate differ.
  */
private[sql] object SubqueryBuilder:

  /** Build a subquery Column.
    *
    * @param rel the DataFrame relation to wrap as a subquery
    * @param subqueryType IN / SCALAR / EXISTS
    * @param description human-readable description used in the `require` error message, e.g.
    *   "DataFrame used as a scalar subquery" or "DataFrame used in IN-subquery"
    * @param inValues values list for IN subqueries; ignored (empty) for SCALAR/EXISTS
    * @param baseRelations additional subquery relations to prepend before appending `rel`;
    *   used by `Column.buildInSubquery` to inherit the receiver's accumulated relations
    */
  def build(
      rel: Relation,
      subqueryType: SubqueryExpression.SubqueryType,
      description: String,
      inValues: java.util.List[Expression] = java.util.List.of(),
      baseRelations: Seq[Relation] = Seq.empty
  ): Column =
    require(
      rel.hasCommon,
      s"$description has no RelationCommon (plan_id missing) — " +
        "ensure the DataFrame was constructed through a SparkSession"
    )
    val planId = rel.getCommon.getPlanId
    val sqBuilder = SubqueryExpression.newBuilder()
      .setPlanId(planId)
      .setSubqueryType(subqueryType)
    if !inValues.isEmpty then sqBuilder.addAllInSubqueryValues(inValues)
    val subExpr = Expression.newBuilder()
      .setSubqueryExpression(sqBuilder.build())
      .build()
    Column(subExpr, baseRelations :+ rel)
