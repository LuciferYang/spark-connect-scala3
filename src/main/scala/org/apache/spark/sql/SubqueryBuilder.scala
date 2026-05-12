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
  * only the `SubqueryKind`, the optional in-subquery values list, and the base `subqueryRelations`
  * to propagate differ.
  */
private[sql] object SubqueryBuilder:

  /** The three subquery categories supported by the Spark Connect proto. Each carries its
    * corresponding proto enum value and a stable description used in error messages — avoids
    * stringly-typed call sites and keeps messages uniform.
    */
  enum SubqueryKind(val protoType: SubqueryExpression.SubqueryType, val description: String):
    case In extends SubqueryKind(
          SubqueryExpression.SubqueryType.SUBQUERY_TYPE_IN,
          "DataFrame used in IN-subquery"
        )
    case ScalarFromDataFrame extends SubqueryKind(
          SubqueryExpression.SubqueryType.SUBQUERY_TYPE_SCALAR,
          "DataFrame used as a scalar subquery"
        )
    case ExistsFromDataFrame extends SubqueryKind(
          SubqueryExpression.SubqueryType.SUBQUERY_TYPE_EXISTS,
          "DataFrame used as an EXISTS subquery"
        )
    case ScalarFromDataset extends SubqueryKind(
          SubqueryExpression.SubqueryType.SUBQUERY_TYPE_SCALAR,
          "Dataset used as a scalar subquery"
        )
    case ExistsFromDataset extends SubqueryKind(
          SubqueryExpression.SubqueryType.SUBQUERY_TYPE_EXISTS,
          "Dataset used as an EXISTS subquery"
        )

  /** Build a subquery Column.
    *
    * @param rel
    *   the DataFrame relation to wrap as a subquery
    * @param kind
    *   which subquery category this is (determines proto type + error message wording)
    * @param inValues
    *   values list for IN subqueries; ignored (empty) for SCALAR/EXISTS
    * @param baseRelations
    *   additional subquery relations to prepend before appending `rel`; used by
    *   `Column.buildInSubquery` to inherit the receiver's accumulated relations.
    *   DataFrame/Dataset's `scalar()`/`exists()` pass the default empty — these callers have no
    *   upstream `subqueryRelations` to carry (only `Column` accumulates them).
    */
  def build(
      rel: Relation,
      kind: SubqueryKind,
      inValues: java.util.List[Expression] = java.util.List.of(),
      baseRelations: Seq[Relation] = Seq.empty
  ): Column =
    require(
      rel.hasCommon,
      s"${kind.description} has no RelationCommon (plan_id missing) — " +
        "ensure the DataFrame was constructed through a SparkSession"
    )
    val planId = rel.getCommon.getPlanId
    val sqBuilder = SubqueryExpression.newBuilder()
      .setPlanId(planId)
      .setSubqueryType(kind.protoType)
      .addAllInSubqueryValues(inValues) // no-op for empty list (SCALAR/EXISTS)
    val subExpr = Expression.newBuilder()
      .setSubqueryExpression(sqBuilder.build())
      .build()
    Column(subExpr, baseRelations :+ rel)
