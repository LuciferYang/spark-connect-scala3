package org.apache.spark.sql

import org.apache.spark.connect.proto.expressions.Expression
import org.apache.spark.connect.proto.expressions.Expression.ExprType
import org.apache.spark.connect.proto.relations.*

/**
 * Returned by `DataFrame.groupBy`, `rollup`, or `cube`.
 * Use `.agg(...)` to specify aggregate expressions.
 */
final class GroupedDataFrame private[sql] (
    private val df: DataFrame,
    private val groupingExprs: Seq[Column],
    private val groupType: GroupedDataFrame.GroupType
):

  def agg(aggExpr: Column, aggExprs: Column*): DataFrame =
    val allAggs = aggExpr +: aggExprs
    val aggregate = Aggregate(
      input = Some(df.relation),
      groupType = groupType match
        case GroupedDataFrame.GroupType.GroupBy => Aggregate.GroupType.GROUP_TYPE_GROUPBY
        case GroupedDataFrame.GroupType.Rollup  => Aggregate.GroupType.GROUP_TYPE_ROLLUP
        case GroupedDataFrame.GroupType.Cube    => Aggregate.GroupType.GROUP_TYPE_CUBE
        case GroupedDataFrame.GroupType.Pivot   => Aggregate.GroupType.GROUP_TYPE_PIVOT,
      groupingExpressions = groupingExprs.map(_.expr).toSeq,
      aggregateExpressions = allAggs.map(_.expr).toSeq
    )
    DataFrame(df.session, Relation(
      common = Some(RelationCommon(planId = Some(df.session.nextPlanId()))),
      relType = Relation.RelType.Aggregate(aggregate)
    ))

  def agg(exprs: Map[String, String]): DataFrame =
    val aggCols = exprs.map { (colName, funcName) =>
      Column(Expression(exprType = ExprType.UnresolvedFunction(
        Expression.UnresolvedFunction(
          functionName = funcName,
          arguments = Seq(Column(colName).expr)
        )
      ))).as(s"$funcName($colName)")
    }.toSeq
    if aggCols.isEmpty then
      df
    else
      agg(aggCols.head, aggCols.tail*)

  // Convenience methods
  def count(): DataFrame = agg(functions.count(functions.lit(1)).as("count"))

  def mean(colNames: String*): DataFrame =
    val cols = resolveColNames(colNames, "avg")
    if cols.isEmpty then df else agg(cols.head, cols.tail*)

  def avg(colNames: String*): DataFrame = mean(colNames*)

  def max(colNames: String*): DataFrame =
    val cols = resolveColNames(colNames, "max")
    if cols.isEmpty then df else agg(cols.head, cols.tail*)

  def min(colNames: String*): DataFrame =
    val cols = resolveColNames(colNames, "min")
    if cols.isEmpty then df else agg(cols.head, cols.tail*)

  def sum(colNames: String*): DataFrame =
    val cols = resolveColNames(colNames, "sum")
    if cols.isEmpty then df else agg(cols.head, cols.tail*)

  def pivot(pivotCol: Column): GroupedDataFrame =
    GroupedDataFrame(df, groupingExprs :+ pivotCol, GroupedDataFrame.GroupType.Pivot)

  private def resolveColNames(colNames: Seq[String], funcName: String): Seq[Column] =
    colNames.map { name =>
      Column(Expression(exprType = ExprType.UnresolvedFunction(
        Expression.UnresolvedFunction(
          functionName = funcName,
          arguments = Seq(Column(name).expr)
        )
      ))).as(s"$funcName($name)")
    }

object GroupedDataFrame:
  enum GroupType:
    case GroupBy, Rollup, Cube, Pivot

  private[sql] def apply(
      df: DataFrame,
      groupingExprs: Seq[Column],
      groupType: GroupType
  ): GroupedDataFrame =
    new GroupedDataFrame(df, groupingExprs, groupType)
