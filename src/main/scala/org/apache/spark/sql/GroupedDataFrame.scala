package org.apache.spark.sql

import org.apache.spark.connect.proto.*

/** Returned by `DataFrame.groupBy`, `rollup`, or `cube`. Use `.agg(...)` to specify aggregate
  * expressions.
  */
final class GroupedDataFrame private[sql] (
    private val df: DataFrame,
    private val groupingExprs: Seq[Column],
    private val groupType: GroupedDataFrame.GroupType,
    private val groupingSetsProto: Option[Seq[Aggregate.GroupingSets]] = None,
    private val pivotCol: Option[Column] = None,
    private val pivotValues: Seq[Expression.Literal] = Seq.empty
):

  def agg(aggExpr: Column, aggExprs: Column*): DataFrame =
    val allAggs = aggExpr +: aggExprs
    val aggBuilder = Aggregate.newBuilder()
      .setInput(df.relation)
      .setGroupType(groupType match
        case GroupedDataFrame.GroupType.GroupBy      => Aggregate.GroupType.GROUP_TYPE_GROUPBY
        case GroupedDataFrame.GroupType.Rollup       => Aggregate.GroupType.GROUP_TYPE_ROLLUP
        case GroupedDataFrame.GroupType.Cube         => Aggregate.GroupType.GROUP_TYPE_CUBE
        case GroupedDataFrame.GroupType.Pivot        => Aggregate.GroupType.GROUP_TYPE_PIVOT
        case GroupedDataFrame.GroupType.GroupingSets =>
          Aggregate.GroupType.GROUP_TYPE_GROUPING_SETS)
    groupingExprs.foreach(c => aggBuilder.addGroupingExpressions(c.expr))
    allAggs.foreach(c => aggBuilder.addAggregateExpressions(c.expr))
    groupingSetsProto.foreach(_.foreach(gs => aggBuilder.addGroupingSets(gs)))
    pivotCol.foreach { pc =>
      val pivotBuilder = Aggregate.Pivot.newBuilder().setCol(pc.expr)
      pivotValues.foreach(pivotBuilder.addValues)
      aggBuilder.setPivot(pivotBuilder.build())
    }
    val allCols = groupingExprs ++ pivotCol.toSeq ++ allAggs
    df.withRelation(allCols)(_.setAggregate(aggBuilder.build()))

  def agg(exprs: Map[String, String]): DataFrame =
    val aggCols = exprs.map { (colName, funcName) =>
      Column(Expression.newBuilder().setUnresolvedFunction(
        Expression.UnresolvedFunction.newBuilder()
          .setFunctionName(funcName)
          .addArguments(Column(colName).expr)
          .build()
      ).build()).as(s"$funcName($colName)")
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
    new GroupedDataFrame(
      df,
      groupingExprs,
      GroupedDataFrame.GroupType.Pivot,
      groupingSetsProto,
      pivotCol = Some(pivotCol)
    )

  def pivot(pivotCol: Column, values: Seq[Any]): GroupedDataFrame =
    val litValues = values.map { v =>
      Column.lit(v).expr.getLiteral
    }
    new GroupedDataFrame(
      df,
      groupingExprs,
      GroupedDataFrame.GroupType.Pivot,
      groupingSetsProto,
      pivotCol = Some(pivotCol),
      pivotValues = litValues
    )

  private def resolveColNames(colNames: Seq[String], funcName: String): Seq[Column] =
    colNames.map { name =>
      Column(Expression.newBuilder().setUnresolvedFunction(
        Expression.UnresolvedFunction.newBuilder()
          .setFunctionName(funcName)
          .addArguments(Column(name).expr)
          .build()
      ).build()).as(s"$funcName($name)")
    }

object GroupedDataFrame:
  enum GroupType:
    case GroupBy, Rollup, Cube, Pivot, GroupingSets

  private[sql] def apply(
      df: DataFrame,
      groupingExprs: Seq[Column],
      groupType: GroupType
  ): GroupedDataFrame =
    new GroupedDataFrame(df, groupingExprs, groupType)

  private[sql] def apply(
      df: DataFrame,
      groupingExprs: Seq[Column],
      groupType: GroupType,
      groupingSetsProto: Option[Seq[Aggregate.GroupingSets]]
  ): GroupedDataFrame =
    new GroupedDataFrame(
      df,
      groupingExprs,
      groupType,
      groupingSetsProto = groupingSetsProto
    )
