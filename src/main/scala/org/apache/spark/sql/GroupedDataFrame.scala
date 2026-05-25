package org.apache.spark.sql

import org.apache.spark.connect.proto.*

import scala.reflect.ClassTag

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

  /** Aggregate using `(columnName, functionName)` pairs.
    *
    * Sequence order is preserved and duplicate column keys are NOT collapsed — this matches
    * upstream `RelationalGroupedDataset.agg(aggExpr, aggExprs*)`. The earlier `.toMap`-based
    * implementation silently dropped earlier `(col, fn)` pairs when a later pair shared the same
    * column name, so e.g. `agg("salary" -> "max", "salary" -> "min")` lost the max aggregate.
    */
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame =
    val pairs = aggExpr +: aggExprs
    val aggCols = pairs.map((colName, funcName) => buildAggCol(colName, funcName))
    agg(aggCols.head, aggCols.tail*)

  def agg(exprs: Map[String, String]): DataFrame =
    val aggCols = exprs.map { (colName, funcName) =>
      buildAggCol(colName, funcName)
    }.toSeq
    if aggCols.isEmpty then
      df
    else
      agg(aggCols.head, aggCols.tail*)

  private def buildAggCol(colName: String, funcName: String): Column =
    Column(
      Expression.newBuilder().setUnresolvedFunction(
        Expression.UnresolvedFunction.newBuilder()
          .setFunctionName(funcName)
          .addArguments(Column(colName).expr)
          .build()
      ).build()
    ).as(s"$funcName($colName)")

  /** Java-friendly variant of agg using java.util.Map. */
  def agg(exprs: java.util.Map[String, String]): DataFrame =
    import scala.jdk.CollectionConverters.*
    agg(exprs.asScala.toMap)

  /** Convert this column-based grouped DataFrame into a typed `KeyValueGroupedDataset[K, V]`.
    *
    * K represents the grouping key type (must match the groupBy column types). V represents the
    * full row type.
    */
  def as[K: Encoder: ClassTag, V: Encoder: ClassTag]: KeyValueGroupedDataset[K, V] =
    KeyValueGroupedDataset.fromColumns[K, V](
      Dataset(df, summon[Encoder[V]]),
      groupingExprs
    )

  // Convenience methods
  def count(): DataFrame = agg(functions.count(functions.lit(1)).as("count"))

  def mean(colNames: String*): DataFrame = aggregateNumericColumns(colNames, "avg")

  def avg(colNames: String*): DataFrame = mean(colNames*)

  def max(colNames: String*): DataFrame = aggregateNumericColumns(colNames, "max")

  def min(colNames: String*): DataFrame = aggregateNumericColumns(colNames, "min")

  def sum(colNames: String*): DataFrame = aggregateNumericColumns(colNames, "sum")

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

  def pivot(pivotColumn: String): GroupedDataFrame =
    pivot(Column(pivotColumn))

  def pivot(pivotColumn: String, values: Seq[Any]): GroupedDataFrame =
    pivot(Column(pivotColumn), values)

  def pivot(pivotColumn: String, values: java.util.List[Any]): GroupedDataFrame =
    import scala.jdk.CollectionConverters.*
    pivot(Column(pivotColumn), values.asScala.toSeq)

  def pivot(pivotCol: Column, values: java.util.List[Any]): GroupedDataFrame =
    import scala.jdk.CollectionConverters.*
    pivot(pivotCol, values.asScala.toSeq)

  /** Aggregate one numeric function across the named columns. When `colNames` is empty, fall back
    * to all numeric fields of `df.schema` — matching upstream's `df.groupBy(...).mean()` (no-arg)
    * shorthand for "all numeric columns". The schema lookup triggers an `analyzePlan` RPC; pass
    * explicit `colNames` to keep the call lazy.
    */
  private def aggregateNumericColumns(colNames: Seq[String], funcName: String): DataFrame =
    val effective =
      if colNames.nonEmpty then colNames
      else
        df.schema.fields.collect {
          case f if isNumeric(f.dataType) => f.name
        }.toSeq
    val cols = effective.map(name => buildAggCol(name, funcName))
    if cols.isEmpty then df else agg(cols.head, cols.tail*)

  private def isNumeric(dt: org.apache.spark.sql.types.DataType): Boolean =
    import org.apache.spark.sql.types.*
    dt match
      case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType => true
      case _: DecimalType                                                         => true
      case _                                                                      => false

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
