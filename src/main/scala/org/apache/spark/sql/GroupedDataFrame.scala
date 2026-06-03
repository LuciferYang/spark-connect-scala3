package org.apache.spark.sql

import org.apache.spark.connect.proto.*

import scala.reflect.ClassTag

/** Returned by `DataFrame.groupBy`, `rollup`, or `cube`. Use `.agg(...)` to specify aggregate
  * expressions.
  */
class RelationalGroupedDataset private[sql] (
    private val df: DataFrame,
    private val groupingExprs: Seq[Column],
    private val groupType: RelationalGroupedDataset.GroupType,
    private val groupingSetsProto: Option[Seq[Aggregate.GroupingSets]] = None,
    private val pivotCol: Option[Column] = None,
    private val pivotValues: Seq[Expression.Literal] = Seq.empty
):

  def agg(aggExpr: Column, aggExprs: Column*): DataFrame =
    val allAggs = aggExpr +: aggExprs
    val aggBuilder = Aggregate.newBuilder()
      .setInput(df.relation)
      .setGroupType(groupType match
        case RelationalGroupedDataset.GroupType.GroupBy => Aggregate.GroupType.GROUP_TYPE_GROUPBY
        case RelationalGroupedDataset.GroupType.Rollup  => Aggregate.GroupType.GROUP_TYPE_ROLLUP
        case RelationalGroupedDataset.GroupType.Cube    => Aggregate.GroupType.GROUP_TYPE_CUBE
        case RelationalGroupedDataset.GroupType.Pivot   => Aggregate.GroupType.GROUP_TYPE_PIVOT
        case RelationalGroupedDataset.GroupType.GroupingSets =>
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

  def pivot(pivotCol: Column): RelationalGroupedDataset =
    new RelationalGroupedDataset(
      df,
      groupingExprs,
      RelationalGroupedDataset.GroupType.Pivot,
      groupingSetsProto,
      pivotCol = Some(pivotCol)
    )

  def pivot(pivotCol: Column, values: Seq[Any]): RelationalGroupedDataset =
    val litValues = values.map { v =>
      Column.lit(v).expr.getLiteral
    }
    new RelationalGroupedDataset(
      df,
      groupingExprs,
      RelationalGroupedDataset.GroupType.Pivot,
      groupingSetsProto,
      pivotCol = Some(pivotCol),
      pivotValues = litValues
    )

  def pivot(pivotColumn: String): RelationalGroupedDataset =
    pivot(Column(pivotColumn))

  def pivot(pivotColumn: String, values: Seq[Any]): RelationalGroupedDataset =
    pivot(Column(pivotColumn), values)

  def pivot(pivotColumn: String, values: java.util.List[Any]): RelationalGroupedDataset =
    import scala.jdk.CollectionConverters.*
    pivot(Column(pivotColumn), values.asScala.toSeq)

  def pivot(pivotCol: Column, values: java.util.List[Any]): RelationalGroupedDataset =
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

object RelationalGroupedDataset:
  enum GroupType:
    case GroupBy, Rollup, Cube, Pivot, GroupingSets

  private[sql] def apply(
      df: DataFrame,
      groupingExprs: Seq[Column],
      groupType: GroupType
  ): RelationalGroupedDataset =
    new RelationalGroupedDataset(df, groupingExprs, groupType)

  private[sql] def apply(
      df: DataFrame,
      groupingExprs: Seq[Column],
      groupType: GroupType,
      groupingSetsProto: Option[Seq[Aggregate.GroupingSets]]
  ): RelationalGroupedDataset =
    new RelationalGroupedDataset(
      df,
      groupingExprs,
      groupType,
      groupingSetsProto = groupingSetsProto
    )

/** Backward-compatible name retained for code written against this client before the Spark
  * `RelationalGroupedDataset` name was added.
  */
final class GroupedDataFrame private[sql] (
    df: DataFrame,
    groupingExprs: Seq[Column],
    groupType: RelationalGroupedDataset.GroupType,
    groupingSetsProto: Option[Seq[Aggregate.GroupingSets]] = None,
    pivotCol: Option[Column] = None,
    pivotValues: Seq[Expression.Literal] = Seq.empty
) extends RelationalGroupedDataset(
      df,
      groupingExprs,
      groupType,
      groupingSetsProto,
      pivotCol,
      pivotValues
    )

object GroupedDataFrame:
  type GroupType = RelationalGroupedDataset.GroupType
  val GroupType: RelationalGroupedDataset.GroupType.type = RelationalGroupedDataset.GroupType

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
