package org.apache.spark.sql

import org.apache.spark.connect.proto.{
  Expression,
  Relation,
  SubqueryExpression,
  DataType as ProtoDataType
}
import org.apache.spark.sql.connect.client.DataTypeProtoConverter

import scala.jdk.CollectionConverters.*

/** A column expression in a DataFrame.
  *
  * Column objects are lazy — they build a protobuf Expression tree that gets sent to the server
  * only when an action is triggered.
  */
class Column private[sql] (
    private[sql] val expr: Expression,
    private[sql] val subqueryRelations: Seq[Relation] = Seq.empty
):

  // ---------------------------------------------------------------------------
  // Constructors
  // ---------------------------------------------------------------------------

  /** Reference a column by name. Handles `"*"` and `"xxx.*"` as `UnresolvedStar`. */
  def this(name: String) = this(
    name match
      case "*" =>
        Expression.newBuilder().setUnresolvedStar(
          Expression.UnresolvedStar.newBuilder().build()
        ).build()
      case s if s.endsWith(".*") =>
        Expression.newBuilder().setUnresolvedStar(
          Expression.UnresolvedStar.newBuilder().setUnparsedTarget(s).build()
        ).build()
      case _ =>
        Expression.newBuilder().setUnresolvedAttribute(
          Expression.UnresolvedAttribute.newBuilder().setUnparsedIdentifier(name).build()
        ).build(),
    Seq.empty
  )

  // ---------------------------------------------------------------------------
  // Internal helpers for subquery relation propagation
  // ---------------------------------------------------------------------------

  /** Create a new Column with the given expression, inheriting this Column's subqueryRelations. */
  private def withExpr(newExpr: Expression): Column =
    Column(newExpr, this.subqueryRelations)

  /** Create a new Column with the given expression, merging subqueryRelations from this and other.
    */
  private def withExprMerge(newExpr: Expression, other: Column): Column =
    Column(newExpr, this.subqueryRelations ++ other.subqueryRelations)

  // ---------------------------------------------------------------------------
  // Comparison operators
  // ---------------------------------------------------------------------------

  def ===(other: Column): Column = fn("==", other)
  def =!=(other: Column): Column = !(this === other)

  /** Null-safe equality (NULL <=> NULL is true). */
  def <=>(other: Any): Column = fn("<=>", Column.lit(other))

  /** Java alias for `<=>`. */
  def eqNullSafe(other: Any): Column = <=>(other)
  def >(other: Column): Column = fn(">", other)
  def >=(other: Column): Column = fn(">=", other)
  def <(other: Column): Column = fn("<", other)
  def <=(other: Column): Column = fn("<=", other)

  def ===(v: Any): Column = ===(Column.lit(v))
  def =!=(v: Any): Column = =!=(Column.lit(v))
  def >(v: Any): Column = >(Column.lit(v))
  def >=(v: Any): Column = >=(Column.lit(v))
  def <(v: Any): Column = <(Column.lit(v))
  def <=(v: Any): Column = <=(Column.lit(v))

  // Java-friendly comparison aliases
  def equalTo(other: Any): Column = ===(other)
  def notEqual(other: Any): Column = =!=(other)
  def gt(other: Any): Column = >(Column.lit(other))
  def lt(other: Any): Column = <(Column.lit(other))
  def geq(other: Any): Column = >=(Column.lit(other))
  def leq(other: Any): Column = <=(Column.lit(other))

  // ---------------------------------------------------------------------------
  // Logical operators
  // ---------------------------------------------------------------------------

  def &&(other: Column): Column = fn("and", other)
  def ||(other: Column): Column = fn("or", other)
  def unary_! : Column = fn0("not")

  // Java-friendly logical aliases
  def and(other: Column): Column = &&(other)
  def or(other: Column): Column = ||(other)

  // ---------------------------------------------------------------------------
  // Arithmetic operators
  // ---------------------------------------------------------------------------

  def +(other: Column): Column = fn("+", other)
  def -(other: Column): Column = fn("-", other)
  def *(other: Column): Column = fn("*", other)
  def /(other: Column): Column = fn("/", other)
  def %(other: Column): Column = fn("%", other)
  def unary_- : Column = fn0("negative")

  def plus(v: Any): Column = this + Column.lit(v)
  def minus(v: Any): Column = this - Column.lit(v)
  def multiply(v: Any): Column = this * Column.lit(v)
  def divide(v: Any): Column = this / Column.lit(v)
  def mod(v: Any): Column = this % Column.lit(v)

  // ---------------------------------------------------------------------------
  // Bitwise operators
  // ---------------------------------------------------------------------------

  def bitwiseOR(other: Any): Column = fn("|", Column.lit(other))
  def bitwiseAND(other: Any): Column = fn("&", Column.lit(other))
  def bitwiseXOR(other: Any): Column = fn("^", Column.lit(other))
  def |(other: Any): Column = bitwiseOR(other)
  def &(other: Any): Column = bitwiseAND(other)
  def ^(other: Any): Column = bitwiseXOR(other)

  // ---------------------------------------------------------------------------
  // Null / NaN checks
  // ---------------------------------------------------------------------------

  def isNull: Column = fn0("isnull")
  def isNotNull: Column = fn0("isnotnull")
  def isNaN: Column = fn0("isnan")

  // ---------------------------------------------------------------------------
  // String operators
  // ---------------------------------------------------------------------------

  def contains(other: Column): Column = fn("contains", other)
  def contains(literal: String): Column = contains(Column.lit(literal))

  def startsWith(other: Column): Column = fn("startswith", other)
  def startsWith(literal: String): Column = startsWith(Column.lit(literal))

  def endsWith(other: Column): Column = fn("endswith", other)
  def endsWith(literal: String): Column = endsWith(Column.lit(literal))

  def like(literal: String): Column = fn("like", Column.lit(literal))
  def rlike(literal: String): Column = fn("rlike", Column.lit(literal))

  /** Case-insensitive LIKE. */
  def ilike(literal: String): Column = fn("ilike", Column.lit(literal))

  def isin(values: Any*): Column =
    val args = values.map(v => Column.lit(v).expr)
    val ufBuilder = Expression.UnresolvedFunction.newBuilder()
      .setFunctionName("in")
      .addArguments(expr)
    args.foreach(ufBuilder.addArguments)
    withExpr(Expression.newBuilder().setUnresolvedFunction(ufBuilder.build()).build())

  def between(lower: Any, upper: Any): Column =
    this >= lower && this <= upper

  def substr(startPos: Int, length: Int): Column =
    fn("substring", Column.lit(startPos), Column.lit(length))

  def substr(startPos: Column, len: Column): Column =
    fn("substring", startPos, len)

  /** Scala collection variant of isin. */
  def isInCollection(values: Iterable[?]): Column = isin(values.toSeq*)

  /** IN subquery: `col IN (SELECT ... FROM ...)`. */
  def isin(ds: Dataset[?]): Column =
    val rel = ds.df.relation
    val planId = rel.getCommon.getPlanId
    val subExpr = Expression.newBuilder()
      .setSubqueryExpression(
        SubqueryExpression.newBuilder()
          .setPlanId(planId)
          .setSubqueryType(SubqueryExpression.SubqueryType.SUBQUERY_TYPE_IN)
          .addAllInSubqueryValues(java.util.List.of(expr))
          .build()
      )
      .build()
    Column(subExpr, this.subqueryRelations :+ rel)

  /** IN subquery (DataFrame variant): `col IN (SELECT ... FROM ...)`. */
  def isin(df: DataFrame): Column =
    val rel = df.relation
    val planId = rel.getCommon.getPlanId
    val subExpr = Expression.newBuilder()
      .setSubqueryExpression(
        SubqueryExpression.newBuilder()
          .setPlanId(planId)
          .setSubqueryType(SubqueryExpression.SubqueryType.SUBQUERY_TYPE_IN)
          .addAllInSubqueryValues(java.util.List.of(expr))
          .build()
      )
      .build()
    Column(subExpr, this.subqueryRelations :+ rel)

  // ---------------------------------------------------------------------------
  // when / otherwise (case-when chaining)
  // ---------------------------------------------------------------------------

  def when(condition: Column, value: Any): Column =
    val v = value match
      case c: Column => c
      case other     => Column.lit(other)
    if expr.hasUnresolvedFunction && expr.getUnresolvedFunction.getFunctionName == "when" then
      val uf = expr.getUnresolvedFunction
      val newUf = uf.toBuilder()
        .addArguments(condition.expr)
        .addArguments(v.expr)
        .build()
      Column(
        Expression.newBuilder().setUnresolvedFunction(newUf).build(),
        this.subqueryRelations ++ condition.subqueryRelations ++ v.subqueryRelations
      )
    else
      throw IllegalArgumentException(
        "when() can only be applied on a Column previously generated by when()"
      )

  def otherwise(value: Any): Column =
    val v = value match
      case c: Column => c
      case other     => Column.lit(other)
    if expr.hasUnresolvedFunction && expr.getUnresolvedFunction.getFunctionName == "when" then
      val uf = expr.getUnresolvedFunction
      val newUf = uf.toBuilder()
        .addArguments(v.expr)
        .build()
      Column(
        Expression.newBuilder().setUnresolvedFunction(newUf).build(),
        this.subqueryRelations ++ v.subqueryRelations
      )
    else
      throw IllegalArgumentException(
        "otherwise() can only be applied on a Column previously generated by when()"
      )

  // ---------------------------------------------------------------------------
  // Nested data access
  // ---------------------------------------------------------------------------

  /** Extract value by key (for MapType) or index (for ArrayType). */
  def getItem(key: Any): Column =
    val keyExpr = Column.lit(key).expr
    withExpr(Expression.newBuilder().setUnresolvedExtractValue(
      Expression.UnresolvedExtractValue.newBuilder()
        .setChild(expr)
        .setExtraction(keyExpr)
        .build()
    ).build())

  /** Extract a field from a StructType column by name. */
  def getField(fieldName: String): Column =
    val nameExpr = Column.lit(fieldName).expr
    withExpr(Expression.newBuilder().setUnresolvedExtractValue(
      Expression.UnresolvedExtractValue.newBuilder()
        .setChild(expr)
        .setExtraction(nameExpr)
        .build()
    ).build())

  /** Subscript operator — same as getItem. */
  def apply(key: Any): Column = getItem(key)

  /** Add or replace a field in a StructType column. */
  def withField(fieldName: String, col: Column): Column =
    withExprMerge(
      Expression.newBuilder().setUpdateFields(
        Expression.UpdateFields.newBuilder()
          .setStructExpression(expr)
          .setFieldName(fieldName)
          .setValueExpression(col.expr)
          .build()
      ).build(),
      col
    )

  /** Drop fields from a StructType column by name. */
  def dropFields(fieldNames: String*): Column =
    fieldNames.foldLeft(this) { (acc, name) =>
      acc.withExpr(Expression.newBuilder().setUpdateFields(
        Expression.UpdateFields.newBuilder()
          .setStructExpression(acc.expr)
          .setFieldName(name)
          .build()
      ).build())
    }

  // ---------------------------------------------------------------------------
  // Cast / Alias
  // ---------------------------------------------------------------------------

  def cast(to: String): Column =
    withExpr(Expression.newBuilder().setCast(
      Expression.Cast.newBuilder()
        .setExpr(expr)
        .setTypeStr(to)
        .build()
    ).build())

  /** Cast to the specified DataType. */
  def cast(to: types.DataType): Column =
    withExpr(Expression.newBuilder().setCast(
      Expression.Cast.newBuilder()
        .setExpr(expr)
        .setType(DataTypeProtoConverter.toProto(to))
        .build()
    ).build())

  /** Try-cast to the specified type string; returns null on failure. */
  def try_cast(to: String): Column =
    withExpr(Expression.newBuilder().setCast(
      Expression.Cast.newBuilder()
        .setExpr(expr)
        .setTypeStr(to)
        .setEvalMode(Expression.Cast.EvalMode.EVAL_MODE_TRY)
        .build()
    ).build())

  /** Try-cast to the specified DataType; returns null on failure. */
  def try_cast(to: types.DataType): Column =
    withExpr(Expression.newBuilder().setCast(
      Expression.Cast.newBuilder()
        .setExpr(expr)
        .setType(DataTypeProtoConverter.toProto(to))
        .setEvalMode(Expression.Cast.EvalMode.EVAL_MODE_TRY)
        .build()
    ).build())

  def alias(name: String): Column = as(name)

  def as(name: String): Column =
    withExpr(Expression.newBuilder().setAlias(
      Expression.Alias.newBuilder()
        .setExpr(expr)
        .addName(name)
        .build()
    ).build())

  def name(n: String): Column = as(n)

  /** Assign multiple aliases (for tuple/struct columns). */
  def as(aliases: Seq[String]): Column =
    val aliasBuilder = Expression.Alias.newBuilder().setExpr(expr)
    aliases.foreach(aliasBuilder.addName)
    withExpr(Expression.newBuilder().setAlias(aliasBuilder.build()).build())

  /** Assign an alias with metadata (JSON string). */
  def as(alias: String, metadata: String): Column =
    withExpr(Expression.newBuilder().setAlias(
      Expression.Alias.newBuilder()
        .setExpr(expr)
        .addName(alias)
        .setMetadata(metadata)
        .build()
    ).build())

  /** Provide a type hint to generate a TypedColumn for Dataset operations. */
  def as[U: Encoder]: TypedColumn[Any, U] =
    TypedColumn(expr, summon[Encoder[U]], subqueryRelations)

  // ---------------------------------------------------------------------------
  // Sort
  // ---------------------------------------------------------------------------

  def asc: Column = withSortDirection(ascending = true)
  def asc_nulls_first: Column =
    withSortDirection(ascending = true, nullsFirst = true)
  def asc_nulls_last: Column =
    withSortDirection(ascending = true, nullsFirst = false)
  def desc: Column = withSortDirection(ascending = false)
  def desc_nulls_first: Column =
    withSortDirection(ascending = false, nullsFirst = true)
  def desc_nulls_last: Column =
    withSortDirection(ascending = false, nullsFirst = false)

  /** Convert this column into a SortOrder proto for orderBy. */
  private[sql] def toSortOrder: Expression.SortOrder =
    if expr.hasSortOrder then expr.getSortOrder
    else
      Expression.SortOrder.newBuilder()
        .setChild(expr)
        .setDirection(Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING)
        .setNullOrdering(Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST)
        .build()

  private def withSortDirection(ascending: Boolean): Column =
    withSortDirection(ascending, nullsFirst = ascending)

  private def withSortDirection(ascending: Boolean, nullsFirst: Boolean): Column =
    import Expression.SortOrder.SortDirection.*
    import Expression.SortOrder.NullOrdering.*
    withExpr(Expression.newBuilder().setSortOrder(
      Expression.SortOrder.newBuilder()
        .setChild(expr)
        .setDirection(
          if ascending then SORT_DIRECTION_ASCENDING
          else SORT_DIRECTION_DESCENDING
        )
        .setNullOrdering(
          if nullsFirst then SORT_NULLS_FIRST
          else SORT_NULLS_LAST
        )
        .build()
    ).build())

  // ---------------------------------------------------------------------------
  // Window
  // ---------------------------------------------------------------------------

  def over(window: WindowSpec): Column =
    val windowBuilder = Expression.Window.newBuilder()
      .setWindowFunction(expr)
    window.partitionExprs.foreach(windowBuilder.addPartitionSpec)
    window.orderExprs.foreach(c => windowBuilder.addOrderSpec(c.toSortOrder))
    window.frameSpec.foreach(windowBuilder.setFrameSpec)
    val windowRels = window.orderExprs.flatMap(_.subqueryRelations)
    Column(
      Expression.newBuilder().setWindow(windowBuilder.build()).build(),
      this.subqueryRelations ++ windowRels
    )

  /** Apply a window function with an empty window specification. */
  def over(): Column = over(WindowSpec(Seq.empty, Seq.empty))

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Binary function: fn(this, other) */
  private def fn(name: String, other: Column): Column =
    withExprMerge(
      Expression.newBuilder().setUnresolvedFunction(
        Expression.UnresolvedFunction.newBuilder()
          .setFunctionName(name)
          .addArguments(expr)
          .addArguments(other.expr)
          .build()
      ).build(),
      other
    )

  /** Ternary function: fn(this, a, b) */
  private def fn(name: String, a: Column, b: Column): Column =
    Column(
      Expression.newBuilder().setUnresolvedFunction(
        Expression.UnresolvedFunction.newBuilder()
          .setFunctionName(name)
          .addArguments(expr)
          .addArguments(a.expr)
          .addArguments(b.expr)
          .build()
      ).build(),
      this.subqueryRelations ++ a.subqueryRelations ++ b.subqueryRelations
    )

  /** Unary function: fn(this) */
  private def fn0(name: String): Column =
    withExpr(Expression.newBuilder().setUnresolvedFunction(
      Expression.UnresolvedFunction.newBuilder()
        .setFunctionName(name)
        .addArguments(expr)
        .build()
    ).build())

  override def toString: String = expr.toString

object Column:

  def apply(name: String): Column = new Column(name)

  private[sql] def apply(expr: Expression): Column = new Column(expr)

  private[sql] def apply(expr: Expression, subqueryRelations: Seq[Relation]): Column =
    new Column(expr, subqueryRelations)

  /** Create a literal Column. */
  def lit(value: Any): Column =
    val literal = value match
      case null => Expression.Literal.newBuilder()
          .setNull(ProtoDataType.newBuilder()
            .setNull(ProtoDataType.NULL.getDefaultInstance).build())
          .build()
      case v: Boolean => Expression.Literal.newBuilder().setBoolean(v).build()
      case v: Byte    => Expression.Literal.newBuilder().setByte(v.toInt).build()
      case v: Short   => Expression.Literal.newBuilder().setShort(v.toInt).build()
      case v: Int     => Expression.Literal.newBuilder().setInteger(v).build()
      case v: Long    => Expression.Literal.newBuilder().setLong(v).build()
      case v: Float   => Expression.Literal.newBuilder().setFloat(v).build()
      case v: Double  => Expression.Literal.newBuilder().setDouble(v).build()
      case v: String  => Expression.Literal.newBuilder().setString(v).build()
      case v: Column  => return v // pass through
      case v          => Expression.Literal.newBuilder().setString(v.toString).build()

    Column(Expression.newBuilder().setLiteral(literal).build())

/** WindowSpec with partition, order, and frame specifications. */
final class WindowSpec private[sql] (
    private[sql] val partitionExprs: Seq[Expression],
    private[sql] val orderExprs: Seq[Column],
    private[sql] val frameSpec: Option[Expression.Window.WindowFrame] = None
):
  def partitionBy(cols: Column*): WindowSpec =
    WindowSpec(cols.map(_.expr), orderExprs, frameSpec)

  def orderBy(cols: Column*): WindowSpec =
    WindowSpec(partitionExprs, cols.toSeq, frameSpec)

  def rowsBetween(start: Long, end: Long): WindowSpec =
    WindowSpec(
      partitionExprs,
      orderExprs,
      Some(
        Expression.Window.WindowFrame.newBuilder()
          .setFrameType(Expression.Window.WindowFrame.FrameType.FRAME_TYPE_ROW)
          .setLower(Window.toBoundary(start))
          .setUpper(Window.toBoundary(end))
          .build()
      )
    )

  def rangeBetween(start: Long, end: Long): WindowSpec =
    WindowSpec(
      partitionExprs,
      orderExprs,
      Some(
        Expression.Window.WindowFrame.newBuilder()
          .setFrameType(Expression.Window.WindowFrame.FrameType.FRAME_TYPE_RANGE)
          .setLower(Window.toBoundary(start))
          .setUpper(Window.toBoundary(end))
          .build()
      )
    )

object Window:
  /** Represents the value of `unboundedPreceding` for frame boundaries. */
  val unboundedPreceding: Long = Long.MinValue

  /** Represents the value of `unboundedFollowing` for frame boundaries. */
  val unboundedFollowing: Long = Long.MaxValue

  /** Represents the value of `currentRow` for frame boundaries. */
  val currentRow: Long = 0L

  def partitionBy(cols: Column*): WindowSpec =
    WindowSpec(cols.map(_.expr), Seq.empty)

  def orderBy(cols: Column*): WindowSpec =
    WindowSpec(Seq.empty, cols.toSeq)

  def rowsBetween(start: Long, end: Long): WindowSpec =
    WindowSpec(Seq.empty, Seq.empty).rowsBetween(start, end)

  def rangeBetween(start: Long, end: Long): WindowSpec =
    WindowSpec(Seq.empty, Seq.empty).rangeBetween(start, end)

  private[sql] def toBoundary(value: Long): Expression.Window.WindowFrame.FrameBoundary =
    if value == Long.MinValue then
      Expression.Window.WindowFrame.FrameBoundary.newBuilder()
        .setUnbounded(true).build()
    else if value == Long.MaxValue then
      Expression.Window.WindowFrame.FrameBoundary.newBuilder()
        .setUnbounded(true).build()
    else if value == 0L then
      Expression.Window.WindowFrame.FrameBoundary.newBuilder()
        .setCurrentRow(true).build()
    else
      val litExpr = Column.lit(value).expr
      Expression.Window.WindowFrame.FrameBoundary.newBuilder()
        .setValue(litExpr).build()
