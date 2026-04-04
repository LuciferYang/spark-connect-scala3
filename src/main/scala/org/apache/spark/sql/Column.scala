package org.apache.spark.sql

import org.apache.spark.connect.proto.{Expression, DataType as ProtoDataType}

import scala.jdk.CollectionConverters.*

/**
 * A column expression in a DataFrame.
 *
 * Column objects are lazy — they build a protobuf Expression tree
 * that gets sent to the server only when an action is triggered.
 */
final class Column private[sql] (private[sql] val expr: Expression):

  // ---------------------------------------------------------------------------
  // Constructors
  // ---------------------------------------------------------------------------

  /** Reference a column by name. */
  def this(name: String) = this(
    Expression.newBuilder().setUnresolvedAttribute(
      Expression.UnresolvedAttribute.newBuilder().setUnparsedIdentifier(name).build()
    ).build()
  )

  // ---------------------------------------------------------------------------
  // Comparison operators
  // ---------------------------------------------------------------------------

  def ===(other: Column): Column = fn("==", other)
  def =!=(other: Column): Column = fn("!=", other)
  def >(other: Column): Column   = fn(">", other)
  def >=(other: Column): Column  = fn(">=", other)
  def <(other: Column): Column   = fn("<", other)
  def <=(other: Column): Column  = fn("<=", other)

  def ===(v: Any): Column = ===(Column.lit(v))
  def =!=(v: Any): Column = =!=(Column.lit(v))
  def >(v: Any): Column   = >(Column.lit(v))
  def >=(v: Any): Column  = >=(Column.lit(v))
  def <(v: Any): Column   = <(Column.lit(v))
  def <=(v: Any): Column  = <=(Column.lit(v))

  // ---------------------------------------------------------------------------
  // Logical operators
  // ---------------------------------------------------------------------------

  def &&(other: Column): Column = fn("and", other)
  def ||(other: Column): Column = fn("or", other)
  def unary_! : Column = fn0("not")

  // ---------------------------------------------------------------------------
  // Arithmetic operators
  // ---------------------------------------------------------------------------

  def +(other: Column): Column  = fn("+", other)
  def -(other: Column): Column  = fn("-", other)
  def *(other: Column): Column  = fn("*", other)
  def /(other: Column): Column  = fn("/", other)
  def %(other: Column): Column  = fn("%", other)
  def unary_- : Column = fn0("negative")

  def plus(v: Any): Column  = this + Column.lit(v)
  def minus(v: Any): Column = this - Column.lit(v)
  def multiply(v: Any): Column = this * Column.lit(v)
  def divide(v: Any): Column   = this / Column.lit(v)
  def mod(v: Any): Column     = this % Column.lit(v)

  // ---------------------------------------------------------------------------
  // Null / NaN checks
  // ---------------------------------------------------------------------------

  def isNull: Column    = fn0("isnull")
  def isNotNull: Column = fn0("isnotnull")
  def isNaN: Column     = fn0("isnan")

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

  def isin(values: Any*): Column =
    val args = values.map(v => Column.lit(v).expr)
    val ufBuilder = Expression.UnresolvedFunction.newBuilder()
      .setFunctionName("in")
      .addArguments(expr)
    args.foreach(ufBuilder.addArguments)
    Column(Expression.newBuilder().setUnresolvedFunction(ufBuilder.build()).build())

  def between(lower: Any, upper: Any): Column =
    this >= lower && this <= upper

  def substr(startPos: Int, length: Int): Column =
    fn("substring", Column.lit(startPos), Column.lit(length))

  // ---------------------------------------------------------------------------
  // when / otherwise (case-when chaining)
  // ---------------------------------------------------------------------------

  def when(condition: Column, value: Any): Column =
    val v = value match
      case c: Column => c
      case other => Column.lit(other)
    if expr.hasUnresolvedFunction && expr.getUnresolvedFunction.getFunctionName == "when" then
      val uf = expr.getUnresolvedFunction
      val newUf = uf.toBuilder()
        .addArguments(condition.expr)
        .addArguments(v.expr)
        .build()
      Column(Expression.newBuilder().setUnresolvedFunction(newUf).build())
    else
      throw IllegalArgumentException(
        "when() can only be applied on a Column previously generated by when()"
      )

  def otherwise(value: Any): Column =
    val v = value match
      case c: Column => c
      case other => Column.lit(other)
    if expr.hasUnresolvedFunction && expr.getUnresolvedFunction.getFunctionName == "when" then
      val uf = expr.getUnresolvedFunction
      val newUf = uf.toBuilder()
        .addArguments(v.expr)
        .build()
      Column(Expression.newBuilder().setUnresolvedFunction(newUf).build())
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
    Column(Expression.newBuilder().setUnresolvedExtractValue(
      Expression.UnresolvedExtractValue.newBuilder()
        .setChild(expr)
        .setExtraction(keyExpr)
        .build()
    ).build())

  /** Extract a field from a StructType column by name. */
  def getField(fieldName: String): Column =
    val nameExpr = Column.lit(fieldName).expr
    Column(Expression.newBuilder().setUnresolvedExtractValue(
      Expression.UnresolvedExtractValue.newBuilder()
        .setChild(expr)
        .setExtraction(nameExpr)
        .build()
    ).build())

  /** Subscript operator — same as getItem. */
  def apply(key: Any): Column = getItem(key)

  /** Add or replace a field in a StructType column. */
  def withField(fieldName: String, col: Column): Column =
    Column(Expression.newBuilder().setUpdateFields(
      Expression.UpdateFields.newBuilder()
        .setStructExpression(expr)
        .setFieldName(fieldName)
        .setValueExpression(col.expr)
        .build()
    ).build())

  /** Drop fields from a StructType column by name. */
  def dropFields(fieldNames: String*): Column =
    fieldNames.foldLeft(this) { (acc, name) =>
      Column(Expression.newBuilder().setUpdateFields(
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
    Column(Expression.newBuilder().setCast(
      Expression.Cast.newBuilder()
        .setExpr(expr)
        .setTypeStr(to)
        .build()
    ).build())

  def alias(name: String): Column = as(name)

  def as(name: String): Column =
    Column(Expression.newBuilder().setAlias(
      Expression.Alias.newBuilder()
        .setExpr(expr)
        .addName(name)
        .build()
    ).build())

  def name(n: String): Column = as(n)

  // ---------------------------------------------------------------------------
  // Sort
  // ---------------------------------------------------------------------------

  def asc: Column  = withSortDirection(ascending = true)
  def desc: Column = withSortDirection(ascending = false)

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
    import Expression.SortOrder.SortDirection.*
    import Expression.SortOrder.NullOrdering.*
    Column(Expression.newBuilder().setSortOrder(
      Expression.SortOrder.newBuilder()
        .setChild(expr)
        .setDirection(
          if ascending then SORT_DIRECTION_ASCENDING
          else SORT_DIRECTION_DESCENDING)
        .setNullOrdering(
          if ascending then SORT_NULLS_FIRST
          else SORT_NULLS_LAST)
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
    Column(Expression.newBuilder().setWindow(windowBuilder.build()).build())

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Binary function: fn(this, other) */
  private def fn(name: String, other: Column): Column =
    Column(Expression.newBuilder().setUnresolvedFunction(
      Expression.UnresolvedFunction.newBuilder()
        .setFunctionName(name)
        .addArguments(expr)
        .addArguments(other.expr)
        .build()
    ).build())

  /** Ternary function: fn(this, a, b) */
  private def fn(name: String, a: Column, b: Column): Column =
    Column(Expression.newBuilder().setUnresolvedFunction(
      Expression.UnresolvedFunction.newBuilder()
        .setFunctionName(name)
        .addArguments(expr)
        .addArguments(a.expr)
        .addArguments(b.expr)
        .build()
    ).build())

  /** Unary function: fn(this) */
  private def fn0(name: String): Column =
    Column(Expression.newBuilder().setUnresolvedFunction(
      Expression.UnresolvedFunction.newBuilder()
        .setFunctionName(name)
        .addArguments(expr)
        .build()
    ).build())

  override def toString: String = expr.toString

object Column:

  def apply(name: String): Column = new Column(name)

  private[sql] def apply(expr: Expression): Column = new Column(expr)

  /** Create a literal Column. */
  def lit(value: Any): Column =
    val literal = value match
      case null          => Expression.Literal.newBuilder()
                              .setNull(ProtoDataType.newBuilder()
                                .setNull(ProtoDataType.NULL.getDefaultInstance).build())
                              .build()
      case v: Boolean    => Expression.Literal.newBuilder().setBoolean(v).build()
      case v: Byte       => Expression.Literal.newBuilder().setByte(v.toInt).build()
      case v: Short      => Expression.Literal.newBuilder().setShort(v.toInt).build()
      case v: Int        => Expression.Literal.newBuilder().setInteger(v).build()
      case v: Long       => Expression.Literal.newBuilder().setLong(v).build()
      case v: Float      => Expression.Literal.newBuilder().setFloat(v).build()
      case v: Double     => Expression.Literal.newBuilder().setDouble(v).build()
      case v: String     => Expression.Literal.newBuilder().setString(v).build()
      case v: Column     => return v // pass through
      case v             => Expression.Literal.newBuilder().setString(v.toString).build()

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
    WindowSpec(partitionExprs, orderExprs, Some(
      Expression.Window.WindowFrame.newBuilder()
        .setFrameType(Expression.Window.WindowFrame.FrameType.FRAME_TYPE_ROW)
        .setLower(Window.toBoundary(start))
        .setUpper(Window.toBoundary(end))
        .build()
    ))

  def rangeBetween(start: Long, end: Long): WindowSpec =
    WindowSpec(partitionExprs, orderExprs, Some(
      Expression.Window.WindowFrame.newBuilder()
        .setFrameType(Expression.Window.WindowFrame.FrameType.FRAME_TYPE_RANGE)
        .setLower(Window.toBoundary(start))
        .setUpper(Window.toBoundary(end))
        .build()
    ))

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
