package org.apache.spark.sql

import org.apache.spark.connect.proto.expressions.Expression
import org.apache.spark.connect.proto.expressions.Expression.ExprType

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
    Expression(exprType = ExprType.UnresolvedAttribute(
      Expression.UnresolvedAttribute(unparsedIdentifier = name)
    ))
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
    Column(Expression(exprType = ExprType.UnresolvedFunction(
      Expression.UnresolvedFunction(
        functionName = "in",
        arguments = expr +: args.toSeq
      )
    )))

  def between(lower: Any, upper: Any): Column =
    this >= lower && this <= upper

  def substr(startPos: Int, length: Int): Column =
    fn("substring", Column.lit(startPos), Column.lit(length))

  // ---------------------------------------------------------------------------
  // Cast / Alias
  // ---------------------------------------------------------------------------

  def cast(to: String): Column =
    Column(Expression(exprType = ExprType.Cast(
      Expression.Cast(
        expr = Some(expr),
        castToType = Expression.Cast.CastToType.TypeStr(to)
      )
    )))

  def alias(name: String): Column = as(name)

  def as(name: String): Column =
    Column(Expression(exprType = ExprType.Alias(
      Expression.Alias(expr = Some(expr), name = Seq(name))
    )))

  def name(n: String): Column = as(n)

  // ---------------------------------------------------------------------------
  // Sort
  // ---------------------------------------------------------------------------

  def asc: Column  = withSortDirection(ascending = true)
  def desc: Column = withSortDirection(ascending = false)

  /** Convert this column into a SortOrder proto for orderBy. */
  private[sql] def toSortOrder: Expression.SortOrder =
    expr.exprType match
      case ExprType.SortOrder(so) => so
      case _ =>
        Expression.SortOrder(
          child = Some(expr),
          direction = Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING,
          nullOrdering = Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST
        )

  private def withSortDirection(ascending: Boolean): Column =
    import Expression.SortOrder.SortDirection.*
    import Expression.SortOrder.NullOrdering.*
    Column(Expression(exprType = ExprType.SortOrder(
      Expression.SortOrder(
        child = Some(expr),
        direction =
          if ascending then SORT_DIRECTION_ASCENDING
          else SORT_DIRECTION_DESCENDING,
        nullOrdering =
          if ascending then SORT_NULLS_FIRST
          else SORT_NULLS_LAST
      )
    )))

  // ---------------------------------------------------------------------------
  // Window
  // ---------------------------------------------------------------------------

  def over(window: WindowSpec): Column =
    Column(Expression(exprType = ExprType.Window(
      Expression.Window(
        windowFunction = Some(expr),
        partitionSpec = window.partitionExprs,
        orderSpec = window.orderExprs.map(_.toSortOrder),
        frameSpec = None
      )
    )))

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Binary function: fn(this, other) */
  private def fn(name: String, other: Column): Column =
    Column(Expression(exprType = ExprType.UnresolvedFunction(
      Expression.UnresolvedFunction(
        functionName = name,
        arguments = Seq(expr, other.expr)
      )
    )))

  /** Ternary function: fn(this, a, b) */
  private def fn(name: String, a: Column, b: Column): Column =
    Column(Expression(exprType = ExprType.UnresolvedFunction(
      Expression.UnresolvedFunction(
        functionName = name,
        arguments = Seq(expr, a.expr, b.expr)
      )
    )))

  /** Unary function: fn(this) */
  private def fn0(name: String): Column =
    Column(Expression(exprType = ExprType.UnresolvedFunction(
      Expression.UnresolvedFunction(
        functionName = name,
        arguments = Seq(expr)
      )
    )))

  override def toString: String = expr.toString

object Column:

  def apply(name: String): Column = new Column(name)

  private[sql] def apply(expr: Expression): Column = new Column(expr)

  /** Create a literal Column. */
  def lit(value: Any): Column =
    val literal = value match
      case null          => Expression.Literal(literalType = Expression.Literal.LiteralType.Null(
                              org.apache.spark.connect.proto.types.DataType(
                                kind = org.apache.spark.connect.proto.types.DataType.Kind.Null(
                                  org.apache.spark.connect.proto.types.DataType.NULL()
                                )
                              )
                            ))
      case v: Boolean    => Expression.Literal(literalType = Expression.Literal.LiteralType.Boolean(v))
      case v: Byte       => Expression.Literal(literalType = Expression.Literal.LiteralType.Byte(v.toInt))
      case v: Short      => Expression.Literal(literalType = Expression.Literal.LiteralType.Short(v.toInt))
      case v: Int        => Expression.Literal(literalType = Expression.Literal.LiteralType.Integer(v))
      case v: Long       => Expression.Literal(literalType = Expression.Literal.LiteralType.Long(v))
      case v: Float      => Expression.Literal(literalType = Expression.Literal.LiteralType.Float(v))
      case v: Double     => Expression.Literal(literalType = Expression.Literal.LiteralType.Double(v))
      case v: String     => Expression.Literal(literalType = Expression.Literal.LiteralType.String(v))
      case v: Column     => return v // pass through
      case v             => Expression.Literal(literalType = Expression.Literal.LiteralType.String(v.toString))

    Column(Expression(exprType = ExprType.Literal(literal)))

/** Minimal WindowSpec for Column.over(). */
final class WindowSpec private[sql] (
    private[sql] val partitionExprs: Seq[Expression],
    private[sql] val orderExprs: Seq[Column]
):
  def partitionBy(cols: Column*): WindowSpec =
    WindowSpec(cols.map(_.expr), orderExprs)

  def orderBy(cols: Column*): WindowSpec =
    WindowSpec(partitionExprs, cols.toSeq)

object Window:
  def partitionBy(cols: Column*): WindowSpec =
    WindowSpec(cols.map(_.expr), Seq.empty)

  def orderBy(cols: Column*): WindowSpec =
    WindowSpec(Seq.empty, cols.toSeq)
