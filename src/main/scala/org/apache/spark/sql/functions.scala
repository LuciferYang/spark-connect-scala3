package org.apache.spark.sql

import org.apache.spark.connect.proto.expressions.Expression
import org.apache.spark.connect.proto.expressions.Expression.ExprType

/** Built-in Spark SQL functions. */
object functions:

  // ---------------------------------------------------------------------------
  // Column references
  // ---------------------------------------------------------------------------

  def col(name: String): Column = Column(name)
  def column(name: String): Column = Column(name)

  def lit(value: Any): Column = Column.lit(value)

  def expr(sqlExpr: String): Column =
    Column(Expression(exprType = ExprType.ExpressionString(
      Expression.ExpressionString(expression = sqlExpr)
    )))

  // ---------------------------------------------------------------------------
  // Aggregate functions
  // ---------------------------------------------------------------------------

  def count(col: Column): Column = callFn("count", col)
  def count(colName: String): Column = count(Column(colName))
  def sum(col: Column): Column = callFn("sum", col)
  def sum(colName: String): Column = sum(Column(colName))
  def avg(col: Column): Column = callFn("avg", col)
  def avg(colName: String): Column = avg(Column(colName))
  def mean(col: Column): Column = avg(col)
  def min(col: Column): Column = callFn("min", col)
  def min(colName: String): Column = min(Column(colName))
  def max(col: Column): Column = callFn("max", col)
  def max(colName: String): Column = max(Column(colName))
  def first(col: Column): Column = callFn("first", col)
  def last(col: Column): Column = callFn("last", col)
  def countDistinct(col: Column, cols: Column*): Column =
    callFn("count", isDistinct = true, (col +: cols)*)
  def collect_list(col: Column): Column = callFn("collect_list", col)
  def collect_set(col: Column): Column = callFn("collect_set", col)

  // ---------------------------------------------------------------------------
  // Math functions
  // ---------------------------------------------------------------------------

  def abs(col: Column): Column    = callFn("abs", col)
  def sqrt(col: Column): Column   = callFn("sqrt", col)
  def pow(l: Column, r: Column): Column = callFn("power", l, r)
  def round(col: Column, scale: Int = 0): Column = callFn("round", col, Column.lit(scale))
  def floor(col: Column): Column  = callFn("floor", col)
  def ceil(col: Column): Column   = callFn("ceil", col)
  def log(col: Column): Column    = callFn("ln", col)
  def log10(col: Column): Column  = callFn("log10", col)
  def log2(col: Column): Column   = callFn("log2", col)
  def exp(col: Column): Column    = callFn("exp", col)
  def greatest(cols: Column*): Column = callFn("greatest", cols*)
  def least(cols: Column*): Column = callFn("least", cols*)
  def rand(seed: Long = 0L): Column = callFn("rand", Column.lit(seed))
  def randn(seed: Long = 0L): Column = callFn("randn", Column.lit(seed))

  // ---------------------------------------------------------------------------
  // String functions
  // ---------------------------------------------------------------------------

  def concat(cols: Column*): Column   = callFn("concat", cols*)
  def concat_ws(sep: String, cols: Column*): Column =
    callFn("concat_ws", (Column.lit(sep) +: cols)*)
  def upper(col: Column): Column      = callFn("upper", col)
  def lower(col: Column): Column      = callFn("lower", col)
  def trim(col: Column): Column       = callFn("trim", col)
  def ltrim(col: Column): Column      = callFn("ltrim", col)
  def rtrim(col: Column): Column      = callFn("rtrim", col)
  def substring(col: Column, pos: Int, len: Int): Column =
    callFn("substring", col, Column.lit(pos), Column.lit(len))
  def length(col: Column): Column     = callFn("length", col)
  def replace(col: Column, search: Column, replacement: Column): Column =
    callFn("replace", col, search, replacement)
  def lpad(col: Column, len: Int, pad: String): Column =
    callFn("lpad", col, Column.lit(len), Column.lit(pad))
  def rpad(col: Column, len: Int, pad: String): Column =
    callFn("rpad", col, Column.lit(len), Column.lit(pad))

  // ---------------------------------------------------------------------------
  // Date / Time functions
  // ---------------------------------------------------------------------------

  def current_date(): Column      = callFn("current_date")
  def current_timestamp(): Column = callFn("current_timestamp")
  def year(col: Column): Column   = callFn("year", col)
  def month(col: Column): Column  = callFn("month", col)
  def dayofmonth(col: Column): Column = callFn("dayofmonth", col)
  def hour(col: Column): Column   = callFn("hour", col)
  def minute(col: Column): Column = callFn("minute", col)
  def second(col: Column): Column = callFn("second", col)
  def date_add(start: Column, days: Int): Column =
    callFn("date_add", start, Column.lit(days))
  def date_sub(start: Column, days: Int): Column =
    callFn("date_sub", start, Column.lit(days))
  def to_date(col: Column): Column = callFn("to_date", col)
  def to_date(col: Column, fmt: String): Column = callFn("to_date", col, Column.lit(fmt))
  def to_timestamp(col: Column): Column = callFn("to_timestamp", col)
  def to_timestamp(col: Column, fmt: String): Column = callFn("to_timestamp", col, Column.lit(fmt))
  def date_format(col: Column, fmt: String): Column =
    callFn("date_format", col, Column.lit(fmt))

  // ---------------------------------------------------------------------------
  // Null handling
  // ---------------------------------------------------------------------------

  def coalesce(cols: Column*): Column = callFn("coalesce", cols*)

  // ---------------------------------------------------------------------------
  // Conditional
  // ---------------------------------------------------------------------------

  def when(condition: Column, value: Any): Column =
    val v = value match
      case c: Column => c
      case other => Column.lit(other)
    callFn("when", condition, v)

  // ---------------------------------------------------------------------------
  // Collection functions
  // ---------------------------------------------------------------------------

  def array(cols: Column*): Column     = callFn("array", cols*)
  def struct(cols: Column*): Column    = callFn("struct", cols*)
  def explode(col: Column): Column     = callFn("explode", col)
  def size(col: Column): Column        = callFn("size", col)
  def array_contains(col: Column, value: Any): Column =
    callFn("array_contains", col, Column.lit(value))

  // ---------------------------------------------------------------------------
  // Window functions
  // ---------------------------------------------------------------------------

  def row_number(): Column  = callFn("row_number")
  def rank(): Column        = callFn("rank")
  def dense_rank(): Column  = callFn("dense_rank")
  def lead(col: Column, offset: Int = 1): Column =
    callFn("lead", col, Column.lit(offset))
  def lag(col: Column, offset: Int = 1): Column =
    callFn("lag", col, Column.lit(offset))
  def ntile(n: Int): Column = callFn("ntile", Column.lit(n))

  // ---------------------------------------------------------------------------
  // Helper
  // ---------------------------------------------------------------------------

  private def callFn(name: String, cols: Column*): Column =
    callFn(name, isDistinct = false, cols*)

  private def callFn(name: String, isDistinct: Boolean, cols: Column*): Column =
    Column(Expression(exprType = ExprType.UnresolvedFunction(
      Expression.UnresolvedFunction(
        functionName = name,
        arguments = cols.map(_.expr).toSeq,
        isDistinct = isDistinct
      )
    )))
