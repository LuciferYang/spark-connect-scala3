package org.apache.spark.sql

import org.apache.spark.connect.proto.Expression

/** Built-in Spark SQL functions. */
object functions:

  // ---------------------------------------------------------------------------
  // Column references
  // ---------------------------------------------------------------------------

  def col(name: String): Column = Column(name)
  def column(name: String): Column = Column(name)

  def lit(value: Any): Column = Column.lit(value)

  def expr(sqlExpr: String): Column =
    Column(Expression.newBuilder()
      .setExpressionString(Expression.ExpressionString.newBuilder()
        .setExpression(sqlExpr).build())
      .build())

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
  def explode_outer(col: Column): Column = callFn("explode_outer", col)
  def posexplode(col: Column): Column  = callFn("posexplode", col)
  def posexplode_outer(col: Column): Column = callFn("posexplode_outer", col)
  def size(col: Column): Column        = callFn("size", col)
  def array_contains(col: Column, value: Any): Column =
    callFn("array_contains", col, Column.lit(value))
  def array_sort(col: Column): Column  = callFn("array_sort", col)
  def array_distinct(col: Column): Column = callFn("array_distinct", col)
  def array_intersect(col1: Column, col2: Column): Column = callFn("array_intersect", col1, col2)
  def array_union(col1: Column, col2: Column): Column = callFn("array_union", col1, col2)
  def array_except(col1: Column, col2: Column): Column = callFn("array_except", col1, col2)
  def array_join(col: Column, delimiter: String): Column =
    callFn("array_join", col, Column.lit(delimiter))
  def array_join(col: Column, delimiter: String, nullReplacement: String): Column =
    callFn("array_join", col, Column.lit(delimiter), Column.lit(nullReplacement))
  def array_max(col: Column): Column   = callFn("array_max", col)
  def array_min(col: Column): Column   = callFn("array_min", col)
  def array_position(col: Column, value: Any): Column =
    callFn("array_position", col, Column.lit(value))
  def array_remove(col: Column, element: Any): Column =
    callFn("array_remove", col, Column.lit(element))
  def array_repeat(col: Column, count: Int): Column =
    callFn("array_repeat", col, Column.lit(count))
  def arrays_zip(cols: Column*): Column = callFn("arrays_zip", cols*)
  def flatten(col: Column): Column     = callFn("flatten", col)
  def element_at(col: Column, extraction: Any): Column =
    callFn("element_at", col, Column.lit(extraction))
  def slice(col: Column, start: Int, length: Int): Column =
    callFn("slice", col, Column.lit(start), Column.lit(length))
  def reverse(col: Column): Column     = callFn("reverse", col)
  def shuffle(col: Column): Column     = callFn("shuffle", col)
  def sort_array(col: Column, asc: Boolean = true): Column =
    callFn("sort_array", col, Column.lit(asc))

  // Map functions
  def map(cols: Column*): Column       = callFn("map", cols*)
  def map_from_arrays(keys: Column, values: Column): Column =
    callFn("map_from_arrays", keys, values)
  def map_from_entries(col: Column): Column = callFn("map_from_entries", col)
  def map_keys(col: Column): Column    = callFn("map_keys", col)
  def map_values(col: Column): Column  = callFn("map_values", col)
  def map_entries(col: Column): Column = callFn("map_entries", col)
  def map_concat(cols: Column*): Column = callFn("map_concat", cols*)
  def map_filter(col: Column, f: Column): Column = callFn("map_filter", col, f)
  def map_zip_with(left: Column, right: Column, f: Column): Column =
    callFn("map_zip_with", left, right, f)

  // ---------------------------------------------------------------------------
  // JSON functions
  // ---------------------------------------------------------------------------

  def from_json(col: Column, schema: String): Column =
    callFn("from_json", col, Column.lit(schema))

  def to_json(col: Column): Column = callFn("to_json", col)

  def json_tuple(col: Column, fields: String*): Column =
    callFn("json_tuple", (col +: fields.map(Column.lit(_)))*)

  def get_json_object(col: Column, path: String): Column =
    callFn("get_json_object", col, Column.lit(path))

  def schema_of_json(json: String): Column =
    callFn("schema_of_json", Column.lit(json))

  // ---------------------------------------------------------------------------
  // Regex / String functions (extended)
  // ---------------------------------------------------------------------------

  def regexp_extract(col: Column, pattern: String, idx: Int): Column =
    callFn("regexp_extract", col, Column.lit(pattern), Column.lit(idx))

  def regexp_replace(col: Column, pattern: String, replacement: String): Column =
    callFn("regexp_replace", col, Column.lit(pattern), Column.lit(replacement))

  def regexp_replace(col: Column, pattern: Column, replacement: Column): Column =
    callFn("regexp_replace", col, pattern, replacement)

  def split(col: Column, pattern: String): Column =
    callFn("split", col, Column.lit(pattern))

  def split(col: Column, pattern: String, limit: Int): Column =
    callFn("split", col, Column.lit(pattern), Column.lit(limit))

  def initcap(col: Column): Column     = callFn("initcap", col)
  def soundex(col: Column): Column     = callFn("soundex", col)
  def levenshtein(l: Column, r: Column): Column = callFn("levenshtein", l, r)
  def ascii(col: Column): Column       = callFn("ascii", col)
  def base64(col: Column): Column      = callFn("base64", col)
  def unbase64(col: Column): Column    = callFn("unbase64", col)
  def decode(col: Column, charset: String): Column = callFn("decode", col, Column.lit(charset))
  def encode(col: Column, charset: String): Column = callFn("encode", col, Column.lit(charset))
  def format_number(col: Column, d: Int): Column = callFn("format_number", col, Column.lit(d))
  def format_string(format: String, args: Column*): Column =
    callFn("format_string", (Column.lit(format) +: args)*)
  def instr(str: Column, substring: String): Column =
    callFn("instr", str, Column.lit(substring))
  def locate(substr: String, str: Column, pos: Int = 1): Column =
    callFn("locate", Column.lit(substr), str, Column.lit(pos))
  def overlay(src: Column, replace: Column, pos: Column, len: Column): Column =
    callFn("overlay", src, replace, pos, len)
  def repeat(col: Column, n: Int): Column = callFn("repeat", col, Column.lit(n))
  def translate(col: Column, matchingString: String, replaceString: String): Column =
    callFn("translate", col, Column.lit(matchingString), Column.lit(replaceString))

  // ---------------------------------------------------------------------------
  // Date / Time functions (extended)
  // ---------------------------------------------------------------------------

  def dayofweek(col: Column): Column   = callFn("dayofweek", col)
  def dayofyear(col: Column): Column   = callFn("dayofyear", col)
  def weekofyear(col: Column): Column  = callFn("weekofyear", col)
  def quarter(col: Column): Column     = callFn("quarter", col)
  def last_day(col: Column): Column    = callFn("last_day", col)
  def next_day(col: Column, dayOfWeek: String): Column =
    callFn("next_day", col, Column.lit(dayOfWeek))
  def months_between(end: Column, start: Column): Column =
    callFn("months_between", end, start)
  def months_between(end: Column, start: Column, roundOff: Boolean): Column =
    callFn("months_between", end, start, Column.lit(roundOff))
  def datediff(end: Column, start: Column): Column = callFn("datediff", end, start)
  def add_months(start: Column, numMonths: Int): Column =
    callFn("add_months", start, Column.lit(numMonths))
  def from_unixtime(ut: Column): Column = callFn("from_unixtime", ut)
  def from_unixtime(ut: Column, fmt: String): Column =
    callFn("from_unixtime", ut, Column.lit(fmt))
  def unix_timestamp(): Column          = callFn("unix_timestamp")
  def unix_timestamp(col: Column): Column = callFn("unix_timestamp", col)
  def unix_timestamp(col: Column, fmt: String): Column =
    callFn("unix_timestamp", col, Column.lit(fmt))
  def from_utc_timestamp(ts: Column, tz: String): Column =
    callFn("from_utc_timestamp", ts, Column.lit(tz))
  def to_utc_timestamp(ts: Column, tz: String): Column =
    callFn("to_utc_timestamp", ts, Column.lit(tz))
  def window(timeColumn: Column, windowDuration: String): Column =
    callFn("window", timeColumn, Column.lit(windowDuration))
  def window(timeColumn: Column, windowDuration: String, slideDuration: String): Column =
    callFn("window", timeColumn, Column.lit(windowDuration), Column.lit(slideDuration))
  def date_trunc(format: String, timestamp: Column): Column =
    callFn("date_trunc", Column.lit(format), timestamp)
  def trunc(date: Column, format: String): Column =
    callFn("trunc", date, Column.lit(format))

  // ---------------------------------------------------------------------------
  // Math functions (extended)
  // ---------------------------------------------------------------------------

  def sin(col: Column): Column    = callFn("sin", col)
  def cos(col: Column): Column    = callFn("cos", col)
  def tan(col: Column): Column    = callFn("tan", col)
  def asin(col: Column): Column   = callFn("asin", col)
  def acos(col: Column): Column   = callFn("acos", col)
  def atan(col: Column): Column   = callFn("atan", col)
  def atan2(l: Column, r: Column): Column = callFn("atan2", l, r)
  def sinh(col: Column): Column   = callFn("sinh", col)
  def cosh(col: Column): Column   = callFn("cosh", col)
  def tanh(col: Column): Column   = callFn("tanh", col)
  def cbrt(col: Column): Column   = callFn("cbrt", col)
  def rint(col: Column): Column   = callFn("rint", col)
  def signum(col: Column): Column = callFn("signum", col)
  def degrees(col: Column): Column = callFn("degrees", col)
  def radians(col: Column): Column = callFn("radians", col)
  def bround(col: Column, scale: Int = 0): Column = callFn("bround", col, Column.lit(scale))
  def bin(col: Column): Column    = callFn("bin", col)
  def hex(col: Column): Column    = callFn("hex", col)
  def unhex(col: Column): Column  = callFn("unhex", col)
  def conv(col: Column, fromBase: Int, toBase: Int): Column =
    callFn("conv", col, Column.lit(fromBase), Column.lit(toBase))
  def factorial(col: Column): Column = callFn("factorial", col)
  def log(base: Double, col: Column): Column = callFn("log", Column.lit(base), col)

  // ---------------------------------------------------------------------------
  // Aggregate functions (extended)
  // ---------------------------------------------------------------------------

  def sumDistinct(col: Column): Column = callFn("sum", isDistinct = true, col)
  def approx_count_distinct(col: Column): Column = callFn("approx_count_distinct", col)
  def approx_count_distinct(col: Column, rsd: Double): Column =
    callFn("approx_count_distinct", col, Column.lit(rsd))
  def variance(col: Column): Column = callFn("variance", col)
  def var_pop(col: Column): Column  = callFn("var_pop", col)
  def var_samp(col: Column): Column = callFn("var_samp", col)
  def stddev(col: Column): Column   = callFn("stddev", col)
  def stddev_pop(col: Column): Column = callFn("stddev_pop", col)
  def stddev_samp(col: Column): Column = callFn("stddev_samp", col)
  def skewness(col: Column): Column = callFn("skewness", col)
  def kurtosis(col: Column): Column = callFn("kurtosis", col)
  def corr(col1: Column, col2: Column): Column = callFn("corr", col1, col2)
  def covar_pop(col1: Column, col2: Column): Column = callFn("covar_pop", col1, col2)
  def covar_samp(col1: Column, col2: Column): Column = callFn("covar_samp", col1, col2)
  def grouping(col: Column): Column = callFn("grouping", col)
  def grouping_id(cols: Column*): Column = callFn("grouping_id", cols*)
  def percentile_approx(col: Column, percentage: Column, accuracy: Column): Column =
    callFn("percentile_approx", col, percentage, accuracy)

  // ---------------------------------------------------------------------------
  // Misc functions
  // ---------------------------------------------------------------------------

  def monotonically_increasing_id(): Column = callFn("monotonically_increasing_id")
  def spark_partition_id(): Column          = callFn("spark_partition_id")
  def input_file_name(): Column             = callFn("input_file_name")
  def typedLit[T](value: T): Column        = Column.lit(value)
  def negate(col: Column): Column           = callFn("negative", col)
  def not(col: Column): Column              = callFn("not", col)
  def bitwiseNOT(col: Column): Column       = callFn("~", col)
  def hash(cols: Column*): Column           = callFn("hash", cols*)
  def xxhash64(cols: Column*): Column       = callFn("xxhash64", cols*)
  def md5(col: Column): Column              = callFn("md5", col)
  def sha1(col: Column): Column             = callFn("sha1", col)
  def sha2(col: Column, numBits: Int): Column = callFn("sha2", col, Column.lit(numBits))
  def crc32(col: Column): Column            = callFn("crc32", col)
  def nanvl(col1: Column, col2: Column): Column = callFn("nanvl", col1, col2)
  def ifnull(col1: Column, col2: Column): Column = callFn("ifnull", col1, col2)
  def nullif(col1: Column, col2: Column): Column = callFn("nullif", col1, col2)
  def nvl(col1: Column, col2: Column): Column = callFn("nvl", col1, col2)
  def nvl2(col1: Column, col2: Column, col3: Column): Column = callFn("nvl2", col1, col2, col3)

  /** Broadcast hint for a DataFrame — returns the same DataFrame with a broadcast hint. */
  def broadcast(df: DataFrame): DataFrame = df.hint("broadcast")

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
    val builder = Expression.UnresolvedFunction.newBuilder()
      .setFunctionName(name)
      .setIsDistinct(isDistinct)
    cols.foreach(c => builder.addArguments(c.expr))
    Column(Expression.newBuilder()
      .setUnresolvedFunction(builder.build())
      .build())

  // ---------------------------------------------------------------------------
  // User-Defined Functions (UDF)
  // ---------------------------------------------------------------------------

  /** Create a UDF from a Function1. */
  inline def udf[T1, R](f: T1 => R): UserDefinedFunction =
    UserDefinedFunction(
      func = f,
      returnType = Encoder.sparkTypeOf[R],
      inputTypes = Seq(Encoder.sparkTypeOf[T1])
    )

  /** Create a UDF from a Function2. */
  inline def udf[T1, T2, R](f: (T1, T2) => R): UserDefinedFunction =
    UserDefinedFunction(
      func = f,
      returnType = Encoder.sparkTypeOf[R],
      inputTypes = Seq(Encoder.sparkTypeOf[T1], Encoder.sparkTypeOf[T2])
    )

  /** Create a UDF from a Function3. */
  inline def udf[T1, T2, T3, R](f: (T1, T2, T3) => R): UserDefinedFunction =
    UserDefinedFunction(
      func = f,
      returnType = Encoder.sparkTypeOf[R],
      inputTypes = Seq(
        Encoder.sparkTypeOf[T1], Encoder.sparkTypeOf[T2], Encoder.sparkTypeOf[T3]
      )
    )

  /** Create a UDF from a Function4. */
  inline def udf[T1, T2, T3, T4, R](f: (T1, T2, T3, T4) => R): UserDefinedFunction =
    UserDefinedFunction(
      func = f,
      returnType = Encoder.sparkTypeOf[R],
      inputTypes = Seq(
        Encoder.sparkTypeOf[T1], Encoder.sparkTypeOf[T2],
        Encoder.sparkTypeOf[T3], Encoder.sparkTypeOf[T4]
      )
    )

  /** Create a UDF from a Function5. */
  inline def udf[T1, T2, T3, T4, T5, R](f: (T1, T2, T3, T4, T5) => R): UserDefinedFunction =
    UserDefinedFunction(
      func = f,
      returnType = Encoder.sparkTypeOf[R],
      inputTypes = Seq(
        Encoder.sparkTypeOf[T1], Encoder.sparkTypeOf[T2],
        Encoder.sparkTypeOf[T3], Encoder.sparkTypeOf[T4],
        Encoder.sparkTypeOf[T5]
      )
    )
