package org.apache.spark.sql

import org.apache.spark.connect.proto.Expression
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.expressions.Aggregator

import scala.annotation.targetName

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
  // Sort functions
  // ---------------------------------------------------------------------------

  def asc(colName: String): Column = col(colName).asc
  def asc_nulls_first(colName: String): Column = col(colName).asc_nulls_first
  def asc_nulls_last(colName: String): Column = col(colName).asc_nulls_last
  def desc(colName: String): Column = col(colName).desc
  def desc_nulls_first(colName: String): Column = col(colName).desc_nulls_first
  def desc_nulls_last(colName: String): Column = col(colName).desc_nulls_last

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
  def any_value(col: Column): Column = callFn("any_value", col)
  def count_if(col: Column): Column = callFn("count_if", col)
  def product(col: Column): Column = callFn("product", col)
  def every(col: Column): Column = callFn("every", col)
  def some(col: Column): Column = callFn("some", col)
  def bool_and(col: Column): Column = callFn("bool_and", col)
  def bool_or(col: Column): Column = callFn("bool_or", col)
  def bit_and(col: Column): Column = callFn("bit_and", col)
  def bit_or(col: Column): Column = callFn("bit_or", col)
  def bit_xor(col: Column): Column = callFn("bit_xor", col)
  def first_value(col: Column): Column = callFn("first_value", col)
  def last_value(col: Column): Column = callFn("last_value", col)
  def max_by(e: Column, ord: Column): Column = callFn("max_by", e, ord)
  def max_by(e: Column, ord: Column, k: Int): Column = callFn("max_by", e, ord, Column.lit(k))
  def max_by(e: Column, ord: Column, k: Column): Column = callFn("max_by", e, ord, k)
  def min_by(e: Column, ord: Column): Column = callFn("min_by", e, ord)
  def min_by(e: Column, ord: Column, k: Int): Column = callFn("min_by", e, ord, Column.lit(k))
  def min_by(e: Column, ord: Column, k: Column): Column = callFn("min_by", e, ord, k)
  def median(e: Column): Column = callFn("median", e)
  def mode(e: Column): Column = callFn("mode", e)
  def mode(e: Column, deterministic: Boolean): Column = callFn("mode", e, Column.lit(deterministic))
  def percentile(e: Column, percentage: Column): Column = callFn("percentile", e, percentage)
  def percentile(e: Column, percentage: Column, frequency: Column): Column =
    callFn("percentile", e, percentage, frequency)
  def any(e: Column): Column = callFn("any", e)
  def std(e: Column): Column = callFn("std", e)
  def sum_distinct(e: Column): Column = callFn("sum", isDistinct = true, e)
  def count_distinct(expr: Column, exprs: Column*): Column =
    callFn("count", isDistinct = true, (expr +: exprs)*)
  def listagg(e: Column): Column = callFn("listagg", e)
  def listagg(e: Column, delimiter: Column): Column = callFn("listagg", e, delimiter)
  def listagg_distinct(e: Column): Column = callFn("listagg_distinct", e)
  def listagg_distinct(e: Column, delimiter: Column): Column =
    callFn("listagg_distinct", e, delimiter)
  def string_agg(e: Column): Column = callFn("string_agg", e)
  def string_agg(e: Column, delimiter: Column): Column = callFn("string_agg", e, delimiter)
  def string_agg_distinct(e: Column): Column = callFn("string_agg_distinct", e)
  def string_agg_distinct(e: Column, delimiter: Column): Column =
    callFn("string_agg_distinct", e, delimiter)
  def histogram_numeric(e: Column, nBins: Column): Column =
    callFn("histogram_numeric", e, nBins)
  def count_min_sketch(e: Column, eps: Column, confidence: Column, seed: Column): Column =
    callFn("count_min_sketch", e, eps, confidence, seed)
  def count_min_sketch(e: Column, eps: Column, confidence: Column): Column =
    callFn("count_min_sketch", e, eps, confidence)
  def regr_avgx(y: Column, x: Column): Column = callFn("regr_avgx", y, x)
  def regr_avgy(y: Column, x: Column): Column = callFn("regr_avgy", y, x)
  def regr_count(y: Column, x: Column): Column = callFn("regr_count", y, x)
  def regr_intercept(y: Column, x: Column): Column = callFn("regr_intercept", y, x)
  def regr_r2(y: Column, x: Column): Column = callFn("regr_r2", y, x)
  def regr_slope(y: Column, x: Column): Column = callFn("regr_slope", y, x)
  def regr_sxx(y: Column, x: Column): Column = callFn("regr_sxx", y, x)
  def regr_sxy(y: Column, x: Column): Column = callFn("regr_sxy", y, x)
  def regr_syy(y: Column, x: Column): Column = callFn("regr_syy", y, x)
  def approx_percentile(e: Column, percentage: Column, accuracy: Column): Column =
    callFn("approx_percentile", e, percentage, accuracy)
  def bitmap_construct_agg(col: Column): Column = callFn("bitmap_construct_agg", col)
  def bitmap_or_agg(col: Column): Column = callFn("bitmap_or_agg", col)
  def bitmap_and_agg(col: Column): Column = callFn("bitmap_and_agg", col)
  def hll_sketch_agg(e: Column, lgConfigK: Column): Column =
    callFn("hll_sketch_agg", e, lgConfigK)
  def hll_sketch_agg(e: Column, lgConfigK: Int): Column =
    callFn("hll_sketch_agg", e, Column.lit(lgConfigK))
  def hll_sketch_agg(e: Column): Column = callFn("hll_sketch_agg", e)
  def hll_union_agg(e: Column, allowDifferentLgConfigK: Column): Column =
    callFn("hll_union_agg", e, allowDifferentLgConfigK)
  def hll_union_agg(e: Column, allowDifferentLgConfigK: Boolean): Column =
    callFn("hll_union_agg", e, Column.lit(allowDifferentLgConfigK))
  def hll_union_agg(e: Column): Column = callFn("hll_union_agg", e)

  // ---------------------------------------------------------------------------
  // Math functions
  // ---------------------------------------------------------------------------

  def abs(col: Column): Column = callFn("abs", col)
  def sqrt(col: Column): Column = callFn("sqrt", col)
  def pow(l: Column, r: Column): Column = callFn("power", l, r)
  def round(col: Column, scale: Int = 0): Column = callFn("round", col, Column.lit(scale))
  def floor(col: Column): Column = callFn("floor", col)
  def ceil(col: Column): Column = callFn("ceil", col)
  def log(col: Column): Column = callFn("ln", col)
  def log10(col: Column): Column = callFn("log10", col)
  def log2(col: Column): Column = callFn("log2", col)
  def exp(col: Column): Column = callFn("exp", col)
  def greatest(cols: Column*): Column = callFn("greatest", cols*)
  def least(cols: Column*): Column = callFn("least", cols*)
  def rand(seed: Long = 0L): Column = callFn("rand", Column.lit(seed))
  def randn(seed: Long = 0L): Column = callFn("randn", Column.lit(seed))
  def log1p(col: Column): Column = callFn("log1p", col)
  def expm1(col: Column): Column = callFn("expm1", col)
  def hypot(l: Column, r: Column): Column = callFn("hypot", l, r)
  def pmod(dividend: Column, divisor: Column): Column =
    callFn("pmod", dividend, divisor)
  def sign(col: Column): Column = callFn("sign", col)
  def e(): Column = callFn("e")
  def pi(): Column = callFn("pi")
  def width_bucket(v: Column, min: Column, max: Column, numBuckets: Column): Column =
    callFn("width_bucket", v, min, max, numBuckets)
  def ceiling(e: Column, scale: Column): Column = callFn("ceiling", e, scale)
  def ceiling(e: Column): Column = callFn("ceiling", e)
  def ln(e: Column): Column = callFn("ln", e)
  def power(l: Column, r: Column): Column = callFn("power", l, r)
  def negative(e: Column): Column = callFn("negative", e)
  def positive(e: Column): Column = callFn("positive", e)
  def try_mod(left: Column, right: Column): Column = callFn("try_mod", left, right)
  def random(seed: Column): Column = callFn("random", seed)
  def random(): Column = callFn("random")
  def uniform(min: Column, max: Column): Column = callFn("uniform", min, max)
  def uniform(min: Column, max: Column, seed: Column): Column =
    callFn("uniform", min, max, seed)

  // ---------------------------------------------------------------------------
  // String functions
  // ---------------------------------------------------------------------------

  def concat(cols: Column*): Column = callFn("concat", cols*)
  def concat_ws(sep: String, cols: Column*): Column =
    callFn("concat_ws", (Column.lit(sep) +: cols)*)
  def upper(col: Column): Column = callFn("upper", col)
  def lower(col: Column): Column = callFn("lower", col)
  def trim(col: Column): Column = callFn("trim", col)
  def ltrim(col: Column): Column = callFn("ltrim", col)
  def rtrim(col: Column): Column = callFn("rtrim", col)
  def substring(col: Column, pos: Int, len: Int): Column =
    callFn("substring", col, Column.lit(pos), Column.lit(len))
  def length(col: Column): Column = callFn("length", col)
  def replace(col: Column, search: Column, replacement: Column): Column =
    callFn("replace", col, search, replacement)
  def lpad(col: Column, len: Int, pad: String): Column =
    callFn("lpad", col, Column.lit(len), Column.lit(pad))
  def rpad(col: Column, len: Int, pad: String): Column =
    callFn("rpad", col, Column.lit(len), Column.lit(pad))
  def left(col: Column, len: Int): Column = callFn("left", col, Column.lit(len))
  def right(col: Column, len: Int): Column =
    callFn("right", col, Column.lit(len))
  def char_length(col: Column): Column = callFn("char_length", col)
  def bit_length(col: Column): Column = callFn("bit_length", col)
  def octet_length(col: Column): Column = callFn("octet_length", col)
  def contains(left: Column, right: Column): Column =
    callFn("contains", left, right)
  def startswith(col: Column, prefix: Column): Column =
    callFn("startswith", col, prefix)
  def endswith(col: Column, suffix: Column): Column =
    callFn("endswith", col, suffix)
  def btrim(col: Column): Column = callFn("btrim", col)
  def position(substr: Column, str: Column): Column =
    callFn("position", substr, str)
  def sentences(str: Column, lang: Column, country: Column): Column =
    callFn("sentences", str, lang, country)
  def char(n: Column): Column = callFn("char", n)
  def chr(n: Column): Column = callFn("chr", n)
  def character_length(str: Column): Column = callFn("character_length", str)
  def substr(str: Column, pos: Column, len: Column): Column =
    callFn("substr", str, pos, len)
  def substr(str: Column, pos: Column): Column = callFn("substr", str, pos)
  def substring_index(str: Column, delim: String, count: Int): Column =
    callFn("substring_index", str, Column.lit(delim), Column.lit(count))
  def split_part(str: Column, delimiter: Column, partNum: Column): Column =
    callFn("split_part", str, delimiter, partNum)
  def find_in_set(str: Column, strArray: Column): Column =
    callFn("find_in_set", str, strArray)
  def elt(inputs: Column*): Column = callFn("elt", inputs*)
  def regexp_count(str: Column, regexp: Column): Column =
    callFn("regexp_count", str, regexp)
  def regexp_extract_all(str: Column, regexp: Column): Column =
    callFn("regexp_extract_all", str, regexp)
  def regexp_extract_all(str: Column, regexp: Column, idx: Column): Column =
    callFn("regexp_extract_all", str, regexp, idx)
  def regexp_instr(str: Column, regexp: Column): Column =
    callFn("regexp_instr", str, regexp)
  def regexp_instr(str: Column, regexp: Column, idx: Column): Column =
    callFn("regexp_instr", str, regexp, idx)
  def regexp_substr(str: Column, regexp: Column): Column =
    callFn("regexp_substr", str, regexp)
  def regexp(str: Column, regexp: Column): Column = callFn("regexp", str, regexp)
  def regexp_like(str: Column, regexp: Column): Column =
    callFn("regexp_like", str, regexp)
  def rlike(str: Column, regexp: Column): Column = callFn("rlike", str, regexp)
  def like(str: Column, pattern: Column, escapeChar: Column): Column =
    callFn("like", str, pattern, escapeChar)
  def like(str: Column, pattern: Column): Column = callFn("like", str, pattern)
  def ilike(str: Column, pattern: Column, escapeChar: Column): Column =
    callFn("ilike", str, pattern, escapeChar)
  def ilike(str: Column, pattern: Column): Column = callFn("ilike", str, pattern)
  def collate(e: Column, collation: String): Column =
    callFn("collate", e, Column.lit(collation))
  def collation(e: Column): Column = callFn("collation", e)
  def to_binary(e: Column, f: Column): Column = callFn("to_binary", e, f)
  def to_binary(e: Column): Column = callFn("to_binary", e)
  def try_to_binary(e: Column, f: Column): Column = callFn("try_to_binary", e, f)
  def try_to_binary(e: Column): Column = callFn("try_to_binary", e)
  def to_char(e: Column, format: Column): Column = callFn("to_char", e, format)
  def to_varchar(e: Column, format: Column): Column = callFn("to_varchar", e, format)
  def to_number(e: Column, format: Column): Column = callFn("to_number", e, format)
  def mask(input: Column): Column = callFn("mask", input)
  def mask(input: Column, upperChar: Column): Column = callFn("mask", input, upperChar)
  def mask(input: Column, upperChar: Column, lowerChar: Column): Column =
    callFn("mask", input, upperChar, lowerChar)
  def mask(input: Column, upperChar: Column, lowerChar: Column, digitChar: Column): Column =
    callFn("mask", input, upperChar, lowerChar, digitChar)
  def mask(
      input: Column,
      upperChar: Column,
      lowerChar: Column,
      digitChar: Column,
      otherChar: Column
  ): Column =
    callFn("mask", input, upperChar, lowerChar, digitChar, otherChar)
  def randstr(length: Column): Column = callFn("randstr", length)
  def randstr(length: Column, seed: Column): Column = callFn("randstr", length, seed)
  def quote(str: Column): Column = callFn("quote", str)
  def printf(format: Column, arguments: Column*): Column =
    callFn("printf", (format +: arguments)*)
  def lcase(str: Column): Column = callFn("lcase", str)
  def ucase(str: Column): Column = callFn("ucase", str)
  def len(e: Column): Column = callFn("len", e)
  def is_valid_utf8(str: Column): Column = callFn("is_valid_utf8", str)
  def make_valid_utf8(str: Column): Column = callFn("make_valid_utf8", str)
  def validate_utf8(str: Column): Column = callFn("validate_utf8", str)
  def try_validate_utf8(str: Column): Column = callFn("try_validate_utf8", str)

  // ---------------------------------------------------------------------------
  // Date / Time functions
  // ---------------------------------------------------------------------------

  def current_date(): Column = callFn("current_date")
  def current_timestamp(): Column = callFn("current_timestamp")
  def year(col: Column): Column = callFn("year", col)
  def month(col: Column): Column = callFn("month", col)
  def dayofmonth(col: Column): Column = callFn("dayofmonth", col)
  def hour(col: Column): Column = callFn("hour", col)
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
  def curdate(): Column = callFn("curdate")
  def current_time(): Column = callFn("current_time")
  def localtimestamp(): Column = callFn("localtimestamp")
  def date_diff(end: Column, start: Column): Column = callFn("date_diff", end, start)
  def dateadd(start: Column, days: Column): Column = callFn("dateadd", start, days)
  def datepart(field: Column, source: Column): Column = callFn("datepart", field, source)
  def day(e: Column): Column = callFn("day", e)
  def dayname(timeExp: Column): Column = callFn("dayname", timeExp)
  def monthname(timeExp: Column): Column = callFn("monthname", timeExp)
  def weekday(e: Column): Column = callFn("weekday", e)
  def convert_timezone(sourceTz: Column, targetTz: Column, sourceTs: Column): Column =
    callFn("convert_timezone", sourceTz, targetTz, sourceTs)
  def convert_timezone(targetTz: Column, sourceTs: Column): Column =
    callFn("convert_timezone", targetTz, sourceTs)

  // ---------------------------------------------------------------------------
  // Null handling / Conditional
  // ---------------------------------------------------------------------------

  def coalesce(cols: Column*): Column = callFn("coalesce", cols*)
  def isnull(col: Column): Column = callFn("isnull", col)
  def isnan(col: Column): Column = callFn("isnan", col)
  def isnotnull(col: Column): Column = callFn("isnotnull", col)
  def assert_true(col: Column): Column = callFn("assert_true", col)
  def raise_error(col: Column): Column = callFn("raise_error", col)

  // ---------------------------------------------------------------------------
  // Conditional
  // ---------------------------------------------------------------------------

  def when(condition: Column, value: Any): Column =
    val v = value match
      case c: Column => c
      case other     => Column.lit(other)
    callFn("when", condition, v)

  // ---------------------------------------------------------------------------
  // Collection functions
  // ---------------------------------------------------------------------------

  def array(cols: Column*): Column = callFn("array", cols*)
  def struct(cols: Column*): Column = callFn("struct", cols*)
  def explode(col: Column): Column = callFn("explode", col)
  def explode_outer(col: Column): Column = callFn("explode_outer", col)
  def posexplode(col: Column): Column = callFn("posexplode", col)
  def posexplode_outer(col: Column): Column = callFn("posexplode_outer", col)
  def size(col: Column): Column = callFn("size", col)
  def array_contains(col: Column, value: Any): Column =
    callFn("array_contains", col, Column.lit(value))
  def array_sort(col: Column): Column = callFn("array_sort", col)
  def array_distinct(col: Column): Column = callFn("array_distinct", col)
  def array_intersect(col1: Column, col2: Column): Column = callFn("array_intersect", col1, col2)
  def array_union(col1: Column, col2: Column): Column = callFn("array_union", col1, col2)
  def array_except(col1: Column, col2: Column): Column = callFn("array_except", col1, col2)
  def array_join(col: Column, delimiter: String): Column =
    callFn("array_join", col, Column.lit(delimiter))
  def array_join(col: Column, delimiter: String, nullReplacement: String): Column =
    callFn("array_join", col, Column.lit(delimiter), Column.lit(nullReplacement))
  def array_max(col: Column): Column = callFn("array_max", col)
  def array_min(col: Column): Column = callFn("array_min", col)
  def array_position(col: Column, value: Any): Column =
    callFn("array_position", col, Column.lit(value))
  def array_remove(col: Column, element: Any): Column =
    callFn("array_remove", col, Column.lit(element))
  def array_repeat(col: Column, count: Int): Column =
    callFn("array_repeat", col, Column.lit(count))
  def arrays_zip(cols: Column*): Column = callFn("arrays_zip", cols*)
  def flatten(col: Column): Column = callFn("flatten", col)
  def element_at(col: Column, extraction: Any): Column =
    callFn("element_at", col, Column.lit(extraction))
  def slice(col: Column, start: Int, length: Int): Column =
    callFn("slice", col, Column.lit(start), Column.lit(length))
  def reverse(col: Column): Column = callFn("reverse", col)
  def shuffle(col: Column): Column = callFn("shuffle", col)
  def sort_array(col: Column, asc: Boolean = true): Column =
    callFn("sort_array", col, Column.lit(asc))
  def array_append(col: Column, element: Column): Column =
    callFn("array_append", col, element)
  def array_prepend(col: Column, element: Column): Column =
    callFn("array_prepend", col, element)
  def array_compact(col: Column): Column = callFn("array_compact", col)
  def array_insert(col: Column, pos: Column, value: Column): Column =
    callFn("array_insert", col, pos, value)
  def arrays_overlap(a1: Column, a2: Column): Column =
    callFn("arrays_overlap", a1, a2)
  def sequence(start: Column, stop: Column, step: Column): Column =
    callFn("sequence", start, stop, step)
  def array_size(col: Column): Column = callFn("array_size", col)
  def get(col: Column, index: Column): Column = callFn("get", col, index)
  def map_contains_key(col: Column, key: Column): Column =
    callFn("map_contains_key", col, key)
  def str_to_map(col: Column, pairDelim: Column, keyValueDelim: Column): Column =
    callFn("str_to_map", col, pairDelim, keyValueDelim)
  def try_element_at(column: Column, value: Column): Column =
    callFn("try_element_at", column, value)
  def cardinality(e: Column): Column = callFn("cardinality", e)
  def named_struct(cols: Column*): Column = callFn("named_struct", cols*)

  // Map functions
  def map(cols: Column*): Column = callFn("map", cols*)
  def map_from_arrays(keys: Column, values: Column): Column =
    callFn("map_from_arrays", keys, values)
  def map_from_entries(col: Column): Column = callFn("map_from_entries", col)
  def map_keys(col: Column): Column = callFn("map_keys", col)
  def map_values(col: Column): Column = callFn("map_values", col)
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
  def json_array_length(e: Column): Column = callFn("json_array_length", e)
  def json_object_keys(e: Column): Column = callFn("json_object_keys", e)

  // ---------------------------------------------------------------------------
  // CSV functions
  // ---------------------------------------------------------------------------

  def from_csv(col: Column, schema: String): Column =
    callFn("from_csv", col, Column.lit(schema))
  def to_csv(col: Column): Column = callFn("to_csv", col)
  def schema_of_csv(csv: String): Column =
    callFn("schema_of_csv", Column.lit(csv))

  // ---------------------------------------------------------------------------
  // XML functions
  // ---------------------------------------------------------------------------

  def xpath(col: Column, path: Column): Column = callFn("xpath", col, path)
  def xpath_string(col: Column, path: Column): Column =
    callFn("xpath_string", col, path)
  def from_xml(e: Column, schema: Column): Column = callFn("from_xml", e, schema)
  def to_xml(e: Column): Column = callFn("to_xml", e)
  def schema_of_xml(xml: Column): Column = callFn("schema_of_xml", xml)
  def schema_of_xml(xml: String): Column = callFn("schema_of_xml", Column.lit(xml))
  def xpath_boolean(xml: Column, path: Column): Column =
    callFn("xpath_boolean", xml, path)
  def xpath_double(xml: Column, path: Column): Column =
    callFn("xpath_double", xml, path)
  def xpath_number(xml: Column, path: Column): Column =
    callFn("xpath_number", xml, path)
  def xpath_float(xml: Column, path: Column): Column =
    callFn("xpath_float", xml, path)
  def xpath_int(xml: Column, path: Column): Column =
    callFn("xpath_int", xml, path)
  def xpath_long(xml: Column, path: Column): Column =
    callFn("xpath_long", xml, path)
  def xpath_short(xml: Column, path: Column): Column =
    callFn("xpath_short", xml, path)

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

  def initcap(col: Column): Column = callFn("initcap", col)
  def soundex(col: Column): Column = callFn("soundex", col)
  def levenshtein(l: Column, r: Column): Column = callFn("levenshtein", l, r)
  def ascii(col: Column): Column = callFn("ascii", col)
  def base64(col: Column): Column = callFn("base64", col)
  def unbase64(col: Column): Column = callFn("unbase64", col)
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

  def dayofweek(col: Column): Column = callFn("dayofweek", col)
  def dayofyear(col: Column): Column = callFn("dayofyear", col)
  def weekofyear(col: Column): Column = callFn("weekofyear", col)
  def quarter(col: Column): Column = callFn("quarter", col)
  def last_day(col: Column): Column = callFn("last_day", col)
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
  def unix_timestamp(): Column = callFn("unix_timestamp")
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
  def make_date(year: Column, month: Column, day: Column): Column =
    callFn("make_date", year, month, day)
  def make_timestamp(
      year: Column,
      month: Column,
      day: Column,
      hours: Column,
      mins: Column,
      secs: Column
  ): Column =
    callFn("make_timestamp", year, month, day, hours, mins, secs)
  def date_part(field: Column, source: Column): Column =
    callFn("date_part", field, source)
  def extract(field: Column, source: Column): Column =
    callFn("extract", field, source)
  def timestamp_seconds(col: Column): Column =
    callFn("timestamp_seconds", col)
  def timestamp_millis(col: Column): Column = callFn("timestamp_millis", col)
  def timestamp_micros(col: Column): Column = callFn("timestamp_micros", col)
  def date_from_unix_date(col: Column): Column =
    callFn("date_from_unix_date", col)
  def current_timezone(): Column = callFn("current_timezone")
  def now(): Column = callFn("now")
  def make_dt_interval(
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column
  ): Column =
    callFn("make_dt_interval", days, hours, mins, secs)
  def make_dt_interval(days: Column, hours: Column, mins: Column): Column =
    callFn("make_dt_interval", days, hours, mins)
  def make_dt_interval(days: Column, hours: Column): Column =
    callFn("make_dt_interval", days, hours)
  def make_dt_interval(days: Column): Column = callFn("make_dt_interval", days)
  def make_dt_interval(): Column = callFn("make_dt_interval")
  def make_interval(
      years: Column,
      months: Column,
      weeks: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column
  ): Column =
    callFn("make_interval", years, months, weeks, days, hours, mins, secs)
  def make_interval(
      years: Column,
      months: Column,
      weeks: Column,
      days: Column,
      hours: Column,
      mins: Column
  ): Column =
    callFn("make_interval", years, months, weeks, days, hours, mins)
  def make_interval(
      years: Column,
      months: Column,
      weeks: Column,
      days: Column,
      hours: Column
  ): Column =
    callFn("make_interval", years, months, weeks, days, hours)
  def make_interval(years: Column, months: Column, weeks: Column, days: Column): Column =
    callFn("make_interval", years, months, weeks, days)
  def make_interval(years: Column, months: Column, weeks: Column): Column =
    callFn("make_interval", years, months, weeks)
  def make_interval(years: Column, months: Column): Column =
    callFn("make_interval", years, months)
  def make_interval(years: Column): Column = callFn("make_interval", years)
  def make_interval(): Column = callFn("make_interval")
  def make_time(hour: Column, minute: Column, second: Column): Column =
    callFn("make_time", hour, minute, second)
  def make_ym_interval(years: Column, months: Column): Column =
    callFn("make_ym_interval", years, months)
  def make_ym_interval(years: Column): Column = callFn("make_ym_interval", years)
  def make_ym_interval(): Column = callFn("make_ym_interval")
  def make_timestamp(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column,
      timezone: Column
  ): Column =
    callFn("make_timestamp", years, months, days, hours, mins, secs, timezone)
  def make_timestamp(date: Column, time: Column, timezone: Column): Column =
    callFn("make_timestamp", date, time, timezone)
  def make_timestamp(date: Column, time: Column): Column =
    callFn("make_timestamp", date, time)
  def make_timestamp_ltz(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column,
      timezone: Column
  ): Column =
    callFn("make_timestamp_ltz", years, months, days, hours, mins, secs, timezone)
  def make_timestamp_ltz(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column
  ): Column =
    callFn("make_timestamp_ltz", years, months, days, hours, mins, secs)
  def make_timestamp_ntz(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column
  ): Column =
    callFn("make_timestamp_ntz", years, months, days, hours, mins, secs)
  def make_timestamp_ntz(date: Column, time: Column): Column =
    callFn("make_timestamp_ntz", date, time)
  def to_timestamp_ltz(timestamp: Column, format: Column): Column =
    callFn("to_timestamp_ltz", timestamp, format)
  def to_timestamp_ltz(timestamp: Column): Column = callFn("to_timestamp_ltz", timestamp)
  def to_timestamp_ntz(timestamp: Column, format: Column): Column =
    callFn("to_timestamp_ntz", timestamp, format)
  def to_timestamp_ntz(timestamp: Column): Column = callFn("to_timestamp_ntz", timestamp)
  def to_time(str: Column): Column = callFn("to_time", str)
  def to_time(str: Column, format: Column): Column = callFn("to_time", str, format)
  def to_unix_timestamp(timeExp: Column, format: Column): Column =
    callFn("to_unix_timestamp", timeExp, format)
  def to_unix_timestamp(timeExp: Column): Column = callFn("to_unix_timestamp", timeExp)
  def unix_date(e: Column): Column = callFn("unix_date", e)
  def unix_micros(e: Column): Column = callFn("unix_micros", e)
  def unix_millis(e: Column): Column = callFn("unix_millis", e)
  def unix_seconds(e: Column): Column = callFn("unix_seconds", e)
  def time_diff(unit: Column, start: Column, end: Column): Column =
    callFn("time_diff", unit, start, end)
  def time_from_micros(e: Column): Column = callFn("time_from_micros", e)
  def time_from_millis(e: Column): Column = callFn("time_from_millis", e)
  def time_from_seconds(e: Column): Column = callFn("time_from_seconds", e)
  def time_to_micros(e: Column): Column = callFn("time_to_micros", e)
  def time_to_millis(e: Column): Column = callFn("time_to_millis", e)
  def time_to_seconds(e: Column): Column = callFn("time_to_seconds", e)
  def time_trunc(unit: Column, time: Column): Column = callFn("time_trunc", unit, time)
  def timestamp_add(unit: String, quantity: Column, ts: Column): Column =
    callFn("timestamp_add", Column.lit(unit), quantity, ts)
  def timestamp_diff(unit: String, start: Column, end: Column): Column =
    callFn("timestamp_diff", Column.lit(unit), start, end)
  def try_to_date(e: Column): Column = callFn("try_to_date", e)
  def try_to_date(e: Column, fmt: String): Column =
    callFn("try_to_date", e, Column.lit(fmt))
  def try_to_time(str: Column): Column = callFn("try_to_time", str)
  def try_to_time(str: Column, format: Column): Column =
    callFn("try_to_time", str, format)
  def try_make_interval(
      years: Column,
      months: Column,
      weeks: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column
  ): Column =
    callFn("try_make_interval", years, months, weeks, days, hours, mins, secs)
  def try_make_interval(
      years: Column,
      months: Column,
      weeks: Column,
      days: Column,
      hours: Column,
      mins: Column
  ): Column =
    callFn("try_make_interval", years, months, weeks, days, hours, mins)
  def try_make_interval(
      years: Column,
      months: Column,
      weeks: Column,
      days: Column,
      hours: Column
  ): Column =
    callFn("try_make_interval", years, months, weeks, days, hours)
  def try_make_interval(
      years: Column,
      months: Column,
      weeks: Column,
      days: Column
  ): Column =
    callFn("try_make_interval", years, months, weeks, days)
  def try_make_interval(years: Column, months: Column, weeks: Column): Column =
    callFn("try_make_interval", years, months, weeks)
  def try_make_interval(years: Column, months: Column): Column =
    callFn("try_make_interval", years, months)
  def try_make_interval(years: Column): Column = callFn("try_make_interval", years)
  def try_make_timestamp(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column,
      timezone: Column
  ): Column =
    callFn("try_make_timestamp", years, months, days, hours, mins, secs, timezone)
  def try_make_timestamp(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column
  ): Column =
    callFn("try_make_timestamp", years, months, days, hours, mins, secs)
  def try_make_timestamp(date: Column, time: Column, timezone: Column): Column =
    callFn("try_make_timestamp", date, time, timezone)
  def try_make_timestamp(date: Column, time: Column): Column =
    callFn("try_make_timestamp", date, time)
  def try_make_timestamp_ltz(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column,
      timezone: Column
  ): Column =
    callFn("try_make_timestamp_ltz", years, months, days, hours, mins, secs, timezone)
  def try_make_timestamp_ltz(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column
  ): Column =
    callFn("try_make_timestamp_ltz", years, months, days, hours, mins, secs)
  def try_make_timestamp_ntz(
      years: Column,
      months: Column,
      days: Column,
      hours: Column,
      mins: Column,
      secs: Column
  ): Column =
    callFn("try_make_timestamp_ntz", years, months, days, hours, mins, secs)
  def try_make_timestamp_ntz(date: Column, time: Column): Column =
    callFn("try_make_timestamp_ntz", date, time)
  def session_window(timeColumn: Column, gapDuration: String): Column =
    callFn("session_window", timeColumn, Column.lit(gapDuration))
  def session_window(timeColumn: Column, gapDuration: Column): Column =
    callFn("session_window", timeColumn, gapDuration)
  def window_time(windowColumn: Column): Column = callFn("window_time", windowColumn)

  // ---------------------------------------------------------------------------
  // Math functions (extended)
  // ---------------------------------------------------------------------------

  def sin(col: Column): Column = callFn("sin", col)
  def cos(col: Column): Column = callFn("cos", col)
  def tan(col: Column): Column = callFn("tan", col)
  def asin(col: Column): Column = callFn("asin", col)
  def acos(col: Column): Column = callFn("acos", col)
  def atan(col: Column): Column = callFn("atan", col)
  def atan2(l: Column, r: Column): Column = callFn("atan2", l, r)
  def sinh(col: Column): Column = callFn("sinh", col)
  def cosh(col: Column): Column = callFn("cosh", col)
  def tanh(col: Column): Column = callFn("tanh", col)
  def cbrt(col: Column): Column = callFn("cbrt", col)
  def rint(col: Column): Column = callFn("rint", col)
  def signum(col: Column): Column = callFn("signum", col)
  def degrees(col: Column): Column = callFn("degrees", col)
  def radians(col: Column): Column = callFn("radians", col)
  def bround(col: Column, scale: Int = 0): Column = callFn("bround", col, Column.lit(scale))
  def bin(col: Column): Column = callFn("bin", col)
  def hex(col: Column): Column = callFn("hex", col)
  def unhex(col: Column): Column = callFn("unhex", col)
  def conv(col: Column, fromBase: Int, toBase: Int): Column =
    callFn("conv", col, Column.lit(fromBase), Column.lit(toBase))
  def factorial(col: Column): Column = callFn("factorial", col)
  def log(base: Double, col: Column): Column = callFn("log", Column.lit(base), col)
  def shiftleft(col: Column, numBits: Int): Column =
    callFn("shiftleft", col, Column.lit(numBits))
  def shiftright(col: Column, numBits: Int): Column =
    callFn("shiftright", col, Column.lit(numBits))
  def shiftrightunsigned(col: Column, numBits: Int): Column =
    callFn("shiftrightunsigned", col, Column.lit(numBits))
  def bit_count(col: Column): Column = callFn("bit_count", col)
  def bit_get(col: Column, pos: Column): Column = callFn("bit_get", col, pos)
  def acosh(e: Column): Column = callFn("acosh", e)
  def asinh(e: Column): Column = callFn("asinh", e)
  def atanh(e: Column): Column = callFn("atanh", e)
  def cot(e: Column): Column = callFn("cot", e)
  def csc(e: Column): Column = callFn("csc", e)
  def sec(e: Column): Column = callFn("sec", e)
  def toDegrees(e: Column): Column = callFn("degrees", e)
  def toRadians(e: Column): Column = callFn("radians", e)

  // ---------------------------------------------------------------------------
  // Aggregate functions (extended)
  // ---------------------------------------------------------------------------

  def sumDistinct(col: Column): Column = callFn("sum", isDistinct = true, col)
  def approx_count_distinct(col: Column): Column = callFn("approx_count_distinct", col)
  def approx_count_distinct(col: Column, rsd: Double): Column =
    callFn("approx_count_distinct", col, Column.lit(rsd))
  @deprecated("Use approx_count_distinct", "2.1.0")
  def approxCountDistinct(col: Column): Column = approx_count_distinct(col)
  @deprecated("Use approx_count_distinct", "2.1.0")
  def approxCountDistinct(col: Column, rsd: Double): Column = approx_count_distinct(col, rsd)
  def variance(col: Column): Column = callFn("variance", col)
  def var_pop(col: Column): Column = callFn("var_pop", col)
  def var_samp(col: Column): Column = callFn("var_samp", col)
  def stddev(col: Column): Column = callFn("stddev", col)
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
  def try_avg(col: Column): Column = callFn("try_avg", col)
  def try_sum(col: Column): Column = callFn("try_sum", col)
  def array_agg(e: Column): Column = callFn("array_agg", e)

  // ---------------------------------------------------------------------------
  // Misc functions
  // ---------------------------------------------------------------------------

  def monotonically_increasing_id(): Column = callFn("monotonically_increasing_id")
  @deprecated("Use monotonically_increasing_id", "2.0.0")
  def monotonicallyIncreasingId(): Column = monotonically_increasing_id()
  def spark_partition_id(): Column = callFn("spark_partition_id")
  def input_file_name(): Column = callFn("input_file_name")
  def typedLit[T](value: T): Column = Column.lit(value)
  def typedlit[T](value: T): Column = typedLit(value)
  def negate(col: Column): Column = callFn("negative", col)
  def not(col: Column): Column = callFn("not", col)
  def bitwiseNOT(col: Column): Column = callFn("~", col)
  def hash(cols: Column*): Column = callFn("hash", cols*)
  def xxhash64(cols: Column*): Column = callFn("xxhash64", cols*)
  def md5(col: Column): Column = callFn("md5", col)
  def sha1(col: Column): Column = callFn("sha1", col)
  def sha2(col: Column, numBits: Int): Column = callFn("sha2", col, Column.lit(numBits))
  def crc32(col: Column): Column = callFn("crc32", col)
  def nanvl(col1: Column, col2: Column): Column = callFn("nanvl", col1, col2)
  def ifnull(col1: Column, col2: Column): Column = callFn("ifnull", col1, col2)
  def nullif(col1: Column, col2: Column): Column = callFn("nullif", col1, col2)
  def nvl(col1: Column, col2: Column): Column = callFn("nvl", col1, col2)
  def nvl2(col1: Column, col2: Column, col3: Column): Column = callFn("nvl2", col1, col2, col3)
  def typeof(col: Column): Column = callFn("typeof", col)
  def version(): Column = callFn("version")
  def current_user(): Column = callFn("current_user")
  def current_catalog(): Column = callFn("current_catalog")
  def current_database(): Column = callFn("current_database")
  def current_schema(): Column = callFn("current_schema")
  def uuid(): Column = callFn("uuid")
  def session_user(): Column = callFn("session_user")
  def stack(n: Column, cols: Column*): Column =
    callFn("stack", (n +: cols)*)
  def inline(col: Column): Column = callFn("inline", col)
  def inline_outer(col: Column): Column = callFn("inline_outer", col)
  def bitmap_bit_position(col: Column): Column = callFn("bitmap_bit_position", col)
  def bitmap_bucket_number(col: Column): Column = callFn("bitmap_bucket_number", col)
  def bitmap_count(col: Column): Column = callFn("bitmap_count", col)
  def aes_encrypt(
      input: Column,
      key: Column,
      mode: Column,
      padding: Column,
      iv: Column,
      aad: Column
  ): Column =
    callFn("aes_encrypt", input, key, mode, padding, iv, aad)
  def aes_encrypt(input: Column, key: Column, mode: Column, padding: Column, iv: Column): Column =
    callFn("aes_encrypt", input, key, mode, padding, iv)
  def aes_encrypt(input: Column, key: Column, mode: Column, padding: Column): Column =
    callFn("aes_encrypt", input, key, mode, padding)
  def aes_encrypt(input: Column, key: Column, mode: Column): Column =
    callFn("aes_encrypt", input, key, mode)
  def aes_encrypt(input: Column, key: Column): Column =
    callFn("aes_encrypt", input, key)
  def aes_decrypt(
      input: Column,
      key: Column,
      mode: Column,
      padding: Column,
      aad: Column
  ): Column =
    callFn("aes_decrypt", input, key, mode, padding, aad)
  def aes_decrypt(input: Column, key: Column, mode: Column, padding: Column): Column =
    callFn("aes_decrypt", input, key, mode, padding)
  def aes_decrypt(input: Column, key: Column, mode: Column): Column =
    callFn("aes_decrypt", input, key, mode)
  def aes_decrypt(input: Column, key: Column): Column =
    callFn("aes_decrypt", input, key)
  def try_aes_decrypt(
      input: Column,
      key: Column,
      mode: Column,
      padding: Column,
      aad: Column
  ): Column =
    callFn("try_aes_decrypt", input, key, mode, padding, aad)
  def try_aes_decrypt(input: Column, key: Column, mode: Column, padding: Column): Column =
    callFn("try_aes_decrypt", input, key, mode, padding)
  def try_aes_decrypt(input: Column, key: Column, mode: Column): Column =
    callFn("try_aes_decrypt", input, key, mode)
  def try_aes_decrypt(input: Column, key: Column): Column =
    callFn("try_aes_decrypt", input, key)
  def reflect(cols: Column*): Column = callFn("reflect", cols*)
  def try_reflect(cols: Column*): Column = callFn("try_reflect", cols*)
  def java_method(cols: Column*): Column = callFn("java_method", cols*)
  def user(): Column = callFn("user")
  def call_function(funcName: String, cols: Column*): Column =
    callFn(funcName, cols*)
  def call_udf(udfName: String, cols: Column*): Column =
    call_function(udfName, cols*)
  def callUDF(udfName: String, cols: Column*): Column =
    call_function(udfName, cols*)
  def input_file_block_length(): Column = callFn("input_file_block_length")
  def input_file_block_start(): Column = callFn("input_file_block_start")
  def sha(col: Column): Column = callFn("sha", col)
  def bitwise_not(e: Column): Column = callFn("~", e)
  def shiftLeft(e: Column, numBits: Int): Column =
    callFn("shiftleft", e, Column.lit(numBits))
  def shiftRight(e: Column, numBits: Int): Column =
    callFn("shiftright", e, Column.lit(numBits))
  def shiftRightUnsigned(e: Column, numBits: Int): Column =
    callFn("shiftrightunsigned", e, Column.lit(numBits))
  def getbit(e: Column, pos: Column): Column = callFn("getbit", e, pos)

  // ---------------------------------------------------------------------------
  // Conditional / Predicate functions
  // ---------------------------------------------------------------------------

  def nullifzero(col: Column): Column = callFn("nullifzero", col)
  def zeroifnull(col: Column): Column = callFn("zeroifnull", col)
  def equal_null(col1: Column, col2: Column): Column = callFn("equal_null", col1, col2)

  // ---------------------------------------------------------------------------
  // URL functions
  // ---------------------------------------------------------------------------

  def url_encode(str: Column): Column = callFn("url_encode", str)
  def url_decode(str: Column): Column = callFn("url_decode", str)
  def try_url_decode(str: Column): Column = callFn("try_url_decode", str)
  def parse_url(url: Column, partToExtract: Column, key: Column): Column =
    callFn("parse_url", url, partToExtract, key)
  def parse_url(url: Column, partToExtract: Column): Column =
    callFn("parse_url", url, partToExtract)
  def try_parse_url(url: Column, partToExtract: Column, key: Column): Column =
    callFn("try_parse_url", url, partToExtract, key)
  def try_parse_url(url: Column, partToExtract: Column): Column =
    callFn("try_parse_url", url, partToExtract)

  // ---------------------------------------------------------------------------
  // VARIANT functions
  // ---------------------------------------------------------------------------

  def parse_json(json: Column): Column = callFn("parse_json", json)
  def try_parse_json(json: Column): Column = callFn("try_parse_json", json)
  def variant_get(v: Column, path: String, targetType: String): Column =
    callFn("variant_get", v, Column.lit(path), Column.lit(targetType))
  def variant_get(v: Column, path: Column, targetType: String): Column =
    callFn("variant_get", v, path, Column.lit(targetType))
  def try_variant_get(v: Column, path: String, targetType: String): Column =
    callFn("try_variant_get", v, Column.lit(path), Column.lit(targetType))
  def try_variant_get(v: Column, path: Column, targetType: String): Column =
    callFn("try_variant_get", v, path, Column.lit(targetType))
  def is_variant_null(v: Column): Column = callFn("is_variant_null", v)
  def schema_of_variant(v: Column): Column = callFn("schema_of_variant", v)
  def schema_of_variant_agg(v: Column): Column = callFn("schema_of_variant_agg", v)
  def to_variant_object(col: Column): Column = callFn("to_variant_object", col)

  // ---------------------------------------------------------------------------
  // Partition transform functions
  // ---------------------------------------------------------------------------

  def years(e: Column): Column = callFn("years", e)
  def months(e: Column): Column = callFn("months", e)
  def days(e: Column): Column = callFn("days", e)
  def hours(e: Column): Column = callFn("hours", e)
  def bucket(numBuckets: Column, e: Column): Column = callFn("bucket", numBuckets, e)
  def bucket(numBuckets: Int, e: Column): Column =
    callFn("bucket", Column.lit(numBuckets), e)

  // ---------------------------------------------------------------------------
  // Datasketch functions (HLL)
  // ---------------------------------------------------------------------------

  def hll_sketch_estimate(c: Column): Column = callFn("hll_sketch_estimate", c)
  def hll_union(c1: Column, c2: Column): Column = callFn("hll_union", c1, c2)
  def hll_union(c1: Column, c2: Column, allowDifferentLgConfigK: Boolean): Column =
    callFn("hll_union", c1, c2, Column.lit(allowDifferentLgConfigK))

  // ---------------------------------------------------------------------------
  // Datasketch functions (Theta)
  // ---------------------------------------------------------------------------

  def theta_sketch_agg(e: Column, lgNomEntries: Column): Column =
    callFn("theta_sketch_agg", e, lgNomEntries)
  def theta_sketch_agg(e: Column, lgNomEntries: Int): Column =
    callFn("theta_sketch_agg", e, Column.lit(lgNomEntries))
  def theta_sketch_agg(e: Column): Column = callFn("theta_sketch_agg", e)
  def theta_intersection_agg(e: Column): Column = callFn("theta_intersection_agg", e)
  def theta_union_agg(e: Column, lgNomEntries: Column): Column =
    callFn("theta_union_agg", e, lgNomEntries)
  def theta_union_agg(e: Column, lgNomEntries: Int): Column =
    callFn("theta_union_agg", e, Column.lit(lgNomEntries))
  def theta_union_agg(e: Column): Column = callFn("theta_union_agg", e)
  def theta_difference(c1: Column, c2: Column): Column =
    callFn("theta_difference", c1, c2)
  def theta_intersection(c1: Column, c2: Column): Column =
    callFn("theta_intersection", c1, c2)
  def theta_sketch_estimate(c: Column): Column = callFn("theta_sketch_estimate", c)
  def theta_union(c1: Column, c2: Column): Column = callFn("theta_union", c1, c2)
  def theta_union(c1: Column, c2: Column, lgNomEntries: Int): Column =
    callFn("theta_union", c1, c2, Column.lit(lgNomEntries))
  def theta_union(c1: Column, c2: Column, lgNomEntries: Column): Column =
    callFn("theta_union", c1, c2, lgNomEntries)

  // ---------------------------------------------------------------------------
  // Datasketch functions (KLL)
  // ---------------------------------------------------------------------------

  def kll_sketch_agg_bigint(e: Column, k: Column): Column =
    callFn("kll_sketch_agg_bigint", e, k)
  def kll_sketch_agg_bigint(e: Column, k: Int): Column =
    callFn("kll_sketch_agg_bigint", e, Column.lit(k))
  def kll_sketch_agg_bigint(e: Column): Column = callFn("kll_sketch_agg_bigint", e)
  def kll_sketch_agg_float(e: Column, k: Column): Column =
    callFn("kll_sketch_agg_float", e, k)
  def kll_sketch_agg_float(e: Column, k: Int): Column =
    callFn("kll_sketch_agg_float", e, Column.lit(k))
  def kll_sketch_agg_float(e: Column): Column = callFn("kll_sketch_agg_float", e)
  def kll_sketch_agg_double(e: Column, k: Column): Column =
    callFn("kll_sketch_agg_double", e, k)
  def kll_sketch_agg_double(e: Column, k: Int): Column =
    callFn("kll_sketch_agg_double", e, Column.lit(k))
  def kll_sketch_agg_double(e: Column): Column = callFn("kll_sketch_agg_double", e)
  def kll_merge_agg_bigint(e: Column, k: Column): Column =
    callFn("kll_merge_agg_bigint", e, k)
  def kll_merge_agg_bigint(e: Column, k: Int): Column =
    callFn("kll_merge_agg_bigint", e, Column.lit(k))
  def kll_merge_agg_bigint(e: Column): Column = callFn("kll_merge_agg_bigint", e)
  def kll_merge_agg_float(e: Column, k: Column): Column =
    callFn("kll_merge_agg_float", e, k)
  def kll_merge_agg_float(e: Column, k: Int): Column =
    callFn("kll_merge_agg_float", e, Column.lit(k))
  def kll_merge_agg_float(e: Column): Column = callFn("kll_merge_agg_float", e)
  def kll_merge_agg_double(e: Column, k: Column): Column =
    callFn("kll_merge_agg_double", e, k)
  def kll_merge_agg_double(e: Column, k: Int): Column =
    callFn("kll_merge_agg_double", e, Column.lit(k))
  def kll_merge_agg_double(e: Column): Column = callFn("kll_merge_agg_double", e)
  def kll_sketch_get_n_bigint(e: Column): Column = callFn("kll_sketch_get_n_bigint", e)
  def kll_sketch_get_n_float(e: Column): Column = callFn("kll_sketch_get_n_float", e)
  def kll_sketch_get_n_double(e: Column): Column = callFn("kll_sketch_get_n_double", e)
  def kll_sketch_get_quantile_bigint(sketch: Column, rank: Column): Column =
    callFn("kll_sketch_get_quantile_bigint", sketch, rank)
  def kll_sketch_get_quantile_float(sketch: Column, rank: Column): Column =
    callFn("kll_sketch_get_quantile_float", sketch, rank)
  def kll_sketch_get_quantile_double(sketch: Column, rank: Column): Column =
    callFn("kll_sketch_get_quantile_double", sketch, rank)
  def kll_sketch_get_rank_bigint(sketch: Column, quantile: Column): Column =
    callFn("kll_sketch_get_rank_bigint", sketch, quantile)
  def kll_sketch_get_rank_float(sketch: Column, quantile: Column): Column =
    callFn("kll_sketch_get_rank_float", sketch, quantile)
  def kll_sketch_get_rank_double(sketch: Column, quantile: Column): Column =
    callFn("kll_sketch_get_rank_double", sketch, quantile)
  def kll_sketch_merge_bigint(left: Column, right: Column): Column =
    callFn("kll_sketch_merge_bigint", left, right)
  def kll_sketch_merge_float(left: Column, right: Column): Column =
    callFn("kll_sketch_merge_float", left, right)
  def kll_sketch_merge_double(left: Column, right: Column): Column =
    callFn("kll_sketch_merge_double", left, right)
  def kll_sketch_to_string_bigint(e: Column): Column =
    callFn("kll_sketch_to_string_bigint", e)
  def kll_sketch_to_string_float(e: Column): Column =
    callFn("kll_sketch_to_string_float", e)
  def kll_sketch_to_string_double(e: Column): Column =
    callFn("kll_sketch_to_string_double", e)

  // ---------------------------------------------------------------------------
  // Datasketch functions (Tuple)
  // ---------------------------------------------------------------------------

  def tuple_sketch_agg_double(
      key: Column,
      summary: Column,
      lgNomEntries: Column,
      mode: Column
  ): Column =
    callFn("tuple_sketch_agg_double", key, summary, lgNomEntries, mode)
  def tuple_sketch_agg_double(
      key: Column,
      summary: Column,
      lgNomEntries: Int,
      mode: String
  ): Column =
    callFn(
      "tuple_sketch_agg_double",
      key,
      summary,
      Column.lit(lgNomEntries),
      Column.lit(mode)
    )
  def tuple_sketch_agg_double(key: Column, summary: Column, lgNomEntries: Int): Column =
    callFn("tuple_sketch_agg_double", key, summary, Column.lit(lgNomEntries))
  def tuple_sketch_agg_double(key: Column, summary: Column): Column =
    callFn("tuple_sketch_agg_double", key, summary)
  def tuple_sketch_agg_integer(
      key: Column,
      summary: Column,
      lgNomEntries: Column,
      mode: Column
  ): Column =
    callFn("tuple_sketch_agg_integer", key, summary, lgNomEntries, mode)
  def tuple_sketch_agg_integer(
      key: Column,
      summary: Column,
      lgNomEntries: Int,
      mode: String
  ): Column =
    callFn(
      "tuple_sketch_agg_integer",
      key,
      summary,
      Column.lit(lgNomEntries),
      Column.lit(mode)
    )
  def tuple_sketch_agg_integer(key: Column, summary: Column, lgNomEntries: Int): Column =
    callFn("tuple_sketch_agg_integer", key, summary, Column.lit(lgNomEntries))
  def tuple_sketch_agg_integer(key: Column, summary: Column): Column =
    callFn("tuple_sketch_agg_integer", key, summary)
  def tuple_intersection_agg_double(e: Column, mode: Column): Column =
    callFn("tuple_intersection_agg_double", e, mode)
  def tuple_intersection_agg_double(e: Column, mode: String): Column =
    callFn("tuple_intersection_agg_double", e, Column.lit(mode))
  def tuple_intersection_agg_double(e: Column): Column =
    callFn("tuple_intersection_agg_double", e)
  def tuple_intersection_agg_integer(e: Column, mode: Column): Column =
    callFn("tuple_intersection_agg_integer", e, mode)
  def tuple_intersection_agg_integer(e: Column, mode: String): Column =
    callFn("tuple_intersection_agg_integer", e, Column.lit(mode))
  def tuple_intersection_agg_integer(e: Column): Column =
    callFn("tuple_intersection_agg_integer", e)
  def tuple_union_agg_double(e: Column, lgNomEntries: Column, mode: Column): Column =
    callFn("tuple_union_agg_double", e, lgNomEntries, mode)
  def tuple_union_agg_double(e: Column, lgNomEntries: Int, mode: String): Column =
    callFn("tuple_union_agg_double", e, Column.lit(lgNomEntries), Column.lit(mode))
  def tuple_union_agg_double(e: Column, lgNomEntries: Int): Column =
    callFn("tuple_union_agg_double", e, Column.lit(lgNomEntries))
  def tuple_union_agg_double(e: Column): Column = callFn("tuple_union_agg_double", e)
  def tuple_union_agg_integer(e: Column, lgNomEntries: Column, mode: Column): Column =
    callFn("tuple_union_agg_integer", e, lgNomEntries, mode)
  def tuple_union_agg_integer(e: Column, lgNomEntries: Int, mode: String): Column =
    callFn("tuple_union_agg_integer", e, Column.lit(lgNomEntries), Column.lit(mode))
  def tuple_union_agg_integer(e: Column, lgNomEntries: Int): Column =
    callFn("tuple_union_agg_integer", e, Column.lit(lgNomEntries))
  def tuple_union_agg_integer(e: Column): Column = callFn("tuple_union_agg_integer", e)
  def tuple_difference_double(c1: Column, c2: Column): Column =
    callFn("tuple_difference_double", c1, c2)
  def tuple_difference_integer(c1: Column, c2: Column): Column =
    callFn("tuple_difference_integer", c1, c2)
  def tuple_intersection_double(c1: Column, c2: Column): Column =
    callFn("tuple_intersection_double", c1, c2)
  def tuple_intersection_double(c1: Column, c2: Column, mode: String): Column =
    callFn("tuple_intersection_double", c1, c2, Column.lit(mode))
  def tuple_intersection_double(c1: Column, c2: Column, mode: Column): Column =
    callFn("tuple_intersection_double", c1, c2, mode)
  def tuple_intersection_integer(c1: Column, c2: Column): Column =
    callFn("tuple_intersection_integer", c1, c2)
  def tuple_intersection_integer(c1: Column, c2: Column, mode: String): Column =
    callFn("tuple_intersection_integer", c1, c2, Column.lit(mode))
  def tuple_intersection_integer(c1: Column, c2: Column, mode: Column): Column =
    callFn("tuple_intersection_integer", c1, c2, mode)
  def tuple_difference_theta_double(c1: Column, c2: Column): Column =
    callFn("tuple_difference_theta_double", c1, c2)
  def tuple_difference_theta_integer(c1: Column, c2: Column): Column =
    callFn("tuple_difference_theta_integer", c1, c2)
  def tuple_intersection_theta_double(c1: Column, c2: Column): Column =
    callFn("tuple_intersection_theta_double", c1, c2)
  def tuple_intersection_theta_double(c1: Column, c2: Column, mode: String): Column =
    callFn("tuple_intersection_theta_double", c1, c2, Column.lit(mode))
  def tuple_intersection_theta_double(c1: Column, c2: Column, mode: Column): Column =
    callFn("tuple_intersection_theta_double", c1, c2, mode)
  def tuple_intersection_theta_integer(c1: Column, c2: Column): Column =
    callFn("tuple_intersection_theta_integer", c1, c2)
  def tuple_intersection_theta_integer(c1: Column, c2: Column, mode: String): Column =
    callFn("tuple_intersection_theta_integer", c1, c2, Column.lit(mode))
  def tuple_intersection_theta_integer(c1: Column, c2: Column, mode: Column): Column =
    callFn("tuple_intersection_theta_integer", c1, c2, mode)
  def tuple_sketch_estimate_double(c: Column): Column =
    callFn("tuple_sketch_estimate_double", c)
  def tuple_sketch_estimate_integer(c: Column): Column =
    callFn("tuple_sketch_estimate_integer", c)
  def tuple_sketch_summary_double(c: Column): Column =
    callFn("tuple_sketch_summary_double", c)
  def tuple_sketch_summary_double(c: Column, mode: String): Column =
    callFn("tuple_sketch_summary_double", c, Column.lit(mode))
  def tuple_sketch_summary_double(c: Column, mode: Column): Column =
    callFn("tuple_sketch_summary_double", c, mode)
  def tuple_sketch_summary_integer(c: Column): Column =
    callFn("tuple_sketch_summary_integer", c)
  def tuple_sketch_summary_integer(c: Column, mode: String): Column =
    callFn("tuple_sketch_summary_integer", c, Column.lit(mode))
  def tuple_sketch_summary_integer(c: Column, mode: Column): Column =
    callFn("tuple_sketch_summary_integer", c, mode)
  def tuple_sketch_theta_double(c: Column): Column =
    callFn("tuple_sketch_theta_double", c)
  def tuple_sketch_theta_integer(c: Column): Column =
    callFn("tuple_sketch_theta_integer", c)
  def tuple_union_double(c1: Column, c2: Column): Column =
    callFn("tuple_union_double", c1, c2)
  def tuple_union_double(c1: Column, c2: Column, lgNomEntries: Int): Column =
    callFn("tuple_union_double", c1, c2, Column.lit(lgNomEntries))
  def tuple_union_double(
      c1: Column,
      c2: Column,
      lgNomEntries: Int,
      mode: String
  ): Column =
    callFn("tuple_union_double", c1, c2, Column.lit(lgNomEntries), Column.lit(mode))
  def tuple_union_double(
      c1: Column,
      c2: Column,
      lgNomEntries: Column,
      mode: Column
  ): Column =
    callFn("tuple_union_double", c1, c2, lgNomEntries, mode)
  def tuple_union_integer(c1: Column, c2: Column): Column =
    callFn("tuple_union_integer", c1, c2)
  def tuple_union_integer(c1: Column, c2: Column, lgNomEntries: Int): Column =
    callFn("tuple_union_integer", c1, c2, Column.lit(lgNomEntries))
  def tuple_union_integer(
      c1: Column,
      c2: Column,
      lgNomEntries: Int,
      mode: String
  ): Column =
    callFn("tuple_union_integer", c1, c2, Column.lit(lgNomEntries), Column.lit(mode))
  def tuple_union_integer(
      c1: Column,
      c2: Column,
      lgNomEntries: Column,
      mode: Column
  ): Column =
    callFn("tuple_union_integer", c1, c2, lgNomEntries, mode)
  def tuple_union_theta_double(c1: Column, c2: Column): Column =
    callFn("tuple_union_theta_double", c1, c2)
  def tuple_union_theta_double(c1: Column, c2: Column, lgNomEntries: Int): Column =
    callFn("tuple_union_theta_double", c1, c2, Column.lit(lgNomEntries))
  def tuple_union_theta_double(
      c1: Column,
      c2: Column,
      lgNomEntries: Int,
      mode: String
  ): Column =
    callFn("tuple_union_theta_double", c1, c2, Column.lit(lgNomEntries), Column.lit(mode))
  def tuple_union_theta_double(
      c1: Column,
      c2: Column,
      lgNomEntries: Column,
      mode: Column
  ): Column =
    callFn("tuple_union_theta_double", c1, c2, lgNomEntries, mode)
  def tuple_union_theta_integer(c1: Column, c2: Column): Column =
    callFn("tuple_union_theta_integer", c1, c2)
  def tuple_union_theta_integer(c1: Column, c2: Column, lgNomEntries: Int): Column =
    callFn("tuple_union_theta_integer", c1, c2, Column.lit(lgNomEntries))
  def tuple_union_theta_integer(
      c1: Column,
      c2: Column,
      lgNomEntries: Int,
      mode: String
  ): Column =
    callFn(
      "tuple_union_theta_integer",
      c1,
      c2,
      Column.lit(lgNomEntries),
      Column.lit(mode)
    )
  def tuple_union_theta_integer(
      c1: Column,
      c2: Column,
      lgNomEntries: Column,
      mode: Column
  ): Column =
    callFn("tuple_union_theta_integer", c1, c2, lgNomEntries, mode)

  // ---------------------------------------------------------------------------
  // Geospatial functions
  // ---------------------------------------------------------------------------

  def st_asbinary(geo: Column): Column = callFn("st_asbinary", geo)
  def st_geogfromwkb(wkb: Column): Column = callFn("st_geogfromwkb", wkb)
  def st_geomfromwkb(wkb: Column): Column = callFn("st_geomfromwkb", wkb)
  def st_geomfromwkb(wkb: Column, srid: Column): Column =
    callFn("st_geomfromwkb", wkb, srid)
  def st_geomfromwkb(wkb: Column, srid: Int): Column =
    callFn("st_geomfromwkb", wkb, Column.lit(srid))
  def st_setsrid(geo: Column, srid: Column): Column = callFn("st_setsrid", geo, srid)
  def st_setsrid(geo: Column, srid: Int): Column =
    callFn("st_setsrid", geo, Column.lit(srid))
  def st_srid(geo: Column): Column = callFn("st_srid", geo)
  // Try functions
  // ---------------------------------------------------------------------------

  def try_add(left: Column, right: Column): Column =
    callFn("try_add", left, right)
  def try_subtract(left: Column, right: Column): Column =
    callFn("try_subtract", left, right)
  def try_multiply(left: Column, right: Column): Column =
    callFn("try_multiply", left, right)
  def try_divide(left: Column, right: Column): Column =
    callFn("try_divide", left, right)
  def try_to_number(col: Column, format: Column): Column =
    callFn("try_to_number", col, format)
  def try_to_timestamp(col: Column): Column = callFn("try_to_timestamp", col)

  /** Broadcast hint for a DataFrame — returns the same DataFrame with a broadcast hint. */
  def broadcast(df: DataFrame): DataFrame = df.hint("broadcast")

  // ---------------------------------------------------------------------------
  // Window functions
  // ---------------------------------------------------------------------------

  def row_number(): Column = callFn("row_number")
  def rank(): Column = callFn("rank")
  def dense_rank(): Column = callFn("dense_rank")
  def lead(col: Column, offset: Int = 1): Column =
    callFn("lead", col, Column.lit(offset))
  def lag(col: Column, offset: Int = 1): Column =
    callFn("lag", col, Column.lit(offset))
  def ntile(n: Int): Column = callFn("ntile", Column.lit(n))
  def percent_rank(): Column = callFn("percent_rank")
  def cume_dist(): Column = callFn("cume_dist")
  def nth_value(col: Column, offset: Int): Column =
    callFn("nth_value", col, Column.lit(offset))

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

  private def callInternalFn(name: String, cols: Column*): Column =
    val builder = Expression.UnresolvedFunction.newBuilder()
      .setFunctionName(name)
      .setIsInternal(true)
    cols.foreach(c => builder.addArguments(c.expr))
    Column(Expression.newBuilder()
      .setUnresolvedFunction(builder.build())
      .build())

  def unwrap_udt(column: Column): Column = callInternalFn("unwrap_udt", column)

  // ---------------------------------------------------------------------------
  // User-Defined Functions (UDF)
  // ---------------------------------------------------------------------------

  /** Create a UDF from a Function0. */
  inline def udf[R](f: () => R): UserDefinedFunction =
    UserDefinedFunction(
      func = f,
      returnType = Encoder.sparkTypeOf[R],
      inputTypes = Seq.empty
    )

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
        Encoder.sparkTypeOf[T1],
        Encoder.sparkTypeOf[T2],
        Encoder.sparkTypeOf[T3]
      )
    )

  /** Create a UDF from a Function4. */
  inline def udf[T1, T2, T3, T4, R](f: (T1, T2, T3, T4) => R): UserDefinedFunction =
    UserDefinedFunction(
      func = f,
      returnType = Encoder.sparkTypeOf[R],
      inputTypes = Seq(
        Encoder.sparkTypeOf[T1],
        Encoder.sparkTypeOf[T2],
        Encoder.sparkTypeOf[T3],
        Encoder.sparkTypeOf[T4]
      )
    )

  /** Create a UDF from a Function5. */
  inline def udf[T1, T2, T3, T4, T5, R](f: (T1, T2, T3, T4, T5) => R): UserDefinedFunction =
    UserDefinedFunction(
      func = f,
      returnType = Encoder.sparkTypeOf[R],
      inputTypes = Seq(
        Encoder.sparkTypeOf[T1],
        Encoder.sparkTypeOf[T2],
        Encoder.sparkTypeOf[T3],
        Encoder.sparkTypeOf[T4],
        Encoder.sparkTypeOf[T5]
      )
    )

  /** Create a UDF from a Function6. */
  inline def udf[T1, T2, T3, T4, T5, T6, R](
      f: (T1, T2, T3, T4, T5, T6) => R
  ): UserDefinedFunction =
    UserDefinedFunction(
      func = f,
      returnType = Encoder.sparkTypeOf[R],
      inputTypes = Seq(
        Encoder.sparkTypeOf[T1],
        Encoder.sparkTypeOf[T2],
        Encoder.sparkTypeOf[T3],
        Encoder.sparkTypeOf[T4],
        Encoder.sparkTypeOf[T5],
        Encoder.sparkTypeOf[T6]
      )
    )

  /** Create a UDF from a Function7. */
  inline def udf[T1, T2, T3, T4, T5, T6, T7, R](
      f: (T1, T2, T3, T4, T5, T6, T7) => R
  ): UserDefinedFunction =
    UserDefinedFunction(
      func = f,
      returnType = Encoder.sparkTypeOf[R],
      inputTypes = Seq(
        Encoder.sparkTypeOf[T1],
        Encoder.sparkTypeOf[T2],
        Encoder.sparkTypeOf[T3],
        Encoder.sparkTypeOf[T4],
        Encoder.sparkTypeOf[T5],
        Encoder.sparkTypeOf[T6],
        Encoder.sparkTypeOf[T7]
      )
    )

  /** Create a UDF from a Function8. */
  inline def udf[T1, T2, T3, T4, T5, T6, T7, T8, R](
      f: (T1, T2, T3, T4, T5, T6, T7, T8) => R
  ): UserDefinedFunction =
    UserDefinedFunction(
      func = f,
      returnType = Encoder.sparkTypeOf[R],
      inputTypes = Seq(
        Encoder.sparkTypeOf[T1],
        Encoder.sparkTypeOf[T2],
        Encoder.sparkTypeOf[T3],
        Encoder.sparkTypeOf[T4],
        Encoder.sparkTypeOf[T5],
        Encoder.sparkTypeOf[T6],
        Encoder.sparkTypeOf[T7],
        Encoder.sparkTypeOf[T8]
      )
    )

  /** Create a UDF from a Function9. */
  inline def udf[T1, T2, T3, T4, T5, T6, T7, T8, T9, R](
      f: (T1, T2, T3, T4, T5, T6, T7, T8, T9) => R
  ): UserDefinedFunction =
    UserDefinedFunction(
      func = f,
      returnType = Encoder.sparkTypeOf[R],
      inputTypes = Seq(
        Encoder.sparkTypeOf[T1],
        Encoder.sparkTypeOf[T2],
        Encoder.sparkTypeOf[T3],
        Encoder.sparkTypeOf[T4],
        Encoder.sparkTypeOf[T5],
        Encoder.sparkTypeOf[T6],
        Encoder.sparkTypeOf[T7],
        Encoder.sparkTypeOf[T8],
        Encoder.sparkTypeOf[T9]
      )
    )

  /** Create a UDF from a Function10. */
  inline def udf[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, R](
      f: (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10) => R
  ): UserDefinedFunction =
    UserDefinedFunction(
      func = f,
      returnType = Encoder.sparkTypeOf[R],
      inputTypes = Seq(
        Encoder.sparkTypeOf[T1],
        Encoder.sparkTypeOf[T2],
        Encoder.sparkTypeOf[T3],
        Encoder.sparkTypeOf[T4],
        Encoder.sparkTypeOf[T5],
        Encoder.sparkTypeOf[T6],
        Encoder.sparkTypeOf[T7],
        Encoder.sparkTypeOf[T8],
        Encoder.sparkTypeOf[T9],
        Encoder.sparkTypeOf[T10]
      )
    )

  // ---------------------------------------------------------------------------
  // User-Defined Aggregate Functions (UDAF)
  // ---------------------------------------------------------------------------

  /** Create a UDAF from an [[Aggregator]] with an implicit input encoder.
    *
    * {{{
    *   val myUdaf = udaf(myAggregator)
    *   df.agg(myUdaf(col("value")))
    * }}}
    */
  def udaf[IN, BUF, OUT](
      agg: Aggregator[IN, BUF, OUT]
  )(using enc: Encoder[IN]): UserDefinedFunction =
    val inputEnc = extractAgnosticEncoder(enc)
    val outputEnc = extractAgnosticEncoder(agg.outputEncoder)
    UserDefinedFunction.forAggregator(agg, inputEnc, outputEnc)

  /** Create a UDAF from an [[Aggregator]] with an explicit input encoder.
    *
    * {{{
    *   val myUdaf = udaf(myAggregator, Encoders.scalaLong)
    *   df.agg(myUdaf(col("value")))
    * }}}
    */
  @targetName("udafWithEncoder")
  def udaf[IN, BUF, OUT](
      agg: Aggregator[IN, BUF, OUT],
      inputEncoder: Encoder[IN]
  ): UserDefinedFunction =
    val inputEnc = extractAgnosticEncoder(inputEncoder)
    val outputEnc = extractAgnosticEncoder(agg.outputEncoder)
    UserDefinedFunction.forAggregator(agg, inputEnc, outputEnc)

  private def extractAgnosticEncoder[T](enc: Encoder[T]): AgnosticEncoder[?] =
    Encoders.asAgnostic(enc)

  // ---------------------------------------------------------------------------
  // Higher-order functions (lambda-based)
  // ---------------------------------------------------------------------------

  /** Apply a transform function to each element of an array. */
  def transform(col: Column, f: Column => Column): Column =
    callFn("transform", col, createLambda1(f))

  /** Apply a transform function (element, index) to each element of an array. */
  def transform(col: Column, f: (Column, Column) => Column): Column =
    callFn("transform", col, createLambda2(f))

  /** Filter elements of an array using a predicate. */
  def filter(col: Column, f: Column => Column): Column =
    callFn("filter", col, createLambda1(f))

  /** Filter elements of an array using a predicate (element, index). */
  def filter(col: Column, f: (Column, Column) => Column): Column =
    callFn("filter", col, createLambda2(f))

  /** Return whether any element of the array satisfies the predicate. */
  def exists(col: Column, f: Column => Column): Column =
    callFn("exists", col, createLambda1(f))

  /** Return whether all elements of the array satisfy the predicate. */
  def forall(col: Column, f: Column => Column): Column =
    callFn("forall", col, createLambda1(f))

  /** Aggregate elements of an array using an accumulator with a finish function. */
  def aggregate(
      col: Column,
      initialValue: Column,
      merge: (Column, Column) => Column,
      finish: Column => Column
  ): Column =
    callFn("aggregate", col, initialValue, createLambda2(merge), createLambda1(finish))

  /** Aggregate elements of an array using an accumulator (no finish function). */
  def aggregate(
      col: Column,
      initialValue: Column,
      merge: (Column, Column) => Column
  ): Column =
    callFn("aggregate", col, initialValue, createLambda2(merge))

  /** Reduce elements of an array using an accumulator with a finish function. Alias of aggregate.
    */
  def reduce(
      expr: Column,
      initialValue: Column,
      merge: (Column, Column) => Column,
      finish: Column => Column
  ): Column =
    callFn("reduce", expr, initialValue, createLambda2(merge), createLambda1(finish))

  /** Reduce elements of an array using an accumulator (no finish). Alias of aggregate. */
  def reduce(
      expr: Column,
      initialValue: Column,
      merge: (Column, Column) => Column
  ): Column =
    callFn("reduce", expr, initialValue, createLambda2(merge))

  /** Merge two arrays element-wise using a function. */
  def zip_with(left: Column, right: Column, f: (Column, Column) => Column): Column =
    callFn("zip_with", left, right, createLambda2(f))

  /** Filter entries in a map using a predicate on key and value. */
  def map_filter(col: Column, f: (Column, Column) => Column): Column =
    callFn("map_filter", col, createLambda2(f))

  /** Transform keys of a map using a function of (key, value). */
  def transform_keys(col: Column, f: (Column, Column) => Column): Column =
    callFn("transform_keys", col, createLambda2(f))

  /** Transform values of a map using a function of (key, value). */
  def transform_values(col: Column, f: (Column, Column) => Column): Column =
    callFn("transform_values", col, createLambda2(f))

  /** Sort an array using a comparator function. */
  def array_sort(col: Column, comparator: (Column, Column) => Column): Column =
    callFn("array_sort", col, createLambda2(comparator))

  // ---------------------------------------------------------------------------
  // Lambda helpers
  // ---------------------------------------------------------------------------

  private def createLambda1(f: Column => Column): Column =
    val x = lambdaVar("x")
    val xCol = Column(Expression.newBuilder().setUnresolvedNamedLambdaVariable(x).build())
    val body = f(xCol).expr
    Column(Expression.newBuilder().setLambdaFunction(
      Expression.LambdaFunction.newBuilder()
        .setFunction(body)
        .addArguments(x)
        .build()
    ).build())

  private def createLambda2(f: (Column, Column) => Column): Column =
    val x = lambdaVar("x")
    val y = lambdaVar("y")
    val xCol = Column(Expression.newBuilder().setUnresolvedNamedLambdaVariable(x).build())
    val yCol = Column(Expression.newBuilder().setUnresolvedNamedLambdaVariable(y).build())
    val body = f(xCol, yCol).expr
    Column(Expression.newBuilder().setLambdaFunction(
      Expression.LambdaFunction.newBuilder()
        .setFunction(body)
        .addArguments(x)
        .addArguments(y)
        .build()
    ).build())

  private def lambdaVar(name: String): Expression.UnresolvedNamedLambdaVariable =
    Expression.UnresolvedNamedLambdaVariable.newBuilder()
      .addNameParts(name)
      .build()
