package org.apache.spark.sql

import org.apache.spark.connect.proto.Expression
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FunctionsSuite extends AnyFunSuite with Matchers:

  private def assertFn(c: Column, expectedName: String, expectedArgCount: Int): Unit =
    c.expr.hasUnresolvedFunction shouldBe true
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe expectedName
    fn.getArgumentsList should have size expectedArgCount

  test("col and column create UnresolvedAttribute") {
    functions.col("x").expr.hasUnresolvedAttribute shouldBe true
    functions.column("y").expr.hasUnresolvedAttribute shouldBe true
  }

  test("lit delegates to Column.lit") {
    val c = functions.lit(42)
    c.expr.hasLiteral shouldBe true
  }

  test("expr creates ExpressionString") {
    val c = functions.expr("a + b")
    c.expr.hasExpressionString shouldBe true
    c.expr.getExpressionString.getExpression shouldBe "a + b"
  }

  test("aggregate functions") {
    assertFn(functions.count(Column("x")), "count", 1)
    assertFn(functions.sum(Column("x")), "sum", 1)
    assertFn(functions.avg(Column("x")), "avg", 1)
    assertFn(functions.min(Column("x")), "min", 1)
    assertFn(functions.max(Column("x")), "max", 1)
    assertFn(functions.first(Column("x")), "first", 1)
    assertFn(functions.last(Column("x")), "last", 1)
  }

  test("countDistinct sets isDistinct") {
    val c = functions.countDistinct(Column("a"), Column("b"))
    c.expr.hasUnresolvedFunction shouldBe true
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "count"
    fn.getIsDistinct shouldBe true
    fn.getArgumentsList should have size 2
  }

  test("math functions") {
    assertFn(functions.abs(Column("x")), "abs", 1)
    assertFn(functions.sqrt(Column("x")), "sqrt", 1)
    assertFn(functions.floor(Column("x")), "floor", 1)
    assertFn(functions.ceil(Column("x")), "ceil", 1)
    assertFn(functions.log(Column("x")), "ln", 1)
    assertFn(functions.exp(Column("x")), "exp", 1)
    assertFn(functions.pow(Column("a"), Column("b")), "power", 2)
    assertFn(functions.round(Column("x"), 2), "round", 2)
  }

  test("string functions") {
    assertFn(functions.upper(Column("x")), "upper", 1)
    assertFn(functions.lower(Column("x")), "lower", 1)
    assertFn(functions.trim(Column("x")), "trim", 1)
    assertFn(functions.length(Column("x")), "length", 1)
    assertFn(functions.concat(Column("a"), Column("b")), "concat", 2)
    assertFn(functions.substring(Column("x"), 1, 3), "substring", 3)
  }

  test("date functions") {
    assertFn(functions.current_date(), "current_date", 0)
    assertFn(functions.current_timestamp(), "current_timestamp", 0)
    assertFn(functions.year(Column("d")), "year", 1)
    assertFn(functions.month(Column("d")), "month", 1)
  }

  test("window functions") {
    assertFn(functions.row_number(), "row_number", 0)
    assertFn(functions.rank(), "rank", 0)
    assertFn(functions.dense_rank(), "dense_rank", 0)
    assertFn(functions.lead(Column("x"), 2), "lead", 2)
    assertFn(functions.lag(Column("x"), 1), "lag", 2)
  }

  test("collection functions") {
    assertFn(functions.array(Column("a"), Column("b")), "array", 2)
    assertFn(functions.struct(Column("a")), "struct", 1)
    assertFn(functions.explode(Column("arr")), "explode", 1)
    assertFn(functions.size(Column("arr")), "size", 1)
  }

  test("string overloads: count(String), sum(String)") {
    assertFn(functions.count("x"), "count", 1)
    assertFn(functions.sum("x"), "sum", 1)
    assertFn(functions.avg("x"), "avg", 1)
    assertFn(functions.min("x"), "min", 1)
    assertFn(functions.max("x"), "max", 1)
  }

  // ----- Phase 2 tests -----

  test("regex functions") {
    assertFn(functions.regexp_extract(Column("x"), "\\d+", 0), "regexp_extract", 3)
    assertFn(functions.regexp_replace(Column("x"), "a", "b"), "regexp_replace", 3)
    assertFn(functions.split(Column("x"), ","), "split", 2)
    assertFn(functions.split(Column("x"), ",", 3), "split", 3)
  }

  test("json functions") {
    assertFn(functions.from_json(Column("j"), "struct<a:int>"), "from_json", 2)
    assertFn(functions.to_json(Column("s")), "to_json", 1)
    assertFn(functions.get_json_object(Column("j"), "$.a"), "get_json_object", 2)
  }

  test("array functions") {
    assertFn(functions.array_sort(Column("arr")), "array_sort", 1)
    assertFn(functions.array_distinct(Column("arr")), "array_distinct", 1)
    assertFn(functions.array_intersect(Column("a"), Column("b")), "array_intersect", 2)
    assertFn(functions.array_union(Column("a"), Column("b")), "array_union", 2)
    assertFn(functions.flatten(Column("nested")), "flatten", 1)
    assertFn(functions.reverse(Column("arr")), "reverse", 1)
    assertFn(functions.element_at(Column("arr"), 1), "element_at", 2)
  }

  test("map functions") {
    assertFn(functions.map_keys(Column("m")), "map_keys", 1)
    assertFn(functions.map_values(Column("m")), "map_values", 1)
    assertFn(functions.map_entries(Column("m")), "map_entries", 1)
    assertFn(functions.map_from_arrays(Column("k"), Column("v")), "map_from_arrays", 2)
  }

  test("extended math functions") {
    assertFn(functions.sin(Column("x")), "sin", 1)
    assertFn(functions.cos(Column("x")), "cos", 1)
    assertFn(functions.cbrt(Column("x")), "cbrt", 1)
    assertFn(functions.atan2(Column("a"), Column("b")), "atan2", 2)
    assertFn(functions.hex(Column("x")), "hex", 1)
    assertFn(functions.bin(Column("x")), "bin", 1)
    assertFn(functions.log(2.0, Column("x")), "log", 2)
  }

  test("extended date functions") {
    assertFn(functions.dayofweek(Column("d")), "dayofweek", 1)
    assertFn(functions.dayofyear(Column("d")), "dayofyear", 1)
    assertFn(functions.quarter(Column("d")), "quarter", 1)
    assertFn(functions.datediff(Column("a"), Column("b")), "datediff", 2)
    assertFn(functions.unix_timestamp(), "unix_timestamp", 0)
    assertFn(functions.from_unixtime(Column("ts")), "from_unixtime", 1)
  }

  test("extended aggregate functions") {
    assertFn(functions.variance(Column("x")), "variance", 1)
    assertFn(functions.stddev(Column("x")), "stddev", 1)
    assertFn(functions.corr(Column("a"), Column("b")), "corr", 2)
    assertFn(functions.approx_count_distinct(Column("x")), "approx_count_distinct", 1)
    assertFn(functions.skewness(Column("x")), "skewness", 1)
    assertFn(functions.kurtosis(Column("x")), "kurtosis", 1)
  }

  test("extended string functions") {
    assertFn(functions.initcap(Column("x")), "initcap", 1)
    assertFn(functions.soundex(Column("x")), "soundex", 1)
    assertFn(functions.ascii(Column("x")), "ascii", 1)
    assertFn(functions.base64(Column("x")), "base64", 1)
    assertFn(functions.repeat(Column("x"), 3), "repeat", 2)
    assertFn(functions.format_number(Column("x"), 2), "format_number", 2)
  }

  test("misc functions") {
    assertFn(functions.monotonically_increasing_id(), "monotonically_increasing_id", 0)
    assertFn(functions.spark_partition_id(), "spark_partition_id", 0)
    assertFn(functions.hash(Column("a"), Column("b")), "hash", 2)
    assertFn(functions.md5(Column("x")), "md5", 1)
    assertFn(functions.sha1(Column("x")), "sha1", 1)
    assertFn(functions.crc32(Column("x")), "crc32", 1)
  }

  test("explode variants") {
    assertFn(functions.explode_outer(Column("arr")), "explode_outer", 1)
    assertFn(functions.posexplode(Column("arr")), "posexplode", 1)
    assertFn(functions.posexplode_outer(Column("arr")), "posexplode_outer", 1)
  }

  // ----- Phase 3 (Task B) tests -----

  test("sort functions") {
    val a = functions.asc("x")
    a.expr.hasSortOrder shouldBe true
    a.expr.getSortOrder.getDirection.toString should include("ASCENDING")

    val anf = functions.asc_nulls_first("x")
    anf.expr.hasSortOrder shouldBe true
    anf.expr.getSortOrder.getDirection.toString should include("ASCENDING")
    anf.expr.getSortOrder.getNullOrdering.toString should include("NULLS_FIRST")

    val anl = functions.asc_nulls_last("x")
    anl.expr.hasSortOrder shouldBe true
    anl.expr.getSortOrder.getDirection.toString should include("ASCENDING")
    anl.expr.getSortOrder.getNullOrdering.toString should include("NULLS_LAST")

    val d = functions.desc("x")
    d.expr.hasSortOrder shouldBe true
    d.expr.getSortOrder.getDirection.toString should include("DESCENDING")

    val dnf = functions.desc_nulls_first("x")
    dnf.expr.hasSortOrder shouldBe true
    dnf.expr.getSortOrder.getDirection.toString should include("DESCENDING")
    dnf.expr.getSortOrder.getNullOrdering.toString should include("NULLS_FIRST")

    val dnl = functions.desc_nulls_last("x")
    dnl.expr.hasSortOrder shouldBe true
    dnl.expr.getSortOrder.getDirection.toString should include("DESCENDING")
    dnl.expr.getSortOrder.getNullOrdering.toString should include("NULLS_LAST")
  }

  test("Column asc_nulls_first/last, desc_nulls_first/last") {
    val c = Column("x")
    c.asc_nulls_first.expr.getSortOrder.getNullOrdering.toString should include("NULLS_FIRST")
    c.asc_nulls_last.expr.getSortOrder.getNullOrdering.toString should include("NULLS_LAST")
    c.desc_nulls_first.expr.getSortOrder.getNullOrdering.toString should include("NULLS_FIRST")
    c.desc_nulls_last.expr.getSortOrder.getNullOrdering.toString should include("NULLS_LAST")
  }

  test("new window functions") {
    assertFn(functions.percent_rank(), "percent_rank", 0)
    assertFn(functions.cume_dist(), "cume_dist", 0)
    assertFn(functions.nth_value(Column("x"), 2), "nth_value", 2)
  }

  test("conditional / null functions") {
    assertFn(functions.isnull(Column("x")), "isnull", 1)
    assertFn(functions.isnan(Column("x")), "isnan", 1)
    assertFn(functions.isnotnull(Column("x")), "isnotnull", 1)
    assertFn(functions.assert_true(Column("x")), "assert_true", 1)
    assertFn(functions.raise_error(Column("x")), "raise_error", 1)
  }

  test("new aggregate functions") {
    assertFn(functions.any_value(Column("x")), "any_value", 1)
    assertFn(functions.count_if(Column("x")), "count_if", 1)
    assertFn(functions.product(Column("x")), "product", 1)
    assertFn(functions.every(Column("x")), "every", 1)
    assertFn(functions.some(Column("x")), "some", 1)
    assertFn(functions.bool_and(Column("x")), "bool_and", 1)
    assertFn(functions.bool_or(Column("x")), "bool_or", 1)
    assertFn(functions.bit_and(Column("x")), "bit_and", 1)
    assertFn(functions.bit_or(Column("x")), "bit_or", 1)
    assertFn(functions.bit_xor(Column("x")), "bit_xor", 1)
    assertFn(functions.first_value(Column("x")), "first_value", 1)
    assertFn(functions.last_value(Column("x")), "last_value", 1)
  }

  test("new math functions") {
    assertFn(functions.log1p(Column("x")), "log1p", 1)
    assertFn(functions.expm1(Column("x")), "expm1", 1)
    assertFn(functions.hypot(Column("a"), Column("b")), "hypot", 2)
    assertFn(functions.pmod(Column("a"), Column("b")), "pmod", 2)
    assertFn(functions.sign(Column("x")), "sign", 1)
    assertFn(functions.e(), "e", 0)
    assertFn(functions.pi(), "pi", 0)
    assertFn(
      functions.width_bucket(Column("v"), Column("lo"), Column("hi"), Column("n")),
      "width_bucket",
      4
    )
  }

  test("new string functions") {
    assertFn(functions.left(Column("x"), 3), "left", 2)
    assertFn(functions.right(Column("x"), 3), "right", 2)
    assertFn(functions.char_length(Column("x")), "char_length", 1)
    assertFn(functions.bit_length(Column("x")), "bit_length", 1)
    assertFn(functions.octet_length(Column("x")), "octet_length", 1)
    assertFn(functions.contains(Column("a"), Column("b")), "contains", 2)
    assertFn(functions.startswith(Column("a"), Column("b")), "startswith", 2)
    assertFn(functions.endswith(Column("a"), Column("b")), "endswith", 2)
    assertFn(functions.btrim(Column("x")), "btrim", 1)
    assertFn(functions.position(Column("sub"), Column("str")), "position", 2)
    assertFn(
      functions.sentences(Column("s"), Column("l"), Column("c")),
      "sentences",
      3
    )
  }

  test("new date/time functions") {
    assertFn(
      functions.make_date(Column("y"), Column("m"), Column("d")),
      "make_date",
      3
    )
    assertFn(
      functions.make_timestamp(
        Column("y"),
        Column("m"),
        Column("d"),
        Column("h"),
        Column("mi"),
        Column("s")
      ),
      "make_timestamp",
      6
    )
    assertFn(functions.date_part(Column("f"), Column("s")), "date_part", 2)
    assertFn(functions.extract(Column("f"), Column("s")), "extract", 2)
    assertFn(functions.timestamp_seconds(Column("x")), "timestamp_seconds", 1)
    assertFn(functions.timestamp_millis(Column("x")), "timestamp_millis", 1)
    assertFn(functions.timestamp_micros(Column("x")), "timestamp_micros", 1)
    assertFn(functions.date_from_unix_date(Column("x")), "date_from_unix_date", 1)
    assertFn(functions.current_timezone(), "current_timezone", 0)
    assertFn(functions.now(), "now", 0)
  }

  test("new collection functions") {
    assertFn(functions.array_append(Column("a"), Column("e")), "array_append", 2)
    assertFn(functions.array_prepend(Column("a"), Column("e")), "array_prepend", 2)
    assertFn(functions.array_compact(Column("a")), "array_compact", 1)
    assertFn(
      functions.array_insert(Column("a"), Column("p"), Column("v")),
      "array_insert",
      3
    )
    assertFn(functions.arrays_overlap(Column("a"), Column("b")), "arrays_overlap", 2)
    assertFn(
      functions.sequence(Column("s"), Column("e"), Column("st")),
      "sequence",
      3
    )
    assertFn(functions.array_size(Column("a")), "array_size", 1)
    assertFn(functions.get(Column("a"), Column("i")), "get", 2)
    assertFn(functions.map_contains_key(Column("m"), Column("k")), "map_contains_key", 2)
    assertFn(
      functions.str_to_map(Column("s"), Column("p"), Column("kv")),
      "str_to_map",
      3
    )
  }

  test("csv functions") {
    assertFn(functions.from_csv(Column("c"), "a INT"), "from_csv", 2)
    assertFn(functions.to_csv(Column("s")), "to_csv", 1)
    assertFn(functions.schema_of_csv("1,2,3"), "schema_of_csv", 1)
  }

  test("xml functions") {
    assertFn(functions.xpath(Column("x"), Column("p")), "xpath", 2)
    assertFn(functions.xpath_string(Column("x"), Column("p")), "xpath_string", 2)
  }

  test("bitwise / shift functions") {
    assertFn(functions.shiftleft(Column("x"), 2), "shiftleft", 2)
    assertFn(functions.shiftright(Column("x"), 2), "shiftright", 2)
    assertFn(functions.shiftrightunsigned(Column("x"), 2), "shiftrightunsigned", 2)
    assertFn(functions.bit_count(Column("x")), "bit_count", 1)
    assertFn(functions.bit_get(Column("x"), Column("p")), "bit_get", 2)
  }

  test("new misc functions") {
    assertFn(functions.typeof(Column("x")), "typeof", 1)
    assertFn(functions.version(), "version", 0)
    assertFn(functions.current_user(), "current_user", 0)
    assertFn(functions.current_catalog(), "current_catalog", 0)
    assertFn(functions.current_database(), "current_database", 0)
    assertFn(functions.current_schema(), "current_schema", 0)
    assertFn(functions.uuid(), "uuid", 0)
    assertFn(functions.session_user(), "session_user", 0)
    assertFn(functions.stack(Column.lit(2), Column("a"), Column("b")), "stack", 3)
    assertFn(functions.inline(Column("x")), "inline", 1)
    assertFn(functions.inline_outer(Column("x")), "inline_outer", 1)
  }

  test("try functions") {
    assertFn(functions.try_add(Column("a"), Column("b")), "try_add", 2)
    assertFn(functions.try_subtract(Column("a"), Column("b")), "try_subtract", 2)
    assertFn(functions.try_multiply(Column("a"), Column("b")), "try_multiply", 2)
    assertFn(functions.try_divide(Column("a"), Column("b")), "try_divide", 2)
    assertFn(functions.try_avg(Column("x")), "try_avg", 1)
    assertFn(functions.try_sum(Column("x")), "try_sum", 1)
    assertFn(functions.try_to_number(Column("x"), Column("f")), "try_to_number", 2)
    assertFn(functions.try_to_timestamp(Column("x")), "try_to_timestamp", 1)
  }

  // ----- Phase 4 (Task E) tests: Higher-order functions -----

  test("transform with 1-arg lambda") {
    val c = functions.transform(Column("arr"), x => x + Column.lit(1))
    c.expr.hasUnresolvedFunction shouldBe true
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "transform"
    fn.getArgumentsList should have size 2
    // second arg should be a LambdaFunction
    fn.getArguments(1).hasLambdaFunction shouldBe true
    fn.getArguments(1).getLambdaFunction.getArgumentsList should have size 1
  }

  test("transform with 2-arg lambda") {
    val c = functions.transform(Column("arr"), (x, i) => x + i)
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "transform"
    fn.getArguments(1).hasLambdaFunction shouldBe true
    fn.getArguments(1).getLambdaFunction.getArgumentsList should have size 2
  }

  test("filter with 1-arg lambda") {
    val c = functions.filter(Column("arr"), x => x > Column.lit(0))
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "filter"
    fn.getArguments(1).hasLambdaFunction shouldBe true
    fn.getArguments(1).getLambdaFunction.getArgumentsList should have size 1
  }

  test("exists builds lambda") {
    val c = functions.exists(Column("arr"), x => x > Column.lit(0))
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "exists"
    fn.getArguments(1).hasLambdaFunction shouldBe true
  }

  test("forall builds lambda") {
    val c = functions.forall(Column("arr"), x => x > Column.lit(0))
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "forall"
    fn.getArguments(1).hasLambdaFunction shouldBe true
  }

  test("aggregate with finish function") {
    val c = functions.aggregate(
      Column("arr"),
      Column.lit(0),
      (acc, x) => acc + x,
      result => result * Column.lit(2)
    )
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "aggregate"
    fn.getArgumentsList should have size 4
    // arg 2 is the merge lambda (2-arg)
    fn.getArguments(2).hasLambdaFunction shouldBe true
    fn.getArguments(2).getLambdaFunction.getArgumentsList should have size 2
    // arg 3 is the finish lambda (1-arg)
    fn.getArguments(3).hasLambdaFunction shouldBe true
    fn.getArguments(3).getLambdaFunction.getArgumentsList should have size 1
  }

  test("aggregate without finish function") {
    val c = functions.aggregate(
      Column("arr"),
      Column.lit(0),
      (acc, x) => acc + x
    )
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "aggregate"
    fn.getArgumentsList should have size 3
  }

  test("zip_with builds lambda") {
    val c = functions.zip_with(Column("a"), Column("b"), (x, y) => x + y)
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "zip_with"
    fn.getArgumentsList should have size 3
    fn.getArguments(2).hasLambdaFunction shouldBe true
    fn.getArguments(2).getLambdaFunction.getArgumentsList should have size 2
  }

  test("map_filter with lambda") {
    val c = functions.map_filter(Column("m"), (k, v) => v > Column.lit(0))
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "map_filter"
    fn.getArguments(1).hasLambdaFunction shouldBe true
  }

  test("transform_keys builds lambda") {
    val c = functions.transform_keys(Column("m"), (k, v) => functions.upper(k))
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "transform_keys"
    fn.getArguments(1).hasLambdaFunction shouldBe true
  }

  test("transform_values builds lambda") {
    val c = functions.transform_values(Column("m"), (k, v) => v + Column.lit(1))
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "transform_values"
    fn.getArguments(1).hasLambdaFunction shouldBe true
  }

  test("array_sort with comparator lambda") {
    val c = functions.array_sort(Column("arr"), (a, b) => a - b)
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "array_sort"
    fn.getArgumentsList should have size 2
    fn.getArguments(1).hasLambdaFunction shouldBe true
    fn.getArguments(1).getLambdaFunction.getArgumentsList should have size 2
  }

  // ===================================================================
  // Comprehensive tests for all untested functions
  // ===================================================================

  // ----- Aggregate functions (untested) -----

  test("mean delegates to avg") {
    assertFn(functions.mean(Column("x")), "avg", 1)
  }

  test("collect_list and collect_set") {
    assertFn(functions.collect_list(Column("x")), "collect_list", 1)
    assertFn(functions.collect_set(Column("x")), "collect_set", 1)
  }

  test("max_by and min_by") {
    assertFn(functions.max_by(Column("e"), Column("o")), "max_by", 2)
    assertFn(functions.max_by(Column("e"), Column("o"), 3), "max_by", 3)
    assertFn(functions.max_by(Column("e"), Column("o"), Column("k")), "max_by", 3)
    assertFn(functions.min_by(Column("e"), Column("o")), "min_by", 2)
    assertFn(functions.min_by(Column("e"), Column("o"), 3), "min_by", 3)
    assertFn(functions.min_by(Column("e"), Column("o"), Column("k")), "min_by", 3)
  }

  test("median and mode") {
    assertFn(functions.median(Column("x")), "median", 1)
    assertFn(functions.mode(Column("x")), "mode", 1)
    assertFn(functions.mode(Column("x"), true), "mode", 2)
  }

  test("percentile") {
    assertFn(functions.percentile(Column("x"), Column("p")), "percentile", 2)
    assertFn(functions.percentile(Column("x"), Column("p"), Column("f")), "percentile", 3)
  }

  test("any and std") {
    assertFn(functions.any(Column("x")), "any", 1)
    assertFn(functions.std(Column("x")), "std", 1)
  }

  test("sum_distinct sets isDistinct") {
    val c = functions.sum_distinct(Column("x"))
    c.expr.hasUnresolvedFunction shouldBe true
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "sum"
    fn.getIsDistinct shouldBe true
    fn.getArgumentsList should have size 1
  }

  test("count_distinct sets isDistinct") {
    val c = functions.count_distinct(Column("a"), Column("b"))
    c.expr.hasUnresolvedFunction shouldBe true
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "count"
    fn.getIsDistinct shouldBe true
    fn.getArgumentsList should have size 2
  }

  test("var_pop and var_samp") {
    assertFn(functions.var_pop(Column("x")), "var_pop", 1)
    assertFn(functions.var_samp(Column("x")), "var_samp", 1)
  }

  test("stddev_pop and stddev_samp") {
    assertFn(functions.stddev_pop(Column("x")), "stddev_pop", 1)
    assertFn(functions.stddev_samp(Column("x")), "stddev_samp", 1)
  }

  test("covar_pop and covar_samp") {
    assertFn(functions.covar_pop(Column("a"), Column("b")), "covar_pop", 2)
    assertFn(functions.covar_samp(Column("a"), Column("b")), "covar_samp", 2)
  }

  test("grouping and grouping_id") {
    assertFn(functions.grouping(Column("x")), "grouping", 1)
    assertFn(functions.grouping_id(Column("a"), Column("b")), "grouping_id", 2)
  }

  test("percentile_approx") {
    assertFn(
      functions.percentile_approx(Column("x"), Column("p"), Column("a")),
      "percentile_approx",
      3
    )
  }

  test("approx_percentile") {
    assertFn(
      functions.approx_percentile(Column("x"), Column("p"), Column("a")),
      "approx_percentile",
      3
    )
  }

  test("array_agg") {
    assertFn(functions.array_agg(Column("x")), "array_agg", 1)
  }

  test("histogram_numeric") {
    assertFn(functions.histogram_numeric(Column("x"), Column("n")), "histogram_numeric", 2)
  }

  test("count_min_sketch") {
    assertFn(
      functions.count_min_sketch(Column("x"), Column("e"), Column("c"), Column("s")),
      "count_min_sketch",
      4
    )
    assertFn(
      functions.count_min_sketch(Column("x"), Column("e"), Column("c")),
      "count_min_sketch",
      3
    )
  }

  test("regr functions") {
    assertFn(functions.regr_avgx(Column("y"), Column("x")), "regr_avgx", 2)
    assertFn(functions.regr_avgy(Column("y"), Column("x")), "regr_avgy", 2)
    assertFn(functions.regr_count(Column("y"), Column("x")), "regr_count", 2)
    assertFn(functions.regr_intercept(Column("y"), Column("x")), "regr_intercept", 2)
    assertFn(functions.regr_r2(Column("y"), Column("x")), "regr_r2", 2)
    assertFn(functions.regr_slope(Column("y"), Column("x")), "regr_slope", 2)
    assertFn(functions.regr_sxx(Column("y"), Column("x")), "regr_sxx", 2)
    assertFn(functions.regr_sxy(Column("y"), Column("x")), "regr_sxy", 2)
    assertFn(functions.regr_syy(Column("y"), Column("x")), "regr_syy", 2)
  }

  test("bitmap aggregate functions") {
    assertFn(functions.bitmap_construct_agg(Column("x")), "bitmap_construct_agg", 1)
    assertFn(functions.bitmap_or_agg(Column("x")), "bitmap_or_agg", 1)
    assertFn(functions.bitmap_and_agg(Column("x")), "bitmap_and_agg", 1)
  }

  test("hll_sketch_agg") {
    assertFn(functions.hll_sketch_agg(Column("x"), Column("k")), "hll_sketch_agg", 2)
    assertFn(functions.hll_sketch_agg(Column("x"), 12), "hll_sketch_agg", 2)
    assertFn(functions.hll_sketch_agg(Column("x")), "hll_sketch_agg", 1)
  }

  test("hll_union_agg") {
    assertFn(functions.hll_union_agg(Column("x"), Column("a")), "hll_union_agg", 2)
    assertFn(functions.hll_union_agg(Column("x"), true), "hll_union_agg", 2)
    assertFn(functions.hll_union_agg(Column("x")), "hll_union_agg", 1)
  }

  test("listagg") {
    assertFn(functions.listagg(Column("x")), "listagg", 1)
    assertFn(functions.listagg(Column("x"), Column("d")), "listagg", 2)
  }

  test("listagg_distinct") {
    assertFn(functions.listagg_distinct(Column("x")), "listagg_distinct", 1)
    assertFn(functions.listagg_distinct(Column("x"), Column("d")), "listagg_distinct", 2)
  }

  test("string_agg") {
    assertFn(functions.string_agg(Column("x")), "string_agg", 1)
    assertFn(functions.string_agg(Column("x"), Column("d")), "string_agg", 2)
  }

  test("string_agg_distinct") {
    assertFn(functions.string_agg_distinct(Column("x")), "string_agg_distinct", 1)
    assertFn(functions.string_agg_distinct(Column("x"), Column("d")), "string_agg_distinct", 2)
  }

  test("sumDistinct (deprecated alias)") {
    val c = functions.sumDistinct(Column("x"))
    c.expr.hasUnresolvedFunction shouldBe true
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "sum"
    fn.getIsDistinct shouldBe true
  }

  // ----- Math functions (untested) -----

  test("log10 and log2") {
    assertFn(functions.log10(Column("x")), "log10", 1)
    assertFn(functions.log2(Column("x")), "log2", 1)
  }

  test("greatest and least") {
    assertFn(functions.greatest(Column("a"), Column("b"), Column("c")), "greatest", 3)
    assertFn(functions.least(Column("a"), Column("b"), Column("c")), "least", 3)
  }

  test("rand and randn") {
    assertFn(functions.rand(), "rand", 1)
    assertFn(functions.rand(42L), "rand", 1)
    assertFn(functions.randn(), "randn", 1)
    assertFn(functions.randn(42L), "randn", 1)
  }

  test("ceiling") {
    assertFn(functions.ceiling(Column("x")), "ceiling", 1)
    assertFn(functions.ceiling(Column("x"), Column("s")), "ceiling", 2)
  }

  test("ln") {
    assertFn(functions.ln(Column("x")), "ln", 1)
  }

  test("negative and positive") {
    assertFn(functions.negative(Column("x")), "negative", 1)
    assertFn(functions.positive(Column("x")), "positive", 1)
  }

  test("trig functions: tan, asin, acos, atan") {
    assertFn(functions.tan(Column("x")), "tan", 1)
    assertFn(functions.asin(Column("x")), "asin", 1)
    assertFn(functions.acos(Column("x")), "acos", 1)
    assertFn(functions.atan(Column("x")), "atan", 1)
  }

  test("hyperbolic functions: sinh, cosh, tanh") {
    assertFn(functions.sinh(Column("x")), "sinh", 1)
    assertFn(functions.cosh(Column("x")), "cosh", 1)
    assertFn(functions.tanh(Column("x")), "tanh", 1)
  }

  test("rint and signum") {
    assertFn(functions.rint(Column("x")), "rint", 1)
    assertFn(functions.signum(Column("x")), "signum", 1)
  }

  test("degrees and radians") {
    assertFn(functions.degrees(Column("x")), "degrees", 1)
    assertFn(functions.radians(Column("x")), "radians", 1)
  }

  test("bround") {
    assertFn(functions.bround(Column("x")), "bround", 2)
    assertFn(functions.bround(Column("x"), 3), "bround", 2)
  }

  test("unhex") {
    assertFn(functions.unhex(Column("x")), "unhex", 1)
  }

  test("conv") {
    assertFn(functions.conv(Column("x"), 10, 16), "conv", 3)
  }

  test("factorial") {
    assertFn(functions.factorial(Column("x")), "factorial", 1)
  }

  test("inverse hyperbolic: acosh, asinh, atanh") {
    assertFn(functions.acosh(Column("x")), "acosh", 1)
    assertFn(functions.asinh(Column("x")), "asinh", 1)
    assertFn(functions.atanh(Column("x")), "atanh", 1)
  }

  test("reciprocal trig: cot, csc, sec") {
    assertFn(functions.cot(Column("x")), "cot", 1)
    assertFn(functions.csc(Column("x")), "csc", 1)
    assertFn(functions.sec(Column("x")), "sec", 1)
  }

  test("power alias") {
    assertFn(functions.power(Column("a"), Column("b")), "power", 2)
  }

  test("try_mod") {
    assertFn(functions.try_mod(Column("a"), Column("b")), "try_mod", 2)
  }

  test("random") {
    assertFn(functions.random(Column("s")), "random", 1)
    assertFn(functions.random(), "random", 0)
  }

  test("uniform") {
    assertFn(functions.uniform(Column("a"), Column("b")), "uniform", 2)
    assertFn(functions.uniform(Column("a"), Column("b"), Column("s")), "uniform", 3)
  }

  test("toDegrees and toRadians") {
    assertFn(functions.toDegrees(Column("x")), "degrees", 1)
    assertFn(functions.toRadians(Column("x")), "radians", 1)
  }

  // ----- String functions (untested) -----

  test("concat_ws") {
    assertFn(functions.concat_ws(",", Column("a"), Column("b")), "concat_ws", 3)
  }

  test("ltrim and rtrim") {
    assertFn(functions.ltrim(Column("x")), "ltrim", 1)
    assertFn(functions.rtrim(Column("x")), "rtrim", 1)
  }

  test("replace") {
    assertFn(functions.replace(Column("x"), Column("s"), Column("r")), "replace", 3)
  }

  test("lpad and rpad") {
    assertFn(functions.lpad(Column("x"), 10, " "), "lpad", 3)
    assertFn(functions.rpad(Column("x"), 10, " "), "rpad", 3)
  }

  test("substring_index") {
    assertFn(functions.substring_index(Column("x"), ".", 2), "substring_index", 3)
  }

  test("split_part") {
    assertFn(
      functions.split_part(Column("x"), Column("d"), Column("n")),
      "split_part",
      3
    )
  }

  test("find_in_set") {
    assertFn(functions.find_in_set(Column("x"), Column("s")), "find_in_set", 2)
  }

  test("elt") {
    assertFn(functions.elt(Column("n"), Column("a"), Column("b")), "elt", 3)
  }

  test("regexp_count") {
    assertFn(functions.regexp_count(Column("s"), Column("r")), "regexp_count", 2)
  }

  test("regexp_extract_all") {
    assertFn(functions.regexp_extract_all(Column("s"), Column("r")), "regexp_extract_all", 2)
    assertFn(
      functions.regexp_extract_all(Column("s"), Column("r"), Column("i")),
      "regexp_extract_all",
      3
    )
  }

  test("regexp_instr") {
    assertFn(functions.regexp_instr(Column("s"), Column("r")), "regexp_instr", 2)
    assertFn(functions.regexp_instr(Column("s"), Column("r"), Column("i")), "regexp_instr", 3)
  }

  test("regexp_substr") {
    assertFn(functions.regexp_substr(Column("s"), Column("r")), "regexp_substr", 2)
  }

  test("regexp, regexp_like, rlike") {
    assertFn(functions.regexp(Column("s"), Column("r")), "regexp", 2)
    assertFn(functions.regexp_like(Column("s"), Column("r")), "regexp_like", 2)
    assertFn(functions.rlike(Column("s"), Column("r")), "rlike", 2)
  }

  test("like and ilike") {
    assertFn(functions.like(Column("s"), Column("p")), "like", 2)
    assertFn(functions.like(Column("s"), Column("p"), Column("e")), "like", 3)
    assertFn(functions.ilike(Column("s"), Column("p")), "ilike", 2)
    assertFn(functions.ilike(Column("s"), Column("p"), Column("e")), "ilike", 3)
  }

  test("collate and collation") {
    assertFn(functions.collate(Column("x"), "utf8"), "collate", 2)
    assertFn(functions.collation(Column("x")), "collation", 1)
  }

  test("to_binary and try_to_binary") {
    assertFn(functions.to_binary(Column("x"), Column("f")), "to_binary", 2)
    assertFn(functions.to_binary(Column("x")), "to_binary", 1)
    assertFn(functions.try_to_binary(Column("x"), Column("f")), "try_to_binary", 2)
    assertFn(functions.try_to_binary(Column("x")), "try_to_binary", 1)
  }

  test("to_char and to_varchar") {
    assertFn(functions.to_char(Column("x"), Column("f")), "to_char", 2)
    assertFn(functions.to_varchar(Column("x"), Column("f")), "to_varchar", 2)
  }

  test("to_number") {
    assertFn(functions.to_number(Column("x"), Column("f")), "to_number", 2)
  }

  test("mask") {
    assertFn(functions.mask(Column("x")), "mask", 1)
    assertFn(functions.mask(Column("x"), Column("u")), "mask", 2)
    assertFn(functions.mask(Column("x"), Column("u"), Column("l")), "mask", 3)
    assertFn(functions.mask(Column("x"), Column("u"), Column("l"), Column("d")), "mask", 4)
    assertFn(
      functions.mask(Column("x"), Column("u"), Column("l"), Column("d"), Column("o")),
      "mask",
      5
    )
  }

  test("printf") {
    assertFn(functions.printf(Column("f"), Column("a"), Column("b")), "printf", 3)
  }

  test("lcase and ucase") {
    assertFn(functions.lcase(Column("x")), "lcase", 1)
    assertFn(functions.ucase(Column("x")), "ucase", 1)
  }

  test("len") {
    assertFn(functions.len(Column("x")), "len", 1)
  }

  test("levenshtein") {
    assertFn(functions.levenshtein(Column("a"), Column("b")), "levenshtein", 2)
  }

  test("unbase64") {
    assertFn(functions.unbase64(Column("x")), "unbase64", 1)
  }

  test("decode and encode") {
    assertFn(functions.decode(Column("x"), "UTF-8"), "decode", 2)
    assertFn(functions.encode(Column("x"), "UTF-8"), "encode", 2)
  }

  test("format_string") {
    assertFn(functions.format_string("%s-%d", Column("a"), Column("b")), "format_string", 3)
  }

  test("instr") {
    assertFn(functions.instr(Column("x"), "sub"), "instr", 2)
  }

  test("locate") {
    assertFn(functions.locate("sub", Column("x")), "locate", 3)
    assertFn(functions.locate("sub", Column("x"), 5), "locate", 3)
  }

  test("overlay") {
    assertFn(
      functions.overlay(Column("s"), Column("r"), Column("p"), Column("l")),
      "overlay",
      4
    )
  }

  test("translate") {
    assertFn(functions.translate(Column("x"), "abc", "xyz"), "translate", 3)
  }

  test("char and chr") {
    assertFn(functions.char(Column("n")), "char", 1)
    assertFn(functions.chr(Column("n")), "chr", 1)
  }

  test("character_length") {
    assertFn(functions.character_length(Column("x")), "character_length", 1)
  }

  test("substr") {
    assertFn(functions.substr(Column("x"), Column("p"), Column("l")), "substr", 3)
    assertFn(functions.substr(Column("x"), Column("p")), "substr", 2)
  }

  test("randstr") {
    assertFn(functions.randstr(Column("l")), "randstr", 1)
    assertFn(functions.randstr(Column("l"), Column("s")), "randstr", 2)
  }

  test("quote") {
    assertFn(functions.quote(Column("x")), "quote", 1)
  }

  test("is_valid_utf8 and related") {
    assertFn(functions.is_valid_utf8(Column("x")), "is_valid_utf8", 1)
    assertFn(functions.make_valid_utf8(Column("x")), "make_valid_utf8", 1)
    assertFn(functions.validate_utf8(Column("x")), "validate_utf8", 1)
    assertFn(functions.try_validate_utf8(Column("x")), "try_validate_utf8", 1)
  }

  // ----- Date/Time functions (untested) -----

  test("dayofmonth, hour, minute, second") {
    assertFn(functions.dayofmonth(Column("d")), "dayofmonth", 1)
    assertFn(functions.hour(Column("t")), "hour", 1)
    assertFn(functions.minute(Column("t")), "minute", 1)
    assertFn(functions.second(Column("t")), "second", 1)
  }

  test("date_add and date_sub") {
    assertFn(functions.date_add(Column("d"), 5), "date_add", 2)
    assertFn(functions.date_sub(Column("d"), 3), "date_sub", 2)
  }

  test("to_date") {
    assertFn(functions.to_date(Column("x")), "to_date", 1)
    assertFn(functions.to_date(Column("x"), "yyyy-MM-dd"), "to_date", 2)
  }

  test("to_timestamp") {
    assertFn(functions.to_timestamp(Column("x")), "to_timestamp", 1)
    assertFn(functions.to_timestamp(Column("x"), "yyyy-MM-dd"), "to_timestamp", 2)
  }

  test("date_format") {
    assertFn(functions.date_format(Column("d"), "yyyy"), "date_format", 2)
  }

  test("curdate and current_time and localtimestamp") {
    assertFn(functions.curdate(), "curdate", 0)
    assertFn(functions.current_time(), "current_time", 0)
    assertFn(functions.localtimestamp(), "localtimestamp", 0)
  }

  test("date_diff, dateadd, datepart") {
    assertFn(functions.date_diff(Column("a"), Column("b")), "date_diff", 2)
    assertFn(functions.dateadd(Column("d"), Column("n")), "dateadd", 2)
    assertFn(functions.datepart(Column("f"), Column("s")), "datepart", 2)
  }

  test("day") {
    assertFn(functions.day(Column("d")), "day", 1)
  }

  test("dayname and monthname") {
    assertFn(functions.dayname(Column("d")), "dayname", 1)
    assertFn(functions.monthname(Column("d")), "monthname", 1)
  }

  test("weekday") {
    assertFn(functions.weekday(Column("d")), "weekday", 1)
  }

  test("weekofyear") {
    assertFn(functions.weekofyear(Column("d")), "weekofyear", 1)
  }

  test("last_day") {
    assertFn(functions.last_day(Column("d")), "last_day", 1)
  }

  test("next_day") {
    assertFn(functions.next_day(Column("d"), "Mon"), "next_day", 2)
  }

  test("months_between") {
    assertFn(functions.months_between(Column("a"), Column("b")), "months_between", 2)
    assertFn(functions.months_between(Column("a"), Column("b"), true), "months_between", 3)
  }

  test("add_months") {
    assertFn(functions.add_months(Column("d"), 3), "add_months", 2)
  }

  test("from_utc_timestamp and to_utc_timestamp") {
    assertFn(functions.from_utc_timestamp(Column("t"), "UTC"), "from_utc_timestamp", 2)
    assertFn(functions.to_utc_timestamp(Column("t"), "UTC"), "to_utc_timestamp", 2)
  }

  test("window") {
    assertFn(functions.window(Column("t"), "10 minutes"), "window", 2)
    assertFn(functions.window(Column("t"), "10 minutes", "5 minutes"), "window", 3)
  }

  test("date_trunc and trunc") {
    assertFn(functions.date_trunc("month", Column("t")), "date_trunc", 2)
    assertFn(functions.trunc(Column("d"), "month"), "trunc", 2)
  }

  test("make_dt_interval") {
    assertFn(
      functions.make_dt_interval(Column("d"), Column("h"), Column("m"), Column("s")),
      "make_dt_interval",
      4
    )
    assertFn(
      functions.make_dt_interval(Column("d"), Column("h"), Column("m")),
      "make_dt_interval",
      3
    )
    assertFn(functions.make_dt_interval(Column("d"), Column("h")), "make_dt_interval", 2)
    assertFn(functions.make_dt_interval(Column("d")), "make_dt_interval", 1)
    assertFn(functions.make_dt_interval(), "make_dt_interval", 0)
  }

  test("make_interval") {
    assertFn(
      functions.make_interval(
        Column("y"),
        Column("mo"),
        Column("w"),
        Column("d"),
        Column("h"),
        Column("mi"),
        Column("s")
      ),
      "make_interval",
      7
    )
    assertFn(
      functions.make_interval(
        Column("y"),
        Column("mo"),
        Column("w"),
        Column("d"),
        Column("h"),
        Column("mi")
      ),
      "make_interval",
      6
    )
    assertFn(
      functions.make_interval(Column("y"), Column("mo"), Column("w"), Column("d"), Column("h")),
      "make_interval",
      5
    )
    assertFn(
      functions.make_interval(Column("y"), Column("mo"), Column("w"), Column("d")),
      "make_interval",
      4
    )
    assertFn(functions.make_interval(Column("y"), Column("mo"), Column("w")), "make_interval", 3)
    assertFn(functions.make_interval(Column("y"), Column("mo")), "make_interval", 2)
    assertFn(functions.make_interval(Column("y")), "make_interval", 1)
    assertFn(functions.make_interval(), "make_interval", 0)
  }

  test("make_time") {
    assertFn(functions.make_time(Column("h"), Column("m"), Column("s")), "make_time", 3)
  }

  test("make_ym_interval") {
    assertFn(functions.make_ym_interval(Column("y"), Column("m")), "make_ym_interval", 2)
    assertFn(functions.make_ym_interval(Column("y")), "make_ym_interval", 1)
    assertFn(functions.make_ym_interval(), "make_ym_interval", 0)
  }

  test("make_timestamp_ltz") {
    assertFn(
      functions.make_timestamp_ltz(
        Column("y"),
        Column("mo"),
        Column("d"),
        Column("h"),
        Column("mi"),
        Column("s"),
        Column("tz")
      ),
      "make_timestamp_ltz",
      7
    )
    assertFn(
      functions.make_timestamp_ltz(
        Column("y"),
        Column("mo"),
        Column("d"),
        Column("h"),
        Column("mi"),
        Column("s")
      ),
      "make_timestamp_ltz",
      6
    )
  }

  test("make_timestamp_ntz") {
    assertFn(
      functions.make_timestamp_ntz(
        Column("y"),
        Column("mo"),
        Column("d"),
        Column("h"),
        Column("mi"),
        Column("s")
      ),
      "make_timestamp_ntz",
      6
    )
    assertFn(
      functions.make_timestamp_ntz(Column("d"), Column("t")),
      "make_timestamp_ntz",
      2
    )
  }

  test("to_timestamp_ltz and to_timestamp_ntz") {
    assertFn(functions.to_timestamp_ltz(Column("t"), Column("f")), "to_timestamp_ltz", 2)
    assertFn(functions.to_timestamp_ltz(Column("t")), "to_timestamp_ltz", 1)
    assertFn(functions.to_timestamp_ntz(Column("t"), Column("f")), "to_timestamp_ntz", 2)
    assertFn(functions.to_timestamp_ntz(Column("t")), "to_timestamp_ntz", 1)
  }

  test("to_unix_timestamp") {
    assertFn(functions.to_unix_timestamp(Column("t"), Column("f")), "to_unix_timestamp", 2)
    assertFn(functions.to_unix_timestamp(Column("t")), "to_unix_timestamp", 1)
  }

  test("unix_date, unix_micros, unix_millis, unix_seconds") {
    assertFn(functions.unix_date(Column("d")), "unix_date", 1)
    assertFn(functions.unix_micros(Column("t")), "unix_micros", 1)
    assertFn(functions.unix_millis(Column("t")), "unix_millis", 1)
    assertFn(functions.unix_seconds(Column("t")), "unix_seconds", 1)
  }

  test("convert_timezone") {
    assertFn(
      functions.convert_timezone(Column("s"), Column("t"), Column("ts")),
      "convert_timezone",
      3
    )
    assertFn(functions.convert_timezone(Column("t"), Column("ts")), "convert_timezone", 2)
  }

  test("make_timestamp overloads") {
    assertFn(
      functions.make_timestamp(
        Column("y"),
        Column("mo"),
        Column("d"),
        Column("h"),
        Column("mi"),
        Column("s"),
        Column("tz")
      ),
      "make_timestamp",
      7
    )
    assertFn(
      functions.make_timestamp(Column("d"), Column("t"), Column("tz")),
      "make_timestamp",
      3
    )
    assertFn(functions.make_timestamp(Column("d"), Column("t")), "make_timestamp", 2)
  }

  test("to_time") {
    assertFn(functions.to_time(Column("s")), "to_time", 1)
    assertFn(functions.to_time(Column("s"), Column("f")), "to_time", 2)
  }

  test("time functions") {
    assertFn(functions.time_diff(Column("u"), Column("s"), Column("e")), "time_diff", 3)
    assertFn(functions.time_from_micros(Column("x")), "time_from_micros", 1)
    assertFn(functions.time_from_millis(Column("x")), "time_from_millis", 1)
    assertFn(functions.time_from_seconds(Column("x")), "time_from_seconds", 1)
    assertFn(functions.time_to_micros(Column("x")), "time_to_micros", 1)
    assertFn(functions.time_to_millis(Column("x")), "time_to_millis", 1)
    assertFn(functions.time_to_seconds(Column("x")), "time_to_seconds", 1)
    assertFn(functions.time_trunc(Column("u"), Column("t")), "time_trunc", 2)
  }

  test("timestamp_add and timestamp_diff") {
    assertFn(
      functions.timestamp_add("HOUR", Column("q"), Column("t")),
      "timestamp_add",
      3
    )
    assertFn(
      functions.timestamp_diff("DAY", Column("s"), Column("e")),
      "timestamp_diff",
      3
    )
  }

  test("try_to_date") {
    assertFn(functions.try_to_date(Column("x")), "try_to_date", 1)
    assertFn(functions.try_to_date(Column("x"), "yyyy-MM-dd"), "try_to_date", 2)
  }

  test("try_to_time") {
    assertFn(functions.try_to_time(Column("x")), "try_to_time", 1)
    assertFn(functions.try_to_time(Column("x"), Column("f")), "try_to_time", 2)
  }

  test("try_make_interval") {
    assertFn(
      functions.try_make_interval(
        Column("y"),
        Column("mo"),
        Column("w"),
        Column("d"),
        Column("h"),
        Column("mi"),
        Column("s")
      ),
      "try_make_interval",
      7
    )
    assertFn(functions.try_make_interval(Column("y")), "try_make_interval", 1)
  }

  test("try_make_timestamp") {
    assertFn(
      functions.try_make_timestamp(
        Column("y"),
        Column("mo"),
        Column("d"),
        Column("h"),
        Column("mi"),
        Column("s"),
        Column("tz")
      ),
      "try_make_timestamp",
      7
    )
    assertFn(
      functions.try_make_timestamp(
        Column("y"),
        Column("mo"),
        Column("d"),
        Column("h"),
        Column("mi"),
        Column("s")
      ),
      "try_make_timestamp",
      6
    )
    assertFn(
      functions.try_make_timestamp(Column("d"), Column("t"), Column("tz")),
      "try_make_timestamp",
      3
    )
    assertFn(functions.try_make_timestamp(Column("d"), Column("t")), "try_make_timestamp", 2)
  }

  test("try_make_timestamp_ltz") {
    assertFn(
      functions.try_make_timestamp_ltz(
        Column("y"),
        Column("mo"),
        Column("d"),
        Column("h"),
        Column("mi"),
        Column("s"),
        Column("tz")
      ),
      "try_make_timestamp_ltz",
      7
    )
    assertFn(
      functions.try_make_timestamp_ltz(
        Column("y"),
        Column("mo"),
        Column("d"),
        Column("h"),
        Column("mi"),
        Column("s")
      ),
      "try_make_timestamp_ltz",
      6
    )
  }

  test("try_make_timestamp_ntz") {
    assertFn(
      functions.try_make_timestamp_ntz(
        Column("y"),
        Column("mo"),
        Column("d"),
        Column("h"),
        Column("mi"),
        Column("s")
      ),
      "try_make_timestamp_ntz",
      6
    )
    assertFn(
      functions.try_make_timestamp_ntz(Column("d"), Column("t")),
      "try_make_timestamp_ntz",
      2
    )
  }

  test("session_window") {
    assertFn(functions.session_window(Column("t"), "10 minutes"), "session_window", 2)
    assertFn(functions.session_window(Column("t"), Column("g")), "session_window", 2)
  }

  test("window_time") {
    assertFn(functions.window_time(Column("w")), "window_time", 1)
  }

  test("from_unixtime with format") {
    assertFn(functions.from_unixtime(Column("t"), "yyyy-MM-dd"), "from_unixtime", 2)
  }

  test("unix_timestamp with column") {
    assertFn(functions.unix_timestamp(Column("t")), "unix_timestamp", 1)
    assertFn(functions.unix_timestamp(Column("t"), "yyyy-MM-dd"), "unix_timestamp", 2)
  }

  // ----- Conditional / Null functions (untested) -----

  test("coalesce") {
    assertFn(functions.coalesce(Column("a"), Column("b"), Column("c")), "coalesce", 3)
  }

  test("when standalone function") {
    assertFn(functions.when(Column("cond"), 42), "when", 2)
    assertFn(functions.when(Column("cond"), Column("v")), "when", 2)
  }

  test("ifnull, nullif, nvl, nvl2") {
    assertFn(functions.ifnull(Column("a"), Column("b")), "ifnull", 2)
    assertFn(functions.nullif(Column("a"), Column("b")), "nullif", 2)
    assertFn(functions.nvl(Column("a"), Column("b")), "nvl", 2)
    assertFn(functions.nvl2(Column("a"), Column("b"), Column("c")), "nvl2", 3)
  }

  test("nanvl") {
    assertFn(functions.nanvl(Column("a"), Column("b")), "nanvl", 2)
  }

  test("nullifzero and zeroifnull") {
    assertFn(functions.nullifzero(Column("x")), "nullifzero", 1)
    assertFn(functions.zeroifnull(Column("x")), "zeroifnull", 1)
  }

  test("equal_null") {
    assertFn(functions.equal_null(Column("a"), Column("b")), "equal_null", 2)
  }

  // ----- Collection functions (untested) -----

  test("array_contains") {
    assertFn(functions.array_contains(Column("arr"), 1), "array_contains", 2)
  }

  test("array_except") {
    assertFn(functions.array_except(Column("a"), Column("b")), "array_except", 2)
  }

  test("array_join") {
    assertFn(functions.array_join(Column("a"), ","), "array_join", 2)
    assertFn(functions.array_join(Column("a"), ",", "null"), "array_join", 3)
  }

  test("array_max and array_min") {
    assertFn(functions.array_max(Column("arr")), "array_max", 1)
    assertFn(functions.array_min(Column("arr")), "array_min", 1)
  }

  test("array_position") {
    assertFn(functions.array_position(Column("arr"), 5), "array_position", 2)
  }

  test("array_remove") {
    assertFn(functions.array_remove(Column("arr"), 5), "array_remove", 2)
  }

  test("array_repeat") {
    assertFn(functions.array_repeat(Column("x"), 3), "array_repeat", 2)
  }

  test("arrays_zip") {
    assertFn(functions.arrays_zip(Column("a"), Column("b")), "arrays_zip", 2)
  }

  test("slice") {
    assertFn(functions.slice(Column("arr"), 1, 3), "slice", 3)
  }

  test("shuffle") {
    assertFn(functions.shuffle(Column("arr")), "shuffle", 1)
  }

  test("sort_array") {
    assertFn(functions.sort_array(Column("arr")), "sort_array", 2)
    assertFn(functions.sort_array(Column("arr"), false), "sort_array", 2)
  }

  test("try_element_at") {
    assertFn(functions.try_element_at(Column("arr"), Column("i")), "try_element_at", 2)
  }

  test("named_struct") {
    assertFn(
      functions.named_struct(Column.lit("a"), Column("x"), Column.lit("b"), Column("y")),
      "named_struct",
      4
    )
  }

  test("map function") {
    assertFn(functions.map(Column("k"), Column("v")), "map", 2)
  }

  test("map_from_entries") {
    assertFn(functions.map_from_entries(Column("x")), "map_from_entries", 1)
  }

  test("map_concat") {
    assertFn(functions.map_concat(Column("a"), Column("b")), "map_concat", 2)
  }

  test("cardinality") {
    assertFn(functions.cardinality(Column("x")), "cardinality", 1)
  }

  // ----- JSON functions (untested) -----

  test("json_tuple") {
    assertFn(functions.json_tuple(Column("j"), "a", "b"), "json_tuple", 3)
  }

  test("schema_of_json") {
    assertFn(functions.schema_of_json("{\"a\":1}"), "schema_of_json", 1)
  }

  test("json_array_length") {
    assertFn(functions.json_array_length(Column("j")), "json_array_length", 1)
  }

  test("json_object_keys") {
    assertFn(functions.json_object_keys(Column("j")), "json_object_keys", 1)
  }

  // ----- XML functions (untested) -----

  test("from_xml and to_xml") {
    assertFn(functions.from_xml(Column("x"), Column("s")), "from_xml", 2)
    assertFn(functions.to_xml(Column("x")), "to_xml", 1)
  }

  test("schema_of_xml") {
    assertFn(functions.schema_of_xml(Column("x")), "schema_of_xml", 1)
    assertFn(functions.schema_of_xml("<a>1</a>"), "schema_of_xml", 1)
  }

  test("xpath_boolean, xpath_double, xpath_float, xpath_int, xpath_long, xpath_short") {
    assertFn(functions.xpath_boolean(Column("x"), Column("p")), "xpath_boolean", 2)
    assertFn(functions.xpath_double(Column("x"), Column("p")), "xpath_double", 2)
    assertFn(functions.xpath_number(Column("x"), Column("p")), "xpath_number", 2)
    assertFn(functions.xpath_float(Column("x"), Column("p")), "xpath_float", 2)
    assertFn(functions.xpath_int(Column("x"), Column("p")), "xpath_int", 2)
    assertFn(functions.xpath_long(Column("x"), Column("p")), "xpath_long", 2)
    assertFn(functions.xpath_short(Column("x"), Column("p")), "xpath_short", 2)
  }

  // ----- URL functions -----

  test("url_encode and url_decode") {
    assertFn(functions.url_encode(Column("x")), "url_encode", 1)
    assertFn(functions.url_decode(Column("x")), "url_decode", 1)
    assertFn(functions.try_url_decode(Column("x")), "try_url_decode", 1)
  }

  test("parse_url") {
    assertFn(functions.parse_url(Column("u"), Column("p"), Column("k")), "parse_url", 3)
    assertFn(functions.parse_url(Column("u"), Column("p")), "parse_url", 2)
  }

  test("try_parse_url") {
    assertFn(functions.try_parse_url(Column("u"), Column("p"), Column("k")), "try_parse_url", 3)
    assertFn(functions.try_parse_url(Column("u"), Column("p")), "try_parse_url", 2)
  }

  // ----- Variant functions -----

  test("parse_json and try_parse_json") {
    assertFn(functions.parse_json(Column("j")), "parse_json", 1)
    assertFn(functions.try_parse_json(Column("j")), "try_parse_json", 1)
  }

  test("variant_get") {
    assertFn(functions.variant_get(Column("v"), "$.a", "string"), "variant_get", 3)
    assertFn(functions.variant_get(Column("v"), Column("p"), "string"), "variant_get", 3)
  }

  test("try_variant_get") {
    assertFn(functions.try_variant_get(Column("v"), "$.a", "string"), "try_variant_get", 3)
    assertFn(functions.try_variant_get(Column("v"), Column("p"), "string"), "try_variant_get", 3)
  }

  test("is_variant_null") {
    assertFn(functions.is_variant_null(Column("v")), "is_variant_null", 1)
  }

  test("schema_of_variant and schema_of_variant_agg") {
    assertFn(functions.schema_of_variant(Column("v")), "schema_of_variant", 1)
    assertFn(functions.schema_of_variant_agg(Column("v")), "schema_of_variant_agg", 1)
  }

  test("to_variant_object") {
    assertFn(functions.to_variant_object(Column("x")), "to_variant_object", 1)
  }

  // ----- Misc functions (untested) -----

  test("input_file_name") {
    assertFn(functions.input_file_name(), "input_file_name", 0)
  }

  test("typedLit delegates to Column.lit") {
    val c = functions.typedLit(42)
    c.expr.hasLiteral shouldBe true
  }

  test("typedlit delegates to Column.lit") {
    val c = functions.typedlit("hello")
    c.expr.hasLiteral shouldBe true
  }

  test("negate") {
    assertFn(functions.negate(Column("x")), "negative", 1)
  }

  test("not function") {
    assertFn(functions.not(Column("x")), "not", 1)
  }

  test("bitwiseNOT") {
    assertFn(functions.bitwiseNOT(Column("x")), "~", 1)
  }

  test("bitwise_not") {
    assertFn(functions.bitwise_not(Column("x")), "~", 1)
  }

  test("xxhash64") {
    assertFn(functions.xxhash64(Column("a"), Column("b")), "xxhash64", 2)
  }

  test("sha2") {
    assertFn(functions.sha2(Column("x"), 256), "sha2", 2)
  }

  test("sha") {
    assertFn(functions.sha(Column("x")), "sha", 1)
  }

  test("ntile") {
    assertFn(functions.ntile(4), "ntile", 1)
  }

  test("call_function") {
    assertFn(functions.call_function("my_fn", Column("a"), Column("b")), "my_fn", 2)
  }

  test("call_udf and callUDF") {
    assertFn(functions.call_udf("my_udf", Column("a")), "my_udf", 1)
    assertFn(functions.callUDF("my_udf", Column("a")), "my_udf", 1)
  }

  test("user") {
    assertFn(functions.user(), "user", 0)
  }

  test("input_file_block_length and input_file_block_start") {
    assertFn(functions.input_file_block_length(), "input_file_block_length", 0)
    assertFn(functions.input_file_block_start(), "input_file_block_start", 0)
  }

  test("getbit") {
    assertFn(functions.getbit(Column("x"), Column("p")), "getbit", 2)
  }

  test("shiftLeft, shiftRight, shiftRightUnsigned aliases") {
    assertFn(functions.shiftLeft(Column("x"), 2), "shiftleft", 2)
    assertFn(functions.shiftRight(Column("x"), 2), "shiftright", 2)
    assertFn(functions.shiftRightUnsigned(Column("x"), 2), "shiftrightunsigned", 2)
  }

  // ----- Partition transform functions -----

  test("years, months, days, hours partition transforms") {
    assertFn(functions.years(Column("x")), "years", 1)
    assertFn(functions.months(Column("x")), "months", 1)
    assertFn(functions.days(Column("x")), "days", 1)
    assertFn(functions.hours(Column("x")), "hours", 1)
  }

  test("bucket") {
    assertFn(functions.bucket(Column("n"), Column("x")), "bucket", 2)
    assertFn(functions.bucket(10, Column("x")), "bucket", 2)
  }

  // ----- Bitmap functions (non-aggregate) -----

  test("bitmap_bit_position, bitmap_bucket_number, bitmap_count") {
    assertFn(functions.bitmap_bit_position(Column("x")), "bitmap_bit_position", 1)
    assertFn(functions.bitmap_bucket_number(Column("x")), "bitmap_bucket_number", 1)
    assertFn(functions.bitmap_count(Column("x")), "bitmap_count", 1)
  }

  // ----- HLL functions (non-aggregate) -----

  test("hll_sketch_estimate") {
    assertFn(functions.hll_sketch_estimate(Column("x")), "hll_sketch_estimate", 1)
  }

  test("hll_union") {
    assertFn(functions.hll_union(Column("a"), Column("b")), "hll_union", 2)
    assertFn(functions.hll_union(Column("a"), Column("b"), true), "hll_union", 3)
  }

  // ----- AES encrypt/decrypt functions -----

  test("aes_encrypt") {
    assertFn(functions.aes_encrypt(Column("i"), Column("k")), "aes_encrypt", 2)
    assertFn(functions.aes_encrypt(Column("i"), Column("k"), Column("m")), "aes_encrypt", 3)
    assertFn(
      functions.aes_encrypt(Column("i"), Column("k"), Column("m"), Column("p")),
      "aes_encrypt",
      4
    )
    assertFn(
      functions.aes_encrypt(Column("i"), Column("k"), Column("m"), Column("p"), Column("v")),
      "aes_encrypt",
      5
    )
    assertFn(
      functions.aes_encrypt(
        Column("i"),
        Column("k"),
        Column("m"),
        Column("p"),
        Column("v"),
        Column("a")
      ),
      "aes_encrypt",
      6
    )
  }

  test("aes_decrypt") {
    assertFn(functions.aes_decrypt(Column("i"), Column("k")), "aes_decrypt", 2)
    assertFn(functions.aes_decrypt(Column("i"), Column("k"), Column("m")), "aes_decrypt", 3)
    assertFn(
      functions.aes_decrypt(Column("i"), Column("k"), Column("m"), Column("p")),
      "aes_decrypt",
      4
    )
    assertFn(
      functions.aes_decrypt(Column("i"), Column("k"), Column("m"), Column("p"), Column("a")),
      "aes_decrypt",
      5
    )
  }

  test("try_aes_decrypt") {
    assertFn(functions.try_aes_decrypt(Column("i"), Column("k")), "try_aes_decrypt", 2)
    assertFn(
      functions.try_aes_decrypt(Column("i"), Column("k"), Column("m")),
      "try_aes_decrypt",
      3
    )
    assertFn(
      functions.try_aes_decrypt(Column("i"), Column("k"), Column("m"), Column("p")),
      "try_aes_decrypt",
      4
    )
    assertFn(
      functions.try_aes_decrypt(Column("i"), Column("k"), Column("m"), Column("p"), Column("a")),
      "try_aes_decrypt",
      5
    )
  }

  // ----- Reflection functions -----

  test("reflect, try_reflect, java_method") {
    assertFn(functions.reflect(Column("c"), Column("m")), "reflect", 2)
    assertFn(functions.try_reflect(Column("c"), Column("m")), "try_reflect", 2)
    assertFn(functions.java_method(Column("c"), Column("m")), "java_method", 2)
  }

  // ----- Theta sketch functions -----

  test("theta_sketch_agg") {
    assertFn(functions.theta_sketch_agg(Column("x"), Column("k")), "theta_sketch_agg", 2)
    assertFn(functions.theta_sketch_agg(Column("x"), 12), "theta_sketch_agg", 2)
    assertFn(functions.theta_sketch_agg(Column("x")), "theta_sketch_agg", 1)
  }

  test("theta_intersection_agg") {
    assertFn(functions.theta_intersection_agg(Column("x")), "theta_intersection_agg", 1)
  }

  test("theta_union_agg") {
    assertFn(functions.theta_union_agg(Column("x"), Column("k")), "theta_union_agg", 2)
    assertFn(functions.theta_union_agg(Column("x"), 12), "theta_union_agg", 2)
    assertFn(functions.theta_union_agg(Column("x")), "theta_union_agg", 1)
  }

  test("theta_difference, theta_intersection, theta_sketch_estimate, theta_union") {
    assertFn(functions.theta_difference(Column("a"), Column("b")), "theta_difference", 2)
    assertFn(functions.theta_intersection(Column("a"), Column("b")), "theta_intersection", 2)
    assertFn(functions.theta_sketch_estimate(Column("x")), "theta_sketch_estimate", 1)
    assertFn(functions.theta_union(Column("a"), Column("b")), "theta_union", 2)
    assertFn(functions.theta_union(Column("a"), Column("b"), 12), "theta_union", 3)
    assertFn(functions.theta_union(Column("a"), Column("b"), Column("k")), "theta_union", 3)
  }

  // ----- Geospatial functions -----

  test("geospatial functions") {
    assertFn(functions.st_asbinary(Column("g")), "st_asbinary", 1)
    assertFn(functions.st_geogfromwkb(Column("w")), "st_geogfromwkb", 1)
    assertFn(functions.st_geomfromwkb(Column("w")), "st_geomfromwkb", 1)
    assertFn(functions.st_geomfromwkb(Column("w"), Column("s")), "st_geomfromwkb", 2)
    assertFn(functions.st_geomfromwkb(Column("w"), 4326), "st_geomfromwkb", 2)
    assertFn(functions.st_setsrid(Column("g"), Column("s")), "st_setsrid", 2)
    assertFn(functions.st_setsrid(Column("g"), 4326), "st_setsrid", 2)
    assertFn(functions.st_srid(Column("g")), "st_srid", 1)
  }

  // ----- unwrap_udt (internal function) -----

  test("unwrap_udt uses internal function") {
    val c = functions.unwrap_udt(Column("x"))
    c.expr.hasUnresolvedFunction shouldBe true
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "unwrap_udt"
    fn.getIsInternal shouldBe true
    fn.getArgumentsList should have size 1
  }

  // ----- approx_count_distinct with rsd -----

  test("approx_count_distinct with rsd") {
    assertFn(functions.approx_count_distinct(Column("x"), 0.05), "approx_count_distinct", 2)
  }

  // ----- round default scale parameter -----

  test("round with default scale") {
    assertFn(functions.round(Column("x")), "round", 2)
  }

  // ----- regexp_replace Column overload -----

  test("regexp_replace with Column pattern and replacement") {
    assertFn(
      functions.regexp_replace(Column("x"), Column("p"), Column("r")),
      "regexp_replace",
      3
    )
  }

  // ----- filter with 2-arg lambda -----

  test("filter with 2-arg lambda (element, index)") {
    val c = functions.filter(Column("arr"), (x, i) => x + i)
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "filter"
    fn.getArguments(1).hasLambdaFunction shouldBe true
    fn.getArguments(1).getLambdaFunction.getArgumentsList should have size 2
  }

  // ----- reduce functions -----

  test("reduce with finish function") {
    val c = functions.reduce(
      Column("arr"),
      Column.lit(0),
      (acc, x) => acc + x,
      result => result * Column.lit(2)
    )
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "reduce"
    fn.getArgumentsList should have size 4
    fn.getArguments(2).hasLambdaFunction shouldBe true
    fn.getArguments(2).getLambdaFunction.getArgumentsList should have size 2
    fn.getArguments(3).hasLambdaFunction shouldBe true
    fn.getArguments(3).getLambdaFunction.getArgumentsList should have size 1
  }

  test("reduce without finish function") {
    val c = functions.reduce(
      Column("arr"),
      Column.lit(0),
      (acc, x) => acc + x
    )
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "reduce"
    fn.getArgumentsList should have size 3
  }

  // ----- map_zip_with -----

  test("map_zip_with with Column arg") {
    assertFn(
      functions.map_zip_with(Column("a"), Column("b"), Column("f")),
      "map_zip_with",
      3
    )
  }

  // ----- map_filter with Column arg (non-lambda overload) -----

  test("map_filter with Column arg") {
    assertFn(functions.map_filter(Column("m"), Column("f")), "map_filter", 2)
  }

  // ----- try_make_interval remaining overloads -----

  test("try_make_interval all overloads") {
    assertFn(
      functions.try_make_interval(
        Column("y"),
        Column("mo"),
        Column("w"),
        Column("d"),
        Column("h"),
        Column("mi")
      ),
      "try_make_interval",
      6
    )
    assertFn(
      functions.try_make_interval(
        Column("y"),
        Column("mo"),
        Column("w"),
        Column("d"),
        Column("h")
      ),
      "try_make_interval",
      5
    )
    assertFn(
      functions.try_make_interval(Column("y"), Column("mo"), Column("w"), Column("d")),
      "try_make_interval",
      4
    )
    assertFn(
      functions.try_make_interval(Column("y"), Column("mo"), Column("w")),
      "try_make_interval",
      3
    )
    assertFn(
      functions.try_make_interval(Column("y"), Column("mo")),
      "try_make_interval",
      2
    )
  }

  // ----- KLL sketch functions -----

  test("kll_sketch_agg_bigint") {
    assertFn(functions.kll_sketch_agg_bigint(Column("x"), Column("k")), "kll_sketch_agg_bigint", 2)
    assertFn(functions.kll_sketch_agg_bigint(Column("x"), 200), "kll_sketch_agg_bigint", 2)
    assertFn(functions.kll_sketch_agg_bigint(Column("x")), "kll_sketch_agg_bigint", 1)
  }

  test("kll_sketch_agg_float") {
    assertFn(functions.kll_sketch_agg_float(Column("x"), Column("k")), "kll_sketch_agg_float", 2)
    assertFn(functions.kll_sketch_agg_float(Column("x"), 200), "kll_sketch_agg_float", 2)
    assertFn(functions.kll_sketch_agg_float(Column("x")), "kll_sketch_agg_float", 1)
  }

  test("kll_sketch_agg_double") {
    assertFn(functions.kll_sketch_agg_double(Column("x"), Column("k")), "kll_sketch_agg_double", 2)
    assertFn(functions.kll_sketch_agg_double(Column("x"), 200), "kll_sketch_agg_double", 2)
    assertFn(functions.kll_sketch_agg_double(Column("x")), "kll_sketch_agg_double", 1)
  }

  test("kll_merge_agg_bigint") {
    assertFn(functions.kll_merge_agg_bigint(Column("x"), Column("k")), "kll_merge_agg_bigint", 2)
    assertFn(functions.kll_merge_agg_bigint(Column("x"), 200), "kll_merge_agg_bigint", 2)
    assertFn(functions.kll_merge_agg_bigint(Column("x")), "kll_merge_agg_bigint", 1)
  }

  test("kll_merge_agg_float") {
    assertFn(functions.kll_merge_agg_float(Column("x"), Column("k")), "kll_merge_agg_float", 2)
    assertFn(functions.kll_merge_agg_float(Column("x"), 200), "kll_merge_agg_float", 2)
    assertFn(functions.kll_merge_agg_float(Column("x")), "kll_merge_agg_float", 1)
  }

  test("kll_merge_agg_double") {
    assertFn(functions.kll_merge_agg_double(Column("x"), Column("k")), "kll_merge_agg_double", 2)
    assertFn(functions.kll_merge_agg_double(Column("x"), 200), "kll_merge_agg_double", 2)
    assertFn(functions.kll_merge_agg_double(Column("x")), "kll_merge_agg_double", 1)
  }

  test("kll_sketch_get_n") {
    assertFn(functions.kll_sketch_get_n_bigint(Column("x")), "kll_sketch_get_n_bigint", 1)
    assertFn(functions.kll_sketch_get_n_float(Column("x")), "kll_sketch_get_n_float", 1)
    assertFn(functions.kll_sketch_get_n_double(Column("x")), "kll_sketch_get_n_double", 1)
  }

  test("kll_sketch_get_quantile") {
    assertFn(
      functions.kll_sketch_get_quantile_bigint(Column("s"), Column("r")),
      "kll_sketch_get_quantile_bigint",
      2
    )
    assertFn(
      functions.kll_sketch_get_quantile_float(Column("s"), Column("r")),
      "kll_sketch_get_quantile_float",
      2
    )
    assertFn(
      functions.kll_sketch_get_quantile_double(Column("s"), Column("r")),
      "kll_sketch_get_quantile_double",
      2
    )
  }

  test("kll_sketch_get_rank") {
    assertFn(
      functions.kll_sketch_get_rank_bigint(Column("s"), Column("q")),
      "kll_sketch_get_rank_bigint",
      2
    )
    assertFn(
      functions.kll_sketch_get_rank_float(Column("s"), Column("q")),
      "kll_sketch_get_rank_float",
      2
    )
    assertFn(
      functions.kll_sketch_get_rank_double(Column("s"), Column("q")),
      "kll_sketch_get_rank_double",
      2
    )
  }

  test("kll_sketch_merge") {
    assertFn(
      functions.kll_sketch_merge_bigint(Column("a"), Column("b")),
      "kll_sketch_merge_bigint",
      2
    )
    assertFn(
      functions.kll_sketch_merge_float(Column("a"), Column("b")),
      "kll_sketch_merge_float",
      2
    )
    assertFn(
      functions.kll_sketch_merge_double(Column("a"), Column("b")),
      "kll_sketch_merge_double",
      2
    )
  }

  test("kll_sketch_to_string") {
    assertFn(functions.kll_sketch_to_string_bigint(Column("x")), "kll_sketch_to_string_bigint", 1)
    assertFn(functions.kll_sketch_to_string_float(Column("x")), "kll_sketch_to_string_float", 1)
    assertFn(functions.kll_sketch_to_string_double(Column("x")), "kll_sketch_to_string_double", 1)
  }

  // ----- Tuple sketch functions -----

  test("tuple_sketch_agg_double") {
    assertFn(
      functions.tuple_sketch_agg_double(Column("k"), Column("s"), Column("n"), Column("m")),
      "tuple_sketch_agg_double",
      4
    )
    assertFn(
      functions.tuple_sketch_agg_double(Column("k"), Column("s"), 12, "Union"),
      "tuple_sketch_agg_double",
      4
    )
    assertFn(
      functions.tuple_sketch_agg_double(Column("k"), Column("s"), 12),
      "tuple_sketch_agg_double",
      3
    )
    assertFn(
      functions.tuple_sketch_agg_double(Column("k"), Column("s")),
      "tuple_sketch_agg_double",
      2
    )
  }

  test("tuple_sketch_agg_integer") {
    assertFn(
      functions.tuple_sketch_agg_integer(Column("k"), Column("s"), Column("n"), Column("m")),
      "tuple_sketch_agg_integer",
      4
    )
    assertFn(
      functions.tuple_sketch_agg_integer(Column("k"), Column("s"), 12, "Union"),
      "tuple_sketch_agg_integer",
      4
    )
    assertFn(
      functions.tuple_sketch_agg_integer(Column("k"), Column("s"), 12),
      "tuple_sketch_agg_integer",
      3
    )
    assertFn(
      functions.tuple_sketch_agg_integer(Column("k"), Column("s")),
      "tuple_sketch_agg_integer",
      2
    )
  }

  test("tuple_intersection_agg_double") {
    assertFn(
      functions.tuple_intersection_agg_double(Column("x"), Column("m")),
      "tuple_intersection_agg_double",
      2
    )
    assertFn(
      functions.tuple_intersection_agg_double(Column("x"), "Union"),
      "tuple_intersection_agg_double",
      2
    )
    assertFn(
      functions.tuple_intersection_agg_double(Column("x")),
      "tuple_intersection_agg_double",
      1
    )
  }

  test("tuple_intersection_agg_integer") {
    assertFn(
      functions.tuple_intersection_agg_integer(Column("x"), Column("m")),
      "tuple_intersection_agg_integer",
      2
    )
    assertFn(
      functions.tuple_intersection_agg_integer(Column("x"), "Union"),
      "tuple_intersection_agg_integer",
      2
    )
    assertFn(
      functions.tuple_intersection_agg_integer(Column("x")),
      "tuple_intersection_agg_integer",
      1
    )
  }

  test("tuple_union_agg_double") {
    assertFn(
      functions.tuple_union_agg_double(Column("x"), Column("n"), Column("m")),
      "tuple_union_agg_double",
      3
    )
    assertFn(
      functions.tuple_union_agg_double(Column("x"), 12, "Union"),
      "tuple_union_agg_double",
      3
    )
    assertFn(
      functions.tuple_union_agg_double(Column("x"), 12),
      "tuple_union_agg_double",
      2
    )
    assertFn(
      functions.tuple_union_agg_double(Column("x")),
      "tuple_union_agg_double",
      1
    )
  }

  test("tuple_union_agg_integer") {
    assertFn(
      functions.tuple_union_agg_integer(Column("x"), Column("n"), Column("m")),
      "tuple_union_agg_integer",
      3
    )
    assertFn(
      functions.tuple_union_agg_integer(Column("x"), 12, "Union"),
      "tuple_union_agg_integer",
      3
    )
    assertFn(
      functions.tuple_union_agg_integer(Column("x"), 12),
      "tuple_union_agg_integer",
      2
    )
    assertFn(
      functions.tuple_union_agg_integer(Column("x")),
      "tuple_union_agg_integer",
      1
    )
  }

  test("tuple_difference_double and tuple_difference_integer") {
    assertFn(
      functions.tuple_difference_double(Column("a"), Column("b")),
      "tuple_difference_double",
      2
    )
    assertFn(
      functions.tuple_difference_integer(Column("a"), Column("b")),
      "tuple_difference_integer",
      2
    )
  }

  test("tuple_intersection_double") {
    assertFn(
      functions.tuple_intersection_double(Column("a"), Column("b")),
      "tuple_intersection_double",
      2
    )
    assertFn(
      functions.tuple_intersection_double(Column("a"), Column("b"), "Union"),
      "tuple_intersection_double",
      3
    )
    assertFn(
      functions.tuple_intersection_double(Column("a"), Column("b"), Column("m")),
      "tuple_intersection_double",
      3
    )
  }

  test("tuple_intersection_integer") {
    assertFn(
      functions.tuple_intersection_integer(Column("a"), Column("b")),
      "tuple_intersection_integer",
      2
    )
    assertFn(
      functions.tuple_intersection_integer(Column("a"), Column("b"), "Union"),
      "tuple_intersection_integer",
      3
    )
    assertFn(
      functions.tuple_intersection_integer(Column("a"), Column("b"), Column("m")),
      "tuple_intersection_integer",
      3
    )
  }

  test("tuple_difference_theta_double and tuple_difference_theta_integer") {
    assertFn(
      functions.tuple_difference_theta_double(Column("a"), Column("b")),
      "tuple_difference_theta_double",
      2
    )
    assertFn(
      functions.tuple_difference_theta_integer(Column("a"), Column("b")),
      "tuple_difference_theta_integer",
      2
    )
  }

  test("tuple_intersection_theta_double") {
    assertFn(
      functions.tuple_intersection_theta_double(Column("a"), Column("b")),
      "tuple_intersection_theta_double",
      2
    )
    assertFn(
      functions.tuple_intersection_theta_double(Column("a"), Column("b"), "Union"),
      "tuple_intersection_theta_double",
      3
    )
    assertFn(
      functions.tuple_intersection_theta_double(Column("a"), Column("b"), Column("m")),
      "tuple_intersection_theta_double",
      3
    )
  }

  test("tuple_intersection_theta_integer") {
    assertFn(
      functions.tuple_intersection_theta_integer(Column("a"), Column("b")),
      "tuple_intersection_theta_integer",
      2
    )
    assertFn(
      functions.tuple_intersection_theta_integer(Column("a"), Column("b"), "Union"),
      "tuple_intersection_theta_integer",
      3
    )
    assertFn(
      functions.tuple_intersection_theta_integer(Column("a"), Column("b"), Column("m")),
      "tuple_intersection_theta_integer",
      3
    )
  }

  test("tuple_sketch_estimate_double and tuple_sketch_estimate_integer") {
    assertFn(
      functions.tuple_sketch_estimate_double(Column("x")),
      "tuple_sketch_estimate_double",
      1
    )
    assertFn(
      functions.tuple_sketch_estimate_integer(Column("x")),
      "tuple_sketch_estimate_integer",
      1
    )
  }

  test("tuple_sketch_summary_double") {
    assertFn(functions.tuple_sketch_summary_double(Column("x")), "tuple_sketch_summary_double", 1)
    assertFn(
      functions.tuple_sketch_summary_double(Column("x"), "Union"),
      "tuple_sketch_summary_double",
      2
    )
    assertFn(
      functions.tuple_sketch_summary_double(Column("x"), Column("m")),
      "tuple_sketch_summary_double",
      2
    )
  }

  test("tuple_sketch_summary_integer") {
    assertFn(
      functions.tuple_sketch_summary_integer(Column("x")),
      "tuple_sketch_summary_integer",
      1
    )
    assertFn(
      functions.tuple_sketch_summary_integer(Column("x"), "Union"),
      "tuple_sketch_summary_integer",
      2
    )
    assertFn(
      functions.tuple_sketch_summary_integer(Column("x"), Column("m")),
      "tuple_sketch_summary_integer",
      2
    )
  }

  test("tuple_sketch_theta_double and tuple_sketch_theta_integer") {
    assertFn(functions.tuple_sketch_theta_double(Column("x")), "tuple_sketch_theta_double", 1)
    assertFn(functions.tuple_sketch_theta_integer(Column("x")), "tuple_sketch_theta_integer", 1)
  }

  test("tuple_union_double") {
    assertFn(
      functions.tuple_union_double(Column("a"), Column("b")),
      "tuple_union_double",
      2
    )
    assertFn(
      functions.tuple_union_double(Column("a"), Column("b"), 12),
      "tuple_union_double",
      3
    )
    assertFn(
      functions.tuple_union_double(Column("a"), Column("b"), 12, "Union"),
      "tuple_union_double",
      4
    )
    assertFn(
      functions.tuple_union_double(Column("a"), Column("b"), Column("n"), Column("m")),
      "tuple_union_double",
      4
    )
  }

  test("tuple_union_integer") {
    assertFn(
      functions.tuple_union_integer(Column("a"), Column("b")),
      "tuple_union_integer",
      2
    )
    assertFn(
      functions.tuple_union_integer(Column("a"), Column("b"), 12),
      "tuple_union_integer",
      3
    )
    assertFn(
      functions.tuple_union_integer(Column("a"), Column("b"), 12, "Union"),
      "tuple_union_integer",
      4
    )
    assertFn(
      functions.tuple_union_integer(Column("a"), Column("b"), Column("n"), Column("m")),
      "tuple_union_integer",
      4
    )
  }

  test("tuple_union_theta_double") {
    assertFn(
      functions.tuple_union_theta_double(Column("a"), Column("b")),
      "tuple_union_theta_double",
      2
    )
    assertFn(
      functions.tuple_union_theta_double(Column("a"), Column("b"), 12),
      "tuple_union_theta_double",
      3
    )
    assertFn(
      functions.tuple_union_theta_double(Column("a"), Column("b"), 12, "Union"),
      "tuple_union_theta_double",
      4
    )
    assertFn(
      functions.tuple_union_theta_double(Column("a"), Column("b"), Column("n"), Column("m")),
      "tuple_union_theta_double",
      4
    )
  }

  test("tuple_union_theta_integer") {
    assertFn(
      functions.tuple_union_theta_integer(Column("a"), Column("b")),
      "tuple_union_theta_integer",
      2
    )
    assertFn(
      functions.tuple_union_theta_integer(Column("a"), Column("b"), 12),
      "tuple_union_theta_integer",
      3
    )
    assertFn(
      functions.tuple_union_theta_integer(Column("a"), Column("b"), 12, "Union"),
      "tuple_union_theta_integer",
      4
    )
    assertFn(
      functions.tuple_union_theta_integer(Column("a"), Column("b"), Column("n"), Column("m")),
      "tuple_union_theta_integer",
      4
    )
  }

  // ---------- P0 API: String overloads ----------

  test("aggregate string overloads") {
    assertFn(functions.mean("x"), "avg", 1)
    assertFn(functions.collect_list("x"), "collect_list", 1)
    assertFn(functions.collect_set("x"), "collect_set", 1)
    assertFn(functions.kurtosis("x"), "kurtosis", 1)
    assertFn(functions.skewness("x"), "skewness", 1)
    assertFn(functions.stddev("x"), "stddev", 1)
    assertFn(functions.stddev_samp("x"), "stddev_samp", 1)
    assertFn(functions.stddev_pop("x"), "stddev_pop", 1)
    assertFn(functions.variance("x"), "variance", 1)
    assertFn(functions.var_samp("x"), "var_samp", 1)
    assertFn(functions.var_pop("x"), "var_pop", 1)
    assertFn(functions.grouping("x"), "grouping", 1)
  }

  test("sumDistinct string overload") {
    val c = functions.sumDistinct("x")
    c.expr.hasUnresolvedFunction shouldBe true
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "sum"
    fn.getIsDistinct shouldBe true
    fn.getArgumentsList should have size 1
  }

  test("first/last string overloads") {
    assertFn(functions.first("x"), "first", 1)
    assertFn(functions.first("x", ignoreNulls = true), "first", 2)
    assertFn(functions.last("x"), "last", 1)
    assertFn(functions.last("x", ignoreNulls = false), "last", 2)
  }

  test("countDistinct string overload") {
    val c = functions.countDistinct("a", "b", "c")
    c.expr.hasUnresolvedFunction shouldBe true
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "count"
    fn.getIsDistinct shouldBe true
    fn.getArgumentsList should have size 3
  }

  test("grouping_id string overload") {
    assertFn(functions.grouping_id("a", "b"), "grouping_id", 2)
  }

  test("approx_count_distinct string overloads") {
    assertFn(functions.approx_count_distinct("x"), "approx_count_distinct", 1)
    assertFn(functions.approx_count_distinct("x", 0.05), "approx_count_distinct", 2)
  }

  test("math string overloads") {
    assertFn(functions.sqrt("x"), "sqrt", 1)
    assertFn(functions.floor("x"), "floor", 1)
    assertFn(functions.ceil("x"), "ceil", 1)
    assertFn(functions.log("x"), "ln", 1)
    assertFn(functions.log10("x"), "log10", 1)
    assertFn(functions.log2("x"), "log2", 1)
    assertFn(functions.log1p("x"), "log1p", 1)
    assertFn(functions.exp("x"), "exp", 1)
    assertFn(functions.expm1("x"), "expm1", 1)
    assertFn(functions.bin("x"), "bin", 1)
  }

  test("trig string overloads") {
    assertFn(functions.sin("x"), "sin", 1)
    assertFn(functions.cos("x"), "cos", 1)
    assertFn(functions.tan("x"), "tan", 1)
    assertFn(functions.asin("x"), "asin", 1)
    assertFn(functions.acos("x"), "acos", 1)
    assertFn(functions.atan("x"), "atan", 1)
    assertFn(functions.sinh("x"), "sinh", 1)
    assertFn(functions.cosh("x"), "cosh", 1)
    assertFn(functions.tanh("x"), "tanh", 1)
    assertFn(functions.cbrt("x"), "cbrt", 1)
    assertFn(functions.rint("x"), "rint", 1)
    assertFn(functions.signum("x"), "signum", 1)
    assertFn(functions.degrees("x"), "degrees", 1)
    assertFn(functions.radians("x"), "radians", 1)
    assertFn(functions.acosh("x"), "acosh", 1)
    assertFn(functions.asinh("x"), "asinh", 1)
    assertFn(functions.atanh("x"), "atanh", 1)
    assertFn(functions.toDegrees("x"), "degrees", 1)
    assertFn(functions.toRadians("x"), "radians", 1)
  }

  test("collection string overloads") {
    assertFn(functions.array("a", "b", "c"), "array", 3)
    assertFn(functions.struct("a", "b"), "struct", 2)
    assertFn(functions.greatest("a", "b"), "greatest", 2)
    assertFn(functions.least("a", "b", "c"), "least", 3)
  }

  test("lead/lag string overloads") {
    assertFn(functions.lead("x", 2), "lead", 2)
    assertFn(functions.lead("x", 2, 0), "lead", 3)
    assertFn(functions.lag("x", 1), "lag", 2)
    assertFn(functions.lag("x", 1, "default"), "lag", 3)
  }

  test("sketch string overloads") {
    assertFn(functions.hll_sketch_estimate("x"), "hll_sketch_estimate", 1)
    assertFn(functions.hll_sketch_agg("x"), "hll_sketch_agg", 1)
    assertFn(functions.hll_sketch_agg("x", 12), "hll_sketch_agg", 2)
    assertFn(functions.hll_union_agg("x"), "hll_union_agg", 1)
    assertFn(functions.hll_union_agg("x", true), "hll_union_agg", 2)
    assertFn(functions.theta_sketch_estimate("x"), "theta_sketch_estimate", 1)
    assertFn(functions.theta_sketch_agg("x"), "theta_sketch_agg", 1)
    assertFn(functions.theta_sketch_agg("x", 4096), "theta_sketch_agg", 2)
    assertFn(functions.theta_intersection_agg("x"), "theta_intersection_agg", 1)
    assertFn(functions.theta_union_agg("x"), "theta_union_agg", 1)
    assertFn(functions.theta_union_agg("x", 4096), "theta_union_agg", 2)
  }
