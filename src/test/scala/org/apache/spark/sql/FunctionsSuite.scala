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
