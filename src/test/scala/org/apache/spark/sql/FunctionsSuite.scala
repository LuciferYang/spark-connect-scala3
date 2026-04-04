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
