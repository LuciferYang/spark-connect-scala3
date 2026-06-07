package org.apache.spark.sql;

final class JavaTypedAggregatesCompileTest {
  TypedColumn<Long, Double> avg() {
    return org.apache.spark.sql.expressions.javalang.typed.avg(
        value -> value.doubleValue());
  }

  TypedColumn<Long, Long> count() {
    return org.apache.spark.sql.expressions.javalang.typed.count(
        value -> value);
  }

  TypedColumn<Long, Double> sum() {
    return org.apache.spark.sql.expressions.javalang.typed.sum(
        value -> value.doubleValue());
  }

  TypedColumn<Long, Long> sumLong() {
    return org.apache.spark.sql.expressions.javalang.typed.sumLong(
        value -> value);
  }
}
