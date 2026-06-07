package org.apache.spark.sql.expressions.javalang

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.TypedColumn
import org.apache.spark.sql.internal.{TypedAverage, TypedCount, TypedSumDouble, TypedSumLong}

/** Type-safe functions available for Java Dataset operations. */
@deprecated("Use untyped builtin aggregate functions instead.", "3.0.0")
object typed:

  def avg[T](f: MapFunction[T, java.lang.Double]): TypedColumn[T, java.lang.Double] =
    TypedAverage[T](value => f.call(value).doubleValue()).toColumn
      .asInstanceOf[TypedColumn[T, java.lang.Double]]

  def count[T](f: MapFunction[T, Object]): TypedColumn[T, java.lang.Long] =
    TypedCount[T](value => f.call(value)).toColumn
      .asInstanceOf[TypedColumn[T, java.lang.Long]]

  def sum[T](f: MapFunction[T, java.lang.Double]): TypedColumn[T, java.lang.Double] =
    TypedSumDouble[T](value => f.call(value).doubleValue()).toColumn
      .asInstanceOf[TypedColumn[T, java.lang.Double]]

  def sumLong[T](f: MapFunction[T, java.lang.Long]): TypedColumn[T, java.lang.Long] =
    TypedSumLong[T](value => f.call(value).longValue()).toColumn
      .asInstanceOf[TypedColumn[T, java.lang.Long]]
