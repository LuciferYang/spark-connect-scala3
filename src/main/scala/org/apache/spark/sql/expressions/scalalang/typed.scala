package org.apache.spark.sql.expressions.scalalang

import org.apache.spark.sql.TypedColumn
import org.apache.spark.sql.internal.{TypedAverage, TypedCount, TypedSumDouble, TypedSumLong}

/** Typed aggregate functions that return [[TypedColumn]]s for use with
  * [[org.apache.spark.sql.KeyValueGroupedDataset.agg]].
  *
  * {{{
  *   import org.apache.spark.sql.expressions.scalalang.typed
  *   ds.groupByKey(_.name).agg(typed.sum[Person](_.age.toDouble))
  * }}}
  */
@deprecated("Use untyped builtin aggregate functions instead.", "3.0.0")
object typed:

  /** Computes the average of `Double` values extracted by `f`. */
  def avg[IN](f: IN => Double): TypedColumn[IN, Double] =
    TypedAverage(f).toColumn

  /** Counts non-null values extracted by `f`. */
  def count[IN](f: IN => Any): TypedColumn[IN, Long] =
    TypedCount(f).toColumn

  /** Sums `Double` values extracted by `f`. */
  def sum[IN](f: IN => Double): TypedColumn[IN, Double] =
    TypedSumDouble(f).toColumn

  /** Sums `Long` values extracted by `f`. */
  def sumLong[IN](f: IN => Long): TypedColumn[IN, Long] =
    TypedSumLong(f).toColumn
