package org.apache.spark.sql

import org.apache.spark.connect.proto.Expression

/** A [[Column]] that also holds an [[Encoder]], enabling type-safe aggregation.
  *
  * Instances are created by [[expressions.Aggregator.toColumn]]. The Encoder is used by
  * [[KeyValueGroupedDataset.agg]] to deserialize the aggregation result.
  *
  * @tparam T
  *   The input type of the aggregation (contravariant).
  * @tparam U
  *   The output type of the aggregation.
  */
final class TypedColumn[-T, U] private[sql] (
    expr: Expression,
    private[sql] val encoder: Encoder[U]
) extends Column(expr):

  /** Gives this TypedColumn a name (alias). Returns a new TypedColumn with the alias applied. */
  override def name(alias: String): TypedColumn[T, U] =
    TypedColumn(super.as(alias).expr, encoder)
