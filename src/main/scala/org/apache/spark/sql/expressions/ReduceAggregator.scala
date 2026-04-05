package org.apache.spark.sql.expressions

import org.apache.spark.sql.{Encoder, Encoders}

/** An [[Aggregator]] that reduces values using a binary function.
  *
  * Used internally by [[org.apache.spark.sql.KeyValueGroupedDataset.reduceGroups]] to push the
  * reduction to the server as a typed aggregate expression.
  *
  * The buffer is `(Boolean, T)` where `_1` indicates whether at least one value has been seen.
  * When `_1` is `false`, `_2` is a placeholder and must not be read.
  */
@SerialVersionUID(5066084382969966160L)
private[sql] class ReduceAggregator[T](func: (T, T) => T)(using enc: Encoder[T])
    extends Aggregator[T, (Boolean, T), T]:

  @transient private lazy val _zero: T = null.asInstanceOf[T]

  override def zero: (Boolean, T) = (false, _zero)

  override def reduce(b: (Boolean, T), a: T): (Boolean, T) =
    if b._1 then (true, func(b._2, a))
    else (true, a)

  override def merge(b1: (Boolean, T), b2: (Boolean, T)): (Boolean, T) =
    (b1._1, b2._1) match
      case (false, _) => b2
      case (_, false) => b1
      case _          => (true, func(b1._2, b2._2))

  override def finish(reduction: (Boolean, T)): T =
    if !reduction._1 then throw new IllegalStateException("ReduceAggregator requires at least one input row")
    reduction._2

  override def bufferEncoder: Encoder[(Boolean, T)] =
    Encoders.tuple(Encoders.scalaBoolean, enc)

  override def outputEncoder: Encoder[T] = enc
