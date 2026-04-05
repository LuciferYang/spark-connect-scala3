package org.apache.spark.sql.internal

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

/** Computes the average of `Double` values extracted by `f`. */
@SerialVersionUID(1L)
private[sql] class TypedAverage[IN](f: IN => Double)
    extends Aggregator[IN, (Double, Long), Double]:

  override def zero: (Double, Long) = (0.0, 0L)

  override def reduce(b: (Double, Long), a: IN): (Double, Long) =
    (b._1 + f(a), b._2 + 1L)

  override def merge(b1: (Double, Long), b2: (Double, Long)): (Double, Long) =
    (b1._1 + b2._1, b1._2 + b2._2)

  override def finish(r: (Double, Long)): Double = r._1 / r._2

  override def bufferEncoder: Encoder[(Double, Long)] =
    Encoders.tuple(Encoders.scalaDouble, Encoders.scalaLong)

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

/** Counts the number of input rows (ignoring the extracted value). */
@SerialVersionUID(1L)
private[sql] class TypedCount[IN](f: IN => Any) extends Aggregator[IN, Long, Long]:

  override def zero: Long = 0L

  override def reduce(b: Long, a: IN): Long =
    if f(a) == null then b else b + 1L

  override def merge(b1: Long, b2: Long): Long = b1 + b2

  override def finish(r: Long): Long = r

  override def bufferEncoder: Encoder[Long] = Encoders.scalaLong

  override def outputEncoder: Encoder[Long] = Encoders.scalaLong

/** Sums `Double` values extracted by `f`. */
@SerialVersionUID(1L)
private[sql] class TypedSumDouble[IN](f: IN => Double)
    extends Aggregator[IN, Double, Double]:

  override def zero: Double = 0.0

  override def reduce(b: Double, a: IN): Double = b + f(a)

  override def merge(b1: Double, b2: Double): Double = b1 + b2

  override def finish(r: Double): Double = r

  override def bufferEncoder: Encoder[Double] = Encoders.scalaDouble

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

/** Sums `Long` values extracted by `f`. */
@SerialVersionUID(1L)
private[sql] class TypedSumLong[IN](f: IN => Long)
    extends Aggregator[IN, Long, Long]:

  override def zero: Long = 0L

  override def reduce(b: Long, a: IN): Long = b + f(a)

  override def merge(b1: Long, b2: Long): Long = b1 + b2

  override def finish(r: Long): Long = r

  override def bufferEncoder: Encoder[Long] = Encoders.scalaLong

  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
