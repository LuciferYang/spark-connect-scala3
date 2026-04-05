package org.apache.spark.sql

/** Abstract class for writing each element of a streaming query to external storage.
  *
  * {{{
  *   class MyWriter extends ForeachWriter[Row]:
  *     def open(partitionId: Long, epochId: Long): Boolean = true
  *     def process(value: Row): Unit = println(value)
  *     def close(errorOrNull: Throwable): Unit = {}
  * }}}
  */
abstract class ForeachWriter[T] extends Serializable:
  def open(partitionId: Long, epochId: Long): Boolean
  def process(value: T): Unit
  def close(errorOrNull: Throwable): Unit
