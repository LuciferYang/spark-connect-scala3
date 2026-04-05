package org.apache.spark.sql.streaming

/** The output mode of a streaming query, describing which rows are written to the sink. */
sealed trait OutputMode:
  def toString: String

object OutputMode:
  case object Append extends OutputMode:
    override def toString: String = "Append"

  case object Update extends OutputMode:
    override def toString: String = "Update"

  case object Complete extends OutputMode:
    override def toString: String = "Complete"
