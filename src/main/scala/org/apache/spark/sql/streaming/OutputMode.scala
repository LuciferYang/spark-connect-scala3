package org.apache.spark.sql.streaming

import scala.annotation.targetName

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

  @targetName("Append")
  def append(): OutputMode = Append

  @targetName("Update")
  def update(): OutputMode = Update

  @targetName("Complete")
  def complete(): OutputMode = Complete
