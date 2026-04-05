package org.apache.spark.sql.streaming

/** Time mode for stateful processing in transformWithState. */
sealed trait TimeMode:
  def toString: String

object TimeMode:
  case object None extends TimeMode:
    override def toString: String = "None"

  case object ProcessingTime extends TimeMode:
    override def toString: String = "ProcessingTime"

  case object EventTime extends TimeMode:
    override def toString: String = "EventTime"
