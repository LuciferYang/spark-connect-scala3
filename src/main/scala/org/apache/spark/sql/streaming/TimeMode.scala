package org.apache.spark.sql.streaming

import scala.annotation.targetName

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

  @targetName("None")
  def none(): TimeMode = None

  @targetName("ProcessingTime")
  def processingTime(): TimeMode = ProcessingTime

  @targetName("EventTime")
  def eventTime(): TimeMode = EventTime
