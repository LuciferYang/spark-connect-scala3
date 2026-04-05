package org.apache.spark.sql.streaming

/** Timeout configuration for groups that do not receive data for a while. */
sealed trait GroupStateTimeout:
  def toString: String

object GroupStateTimeout:
  case object NoTimeout extends GroupStateTimeout:
    override def toString: String = "NoTimeout"

  case object ProcessingTimeTimeout extends GroupStateTimeout:
    override def toString: String = "ProcessingTimeTimeout"

  case object EventTimeTimeout extends GroupStateTimeout:
    override def toString: String = "EventTimeTimeout"
