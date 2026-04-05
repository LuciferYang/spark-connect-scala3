package org.apache.spark.sql.streaming

/** Timer values available to a StatefulProcessor. */
trait TimerValues:
  def getCurrentProcessingTimeInMs(): Long
  def getCurrentWatermarkInMs(): Long
