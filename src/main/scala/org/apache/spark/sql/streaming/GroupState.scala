package org.apache.spark.sql.streaming

import java.sql.Date
import java.util.concurrent.TimeUnit

/** Wrapper object representing the state for a group in mapGroupsWithState/flatMapGroupsWithState.
  *
  * This is a trait stub — the actual implementation lives on the Spark server side.
  */
trait GroupState[S] extends Serializable:
  def exists: Boolean
  def get: S
  def getOption: Option[S]
  def update(newState: S): Unit
  def remove(): Unit
  def hasTimedOut: Boolean
  def setTimeoutDuration(durationMs: Long): Unit
  def setTimeoutDuration(duration: String): Unit
  def setTimeoutTimestamp(timestampMs: Long): Unit
  def setTimeoutTimestamp(timestampMs: Long, additionalDuration: String): Unit
  def setTimeoutTimestamp(timestamp: Date): Unit
  def setTimeoutTimestamp(timestamp: Date, additionalDuration: String): Unit
  def getCurrentWatermarkMs(): Long
  def getCurrentProcessingTimeMs(): Long
