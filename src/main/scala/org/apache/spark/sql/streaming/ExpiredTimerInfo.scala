package org.apache.spark.sql.streaming

/** Information about an expired timer in transformWithState. */
trait ExpiredTimerInfo:
  def getExpiryTimeInMs(): Long
  def isValid(): Boolean
