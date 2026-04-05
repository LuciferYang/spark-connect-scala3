package org.apache.spark.sql.streaming

import org.apache.spark.sql.Encoder

/** Handle provided to StatefulProcessor for managing state variables and timers.
  *
  * This is a trait stub — the actual implementation lives on the Spark server side.
  */
trait StatefulProcessorHandle:
  def getValueState[T: Encoder](stateName: String): ValueState[T]
  def getValueState[T: Encoder](stateName: String, ttlConfig: TTLConfig): ValueState[T]
  def getListState[T: Encoder](stateName: String): ListState[T]
  def getListState[T: Encoder](stateName: String, ttlConfig: TTLConfig): ListState[T]
  def getMapState[K: Encoder, V: Encoder](stateName: String): MapState[K, V]
  def getMapState[K: Encoder, V: Encoder](
      stateName: String,
      ttlConfig: TTLConfig
  ): MapState[K, V]
  def registerTimer(expiryTimestampMs: Long): Unit
  def deleteTimer(expiryTimestampMs: Long): Unit
  def listTimers(): Iterator[Long]
  def getQueryInfo(): QueryInfo
  def deleteIfExists(stateName: String): Unit
