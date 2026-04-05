package org.apache.spark.sql.streaming

import java.io.Serializable

/** Abstract class for stateful processing in transformWithState.
  *
  * Users extend this to implement stateful transformations using state variables
  * (ValueState, ListState, MapState) and timers.
  */
abstract class StatefulProcessor[K, I, O] extends Serializable:
  /** Called when the processor is initialized. Use the handle to create state variables. */
  def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {}

  /** Called for each key with an iterator of input values. */
  def handleInputRows(key: K, inputRows: Iterator[I], timerValues: TimerValues): Iterator[O]

  /** Called when a timer expires. */
  def handleExpiredTimer(
      key: K,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo
  ): Iterator[O] =
    Iterator.empty

  /** Called when the processor is closed. */
  def close(): Unit = {}

/** StatefulProcessor variant that supports initial state. */
abstract class StatefulProcessorWithInitialState[K, I, O, S] extends StatefulProcessor[K, I, O]:
  /** Called for each key in the initial state. */
  def handleInitialState(key: K, initialState: S, timerValues: TimerValues): Unit

/** Placeholder type used when no initial state is provided. */
case class EmptyInitialStateStruct()
