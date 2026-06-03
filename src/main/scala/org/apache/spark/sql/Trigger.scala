package org.apache.spark.sql

import org.apache.spark.sql.execution.streaming.*

/** Compatibility facade for the historical root-package Trigger.
  *
  * Prefer [[org.apache.spark.sql.streaming.Trigger]], which matches upstream Spark Connect.
  */
type Trigger = streaming.Trigger

object Trigger:
  type ProcessingTime = ProcessingTimeTrigger

  object ProcessingTime:
    def apply(intervalMs: Long): ProcessingTimeTrigger =
      ProcessingTimeTrigger.create(intervalMs, java.util.concurrent.TimeUnit.MILLISECONDS)

    def apply(
        interval: Long,
        timeUnit: java.util.concurrent.TimeUnit
    ): ProcessingTimeTrigger =
      ProcessingTimeTrigger.create(interval, timeUnit)

    def apply(interval: scala.concurrent.duration.Duration): ProcessingTimeTrigger =
      ProcessingTimeTrigger(interval)

    def apply(interval: String): ProcessingTimeTrigger =
      ProcessingTimeTrigger(interval)

    def unapply(trigger: ProcessingTimeTrigger): Option[Long] =
      Some(trigger.intervalMs)

  def processingTime(intervalMs: Long): ProcessingTimeTrigger =
    ProcessingTime(intervalMs)

  def processingTime(
      interval: Long,
      timeUnit: java.util.concurrent.TimeUnit
  ): ProcessingTimeTrigger =
    ProcessingTime(interval, timeUnit)

  def processingTime(
      interval: scala.concurrent.duration.Duration
  ): ProcessingTimeTrigger =
    ProcessingTime(interval)

  def processingTime(interval: String): ProcessingTimeTrigger =
    ProcessingTime(interval)

  val AvailableNow: streaming.Trigger =
    streaming.Trigger.AvailableNow()

  val Once: streaming.Trigger =
    streaming.Trigger.Once()

  type Continuous = ContinuousTrigger

  object Continuous:
    def apply(intervalMs: Long): ContinuousTrigger =
      ContinuousTrigger(intervalMs)

    def apply(
        interval: Long,
        timeUnit: java.util.concurrent.TimeUnit
    ): ContinuousTrigger =
      ContinuousTrigger.create(interval, timeUnit)

    def apply(interval: scala.concurrent.duration.Duration): ContinuousTrigger =
      ContinuousTrigger(interval)

    def apply(interval: String): ContinuousTrigger =
      ContinuousTrigger(interval)

    def unapply(trigger: ContinuousTrigger): Option[Long] =
      Some(trigger.intervalMs)

  def continuous(intervalMs: Long): ContinuousTrigger =
    Continuous(intervalMs)

  def continuous(
      interval: Long,
      timeUnit: java.util.concurrent.TimeUnit
  ): ContinuousTrigger =
    Continuous(interval, timeUnit)

  def continuous(interval: scala.concurrent.duration.Duration): ContinuousTrigger =
    Continuous(interval)

  def continuous(interval: String): ContinuousTrigger =
    Continuous(interval)

  type RealTime = RealTimeTrigger

  object RealTime:
    def apply(): RealTimeTrigger =
      RealTimeTrigger()

    def apply(batchDurationMs: Long): RealTimeTrigger =
      RealTimeTrigger(batchDurationMs)

    def apply(
        batchDuration: Long,
        timeUnit: java.util.concurrent.TimeUnit
    ): RealTimeTrigger =
      RealTimeTrigger.create(batchDuration, timeUnit)

    def apply(batchDuration: scala.concurrent.duration.Duration): RealTimeTrigger =
      RealTimeTrigger(batchDuration)

    def apply(batchDuration: String): RealTimeTrigger =
      RealTimeTrigger(batchDuration)

    def unapply(trigger: RealTimeTrigger): Option[Long] =
      Some(trigger.batchDurationMs)

  def realTime(): RealTimeTrigger =
    RealTime()

  def realTime(batchDurationMs: Long): RealTimeTrigger =
    RealTime(batchDurationMs)

  def realTime(
      batchDuration: Long,
      timeUnit: java.util.concurrent.TimeUnit
  ): RealTimeTrigger =
    RealTime(batchDuration, timeUnit)

  def realTime(batchDuration: scala.concurrent.duration.Duration): RealTimeTrigger =
    RealTime(batchDuration)

  def realTime(batchDuration: String): RealTimeTrigger =
    RealTime(batchDuration)
