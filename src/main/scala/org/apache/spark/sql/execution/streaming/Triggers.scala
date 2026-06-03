package org.apache.spark.sql.execution.streaming

import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.streaming.Trigger

private object Triggers:
  private val MicrosPerSecond = 1000L * 1000L
  private val MicrosPerMinute = 60L * MicrosPerSecond
  private val MicrosPerHour = 60L * MicrosPerMinute
  private val MicrosPerDay = 24L * MicrosPerHour

  def validate(intervalMs: Long): Unit =
    require(intervalMs >= 0, "the interval of trigger should not be negative")

  def convert(interval: String): Long =
    val parsed = parseInterval(interval)
    if parsed.months != 0 then
      throw IllegalArgumentException(s"Intervals with year-month fields are not supported: $interval")
    Math.floorDiv(parsed.microseconds, 1000L)

  def convert(interval: Duration): Long =
    require(interval != null, "interval must not be null")
    require(interval.isFinite, "interval must be finite")
    interval.toMillis

  def convert(interval: Long, unit: TimeUnit): Long =
    require(unit != null, "timeUnit must not be null")
    unit.toMillis(interval)

  private case class ParsedInterval(months: Long, microseconds: Long)

  private def parseInterval(interval: String): ParsedInterval =
    require(interval != null && interval.trim.nonEmpty, "interval must not be empty")
    val parts = interval.trim.toLowerCase(Locale.ROOT).split("\\s+").toList
    val tokens = parts match
      case "interval" :: tail => tail
      case other              => other
    if tokens.isEmpty then
      throw IllegalArgumentException(s"Cannot parse interval: $interval")

    try
      parseTokens(tokens)
    catch
      case e: ArithmeticException =>
        throw IllegalArgumentException(s"Cannot parse interval: $interval", e)
      case e: NumberFormatException =>
        throw IllegalArgumentException(s"Cannot parse interval: $interval", e)

  private def parseTokens(tokens: List[String]): ParsedInterval =
    var months = 0L
    var micros = 0L
    var index = 0
    while index < tokens.length do
      val first = tokens(index)
      val (value, unitIndex) =
        if first == "+" || first == "-" then
          if index + 2 >= tokens.length then
            throw new NumberFormatException("invalid interval token count")
          (first + tokens(index + 1), index + 2)
        else
          if index + 1 >= tokens.length then
            throw new NumberFormatException("invalid interval token count")
          (first, index + 1)

      val unit = tokens(unitIndex)
      months = Math.addExact(months, monthsFor(value, unit))
      micros = Math.addExact(micros, microsFor(value, unit))
      index = unitIndex + 1
    ParsedInterval(months, micros)

  private def monthsFor(value: String, unit: String): Long =
    unit match
      case "year" | "years" =>
        Math.multiplyExact(integerValue(value), 12L)
      case "month" | "months" =>
        integerValue(value)
      case _ =>
        0L

  private def microsFor(value: String, unit: String): Long =
    unit match
      case "year" | "years" | "month" | "months" =>
        0L
      case "week" | "weeks" =>
        integerMicros(value, 7L * MicrosPerDay)
      case "day" | "days" =>
        integerMicros(value, MicrosPerDay)
      case "hour" | "hours" =>
        integerMicros(value, MicrosPerHour)
      case "minute" | "minutes" =>
        integerMicros(value, MicrosPerMinute)
      case "second" | "seconds" =>
        secondMicros(value)
      case "millisecond" | "milliseconds" =>
        integerMicros(value, 1000L)
      case "microsecond" | "microseconds" =>
        integerMicros(value, 1L)
      case _ =>
        throw new NumberFormatException(s"invalid interval unit: $unit")

  private def integerValue(value: String): Long =
    if !value.matches("[+-]?\\d+") then
      throw new NumberFormatException(s"invalid interval value: $value")
    value.toLong

  private def integerMicros(value: String, multiplier: Long): Long =
    Math.multiplyExact(integerValue(value), multiplier)

  private def secondMicros(value: String): Long =
    val matcher = "([+-]?)(\\d+)(?:\\.(\\d{1,9}))?".r.pattern.matcher(value)
    if !matcher.matches() then
      throw new NumberFormatException(s"invalid seconds value: $value")
    val negative = matcher.group(1) == "-"
    val seconds = Math.multiplyExact(matcher.group(2).toLong, MicrosPerSecond)
    val fraction = Option(matcher.group(3)).map { raw =>
      (raw + "000000000").substring(0, 9).toLong / 1000L
    }.getOrElse(0L)
    val micros = Math.addExact(seconds, fraction)
    if negative then Math.negateExact(micros) else micros

case object OneTimeTrigger extends Trigger

case object AvailableNowTrigger extends Trigger

case class ProcessingTimeTrigger(intervalMs: Long) extends Trigger:
  Triggers.validate(intervalMs)

object ProcessingTimeTrigger:
  def apply(interval: String): ProcessingTimeTrigger =
    ProcessingTimeTrigger(Triggers.convert(interval))

  def apply(interval: Duration): ProcessingTimeTrigger =
    ProcessingTimeTrigger(Triggers.convert(interval))

  def create(interval: String): ProcessingTimeTrigger =
    apply(interval)

  def create(interval: Long, unit: TimeUnit): ProcessingTimeTrigger =
    ProcessingTimeTrigger(Triggers.convert(interval, unit))

case class ContinuousTrigger(intervalMs: Long) extends Trigger:
  Triggers.validate(intervalMs)

object ContinuousTrigger:
  def apply(interval: String): ContinuousTrigger =
    ContinuousTrigger(Triggers.convert(interval))

  def apply(interval: Duration): ContinuousTrigger =
    ContinuousTrigger(Triggers.convert(interval))

  def create(interval: String): ContinuousTrigger =
    apply(interval)

  def create(interval: Long, unit: TimeUnit): ContinuousTrigger =
    ContinuousTrigger(Triggers.convert(interval, unit))

@Experimental
case class RealTimeTrigger(batchDurationMs: Long) extends Trigger:
  require(batchDurationMs > 0, "the batch duration should not be negative")

@Experimental
object RealTimeTrigger:
  def apply(): RealTimeTrigger =
    RealTimeTrigger(Duration(5, scala.concurrent.duration.MINUTES))

  def apply(batchDuration: String): RealTimeTrigger =
    RealTimeTrigger(Triggers.convert(batchDuration))

  def apply(batchDuration: Duration): RealTimeTrigger =
    RealTimeTrigger(Triggers.convert(batchDuration))

  def create(batchDuration: String): RealTimeTrigger =
    apply(batchDuration)

  def create(batchDuration: Long, unit: TimeUnit): RealTimeTrigger =
    RealTimeTrigger(Triggers.convert(batchDuration, unit))
