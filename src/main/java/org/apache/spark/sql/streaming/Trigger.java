package org.apache.spark.sql.streaming;

import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Duration;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.execution.streaming.AvailableNowTrigger$;
import org.apache.spark.sql.execution.streaming.ContinuousTrigger$;
import org.apache.spark.sql.execution.streaming.OneTimeTrigger$;
import org.apache.spark.sql.execution.streaming.ProcessingTimeTrigger$;
import org.apache.spark.sql.execution.streaming.RealTimeTrigger$;

/** Policy used to indicate how often results should be produced by a streaming query. */
@Evolving
public class Trigger {
  public static Trigger ProcessingTime(long intervalMs) {
    return ProcessingTimeTrigger$.MODULE$.create(intervalMs, TimeUnit.MILLISECONDS);
  }

  public static Trigger ProcessingTime(long interval, TimeUnit timeUnit) {
    return ProcessingTimeTrigger$.MODULE$.create(interval, timeUnit);
  }

  public static Trigger ProcessingTime(Duration interval) {
    return ProcessingTimeTrigger$.MODULE$.apply(interval);
  }

  public static Trigger ProcessingTime(String interval) {
    return ProcessingTimeTrigger$.MODULE$.apply(interval);
  }

  @Deprecated(since = "3.4.0")
  public static Trigger Once() {
    return OneTimeTrigger$.MODULE$;
  }

  public static Trigger AvailableNow() {
    return AvailableNowTrigger$.MODULE$;
  }

  public static Trigger Continuous(long intervalMs) {
    return ContinuousTrigger$.MODULE$.apply(intervalMs);
  }

  public static Trigger Continuous(long interval, TimeUnit timeUnit) {
    return ContinuousTrigger$.MODULE$.create(interval, timeUnit);
  }

  public static Trigger Continuous(Duration interval) {
    return ContinuousTrigger$.MODULE$.apply(interval);
  }

  public static Trigger Continuous(String interval) {
    return ContinuousTrigger$.MODULE$.apply(interval);
  }

  @Experimental
  public static Trigger RealTime(long batchDurationMs) {
    return RealTimeTrigger$.MODULE$.apply(batchDurationMs);
  }

  @Experimental
  public static Trigger RealTime(long batchDuration, TimeUnit timeUnit) {
    return RealTimeTrigger$.MODULE$.create(batchDuration, timeUnit);
  }

  @Experimental
  public static Trigger RealTime(Duration batchDuration) {
    return RealTimeTrigger$.MODULE$.apply(batchDuration);
  }

  @Experimental
  public static Trigger RealTime(String batchDuration) {
    return RealTimeTrigger$.MODULE$.apply(batchDuration);
  }

  @Experimental
  public static Trigger RealTime() {
    return RealTimeTrigger$.MODULE$.apply();
  }
}
