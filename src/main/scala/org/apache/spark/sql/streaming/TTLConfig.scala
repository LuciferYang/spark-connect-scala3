package org.apache.spark.sql.streaming

import java.time.Duration

/** TTL (time-to-live) configuration for state variables in transformWithState. */
case class TTLConfig(ttlDuration: Duration)

object TTLConfig:
  val NONE: TTLConfig = TTLConfig(Duration.ZERO)
