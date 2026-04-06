package org.apache.spark.sql

import org.apache.spark.sql.tags.IntegrationTest

/** Integration tests for Structured Streaming. */
@IntegrationTest
class StreamingIntegrationSuite extends IntegrationTestBase:

  test("structured streaming: rate source lifecycle") {
    val df = spark.readStream.format("rate").option("rowsPerSecond", "10").load()
    val query = df.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(1000))
      .start()

    try
      assert(query.id.nonEmpty)
      assert(query.runId.nonEmpty)
      assert(query.isActive)
    finally query.stop()
  }

  test("streams.active lists running queries") {
    val df = spark.readStream.format("rate").option("rowsPerSecond", "1").load()
    val query = df.writeStream
      .format("console")
      .queryName("active_test")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(2000))
      .start()

    try
      val active = spark.streams.active
      assert(active.exists(_.id == query.id))
    finally query.stop()
  }
