package org.apache.spark.sql

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*

class ObservationSuite extends AnyFunSuite with Matchers:

  test("Observation with name") {
    val obs = Observation("test-metrics")
    obs.name shouldBe "test-metrics"
  }

  test("Observation with random name") {
    val obs = Observation()
    obs.name should not be empty
  }

  test("Observation name must not be empty") {
    an[IllegalArgumentException] should be thrownBy Observation("")
  }

  test("Observation can only be registered once") {
    val obs = Observation("single-use")
    obs.markRegistered()
    an[IllegalArgumentException] should be thrownBy obs.markRegistered()
  }

  test("Observation.setMetrics completes the future") {
    val obs = Observation("future-test")
    obs.future.isCompleted shouldBe false
    val row = Row(42L, "hello")
    obs.setMetrics(row) shouldBe true
    obs.future.isCompleted shouldBe true
  }

  test("Observation.setMetrics only sets once") {
    val obs = Observation("once")
    val row1 = Row(1)
    val row2 = Row(2)
    obs.setMetrics(row1) shouldBe true
    obs.setMetrics(row2) shouldBe false
  }

  test("Observation.get returns metric map") {
    val obs = Observation("get-test")
    val schema = types.StructType(
      Seq(
        types.StructField("count", types.LongType),
        types.StructField("max_id", types.IntegerType)
      )
    )
    val row = Row.fromSeqWithSchema(Seq(100L, 42), schema)
    obs.setMetrics(row)
    val result = obs.get
    result("count") shouldBe 100L
    result("max_id") shouldBe 42
  }

  test("Observation.get returns empty map when no schema") {
    val obs = Observation("no-schema")
    obs.setMetrics(Row(1, 2, 3))
    val result = obs.get
    result shouldBe empty
  }

  test("Observation companion apply methods") {
    val named = Observation("named")
    named.name shouldBe "named"
    val anon = Observation()
    anon.name should not be empty
  }
