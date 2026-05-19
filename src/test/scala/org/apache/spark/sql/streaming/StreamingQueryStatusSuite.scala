package org.apache.spark.sql.streaming

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.fasterxml.jackson.databind.ObjectMapper

class StreamingQueryStatusSuite extends AnyFunSuite with Matchers:

  test("constructor exposes message, isDataAvailable, isTriggerActive") {
    val s = new StreamingQueryStatus("processing", true, false)
    s.message shouldBe "processing"
    s.isDataAvailable shouldBe true
    s.isTriggerActive shouldBe false
  }

  test("json emits the three fields as a JSON object") {
    val s = new StreamingQueryStatus("waiting", false, true)
    val parsed = new ObjectMapper().readTree(s.json)
    parsed.get("message").asText shouldBe "waiting"
    parsed.get("isDataAvailable").asBoolean shouldBe false
    parsed.get("isTriggerActive").asBoolean shouldBe true
  }

  test("prettyJson contains line breaks and is valid JSON") {
    val s = new StreamingQueryStatus("idle", false, false)
    val pj = s.prettyJson
    pj should include("\n")
    new ObjectMapper().readTree(pj).get("message").asText shouldBe "idle"
  }

  test("toString delegates to prettyJson") {
    val s = new StreamingQueryStatus("running", true, true)
    s.toString shouldBe s.prettyJson
  }

  test("copy returns a new instance with overridden fields") {
    val s = new StreamingQueryStatus("a", true, false)
    val s2 = s.copy(message = "b")
    s2.message shouldBe "b"
    s2.isDataAvailable shouldBe true
    s2.isTriggerActive shouldBe false
    val s3 = s.copy(isDataAvailable = false, isTriggerActive = true)
    s3.message shouldBe "a"
    s3.isDataAvailable shouldBe false
    s3.isTriggerActive shouldBe true
  }
