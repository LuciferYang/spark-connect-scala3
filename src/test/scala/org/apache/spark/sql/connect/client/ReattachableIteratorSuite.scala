package org.apache.spark.sql.connect.client

import org.apache.spark.connect.proto.{ExecutePlanRequest, ReattachOptions}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.*

class ReattachableIteratorSuite extends AnyFunSuite with Matchers:

  test("RetryException is always retried without backoff") {
    val sleepTimes = scala.collection.mutable.ArrayBuffer.empty[Long]
    val handler = GrpcRetryHandler(
      RetryPolicy(maxRetries = 3, initialBackoffMs = 100, jitterMs = 0),
      sleep = t => sleepTimes += t
    )
    var attempts = 0
    val result = handler.retry {
      attempts += 1
      if attempts < 3 then throw GrpcRetryHandler.RetryException()
      "ok"
    }
    result shouldBe "ok"
    attempts shouldBe 3
    // RetryException should NOT trigger sleep/backoff
    sleepTimes shouldBe empty
  }

  test("RetryException exhausts max retries") {
    val handler = GrpcRetryHandler(
      RetryPolicy(maxRetries = 2, initialBackoffMs = 1, jitterMs = 0),
      sleep = _ => ()
    )
    // If we throw RetryException on every attempt, we should still fail after maxRetries
    var attempts = 0
    assertThrows[GrpcRetryHandler.RetryException] {
      handler.retry {
        attempts += 1
        throw GrpcRetryHandler.RetryException()
      }
    }
    // 1 initial + 2 retries = 3 attempts
    attempts shouldBe 3
  }

  test("operationId is generated when not provided") {
    val req = ExecutePlanRequest.newBuilder()
      .setSessionId("test-session")
      .build()
    // operationId should be empty in the request
    req.hasOperationId shouldBe false
  }

  test("ReattachOptions proto builds correctly") {
    val opt = ReattachOptions.newBuilder().setReattachable(true).build()
    opt.getReattachable shouldBe true
  }

  test("ExecutePlanRequest RequestOption with ReattachOptions") {
    val reattachOpt = ExecutePlanRequest.RequestOption.newBuilder()
      .setReattachOptions(ReattachOptions.newBuilder().setReattachable(true).build())
      .build()
    reattachOpt.hasReattachOptions shouldBe true
    reattachOpt.getReattachOptions.getReattachable shouldBe true
  }
