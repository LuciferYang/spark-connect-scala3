package org.apache.spark.sql.connect.client

import com.google.protobuf.GeneratedMessage
import org.apache.spark.connect.proto.{AnalyzePlanResponse, ConfigResponse, ExecutePlanResponse}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ResponseValidatorSuite extends AnyFunSuite with Matchers:

  test("first response stores server-side session ID") {
    val rv = ResponseValidator()
    rv.getServerSideSessionId shouldBe None

    val resp = ExecutePlanResponse.newBuilder()
      .setServerSideSessionId("session-abc")
      .build()
    rv.verifyResponse(resp)
    rv.getServerSideSessionId shouldBe Some("session-abc")
    rv.isSessionValid shouldBe true
  }

  test("matching session ID passes") {
    val rv = ResponseValidator()
    val resp1 = ExecutePlanResponse.newBuilder()
      .setServerSideSessionId("session-abc")
      .build()
    val resp2 = ExecutePlanResponse.newBuilder()
      .setServerSideSessionId("session-abc")
      .build()
    rv.verifyResponse(resp1)
    rv.verifyResponse(resp2)
    rv.isSessionValid shouldBe true
  }

  test("changed session ID throws IllegalStateException") {
    val rv = ResponseValidator()
    val resp1 = ExecutePlanResponse.newBuilder()
      .setServerSideSessionId("session-abc")
      .build()
    val resp2 = ExecutePlanResponse.newBuilder()
      .setServerSideSessionId("session-xyz")
      .build()
    rv.verifyResponse(resp1)
    assertThrows[IllegalStateException] {
      rv.verifyResponse(resp2)
    }
    rv.isSessionValid shouldBe false
  }

  test("response without server_side_session_id is ignored") {
    val rv = ResponseValidator()
    // ConfigResponse does have server_side_session_id field but empty string
    val resp = ConfigResponse.newBuilder().build()
    rv.verifyResponse(resp) // should not throw
    rv.getServerSideSessionId shouldBe None
  }

  test("wrapIterator validates each element") {
    val rv = ResponseValidator()
    val resp1 = ExecutePlanResponse.newBuilder()
      .setServerSideSessionId("session-a")
      .build()
    val resp2 = ExecutePlanResponse.newBuilder()
      .setServerSideSessionId("session-b")
      .build()

    val inner = new Iterator[ExecutePlanResponse] with AutoCloseable:
      private val items = Iterator(resp1, resp2)
      def hasNext: Boolean = items.hasNext
      def next(): ExecutePlanResponse = items.next()
      def close(): Unit = ()

    val wrapped = rv.wrapIterator(inner)
    wrapped.next() // resp1 — stores "session-a"
    assertThrows[IllegalStateException] {
      wrapped.next() // resp2 — "session-b" mismatches
    }
  }

  test("AnalyzePlanResponse also tracked") {
    val rv = ResponseValidator()
    val resp = AnalyzePlanResponse.newBuilder()
      .setServerSideSessionId("session-analyze")
      .build()
    rv.verifyResponse(resp)
    rv.getServerSideSessionId shouldBe Some("session-analyze")
  }
