package org.apache.spark.sql

import org.apache.spark.sql.tags.IntegrationTest

/** Integration tests for client transport configuration exposed via `SparkSession.Builder`. */
@IntegrationTest
class ClientConfigIntegrationSuite extends IntegrationTestBase:

  test("Builder.maxInboundMessageSize caps inbound gRPC messages") {
    // A tiny inbound limit must cause a large result's Arrow batch to be rejected, proving the
    // limit is applied to the channel — the default 128 MB would let it through. Small control
    // messages (config/schema) stay well under 64 KB, so session setup still succeeds.
    val session = SparkSession
      .builder()
      .remote("sc://localhost:15002")
      .maxInboundMessageSize(64 * 1024)
      .build()
    try
      val ex = intercept[Exception](session.range(200000).collect())
      // Assert the failure is specifically the inbound size cap (gRPC RESOURCE_EXHAUSTED), not an
      // unrelated error such as a connection problem.
      val sb = new StringBuilder
      var cause: Throwable = ex
      var depth = 0
      while cause != null && depth < 50 do
        sb.append(cause.getClass.getSimpleName).append(": ").append(cause.getMessage).append(" | ")
        cause = cause.getCause
        depth += 1
      val chain = sb.toString
      assert(
        chain.contains("RESOURCE_EXHAUSTED") || chain.toLowerCase.contains("exceeds maximum"),
        s"expected an inbound message-size (RESOURCE_EXHAUSTED) failure, but got: $chain"
      )
    finally session.stop()
  }
