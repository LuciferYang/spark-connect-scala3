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
    try intercept[Exception](session.range(200000).collect())
    finally session.stop()
  }
