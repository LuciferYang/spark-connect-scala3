package org.apache.spark.sql.connect.client

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ClientLoggingSuite extends AnyFunSuite with Matchers:

  test("warn routes to the configured handler") {
    val captured = scala.collection.mutable.ArrayBuffer.empty[String]
    ClientLogging.setHandler(line => captured.synchronized(captured += line))
    try ClientLogging.warn("Demo", "boom")
    finally ClientLogging.resetHandler()
    captured should contain("[WARN] [Demo] boom")
  }
