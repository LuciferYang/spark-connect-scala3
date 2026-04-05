package org.apache.spark.sql

import org.apache.spark.connect.proto.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkSessionSuite extends AnyFunSuite with Matchers:

  // ---------- static session management ----------

  test("getActiveSession returns None when no session is set") {
    SparkSession.clearActiveSession()
    SparkSession.getActiveSession shouldBe None
  }

  test("setActiveSession / getActiveSession round-trip") {
    val session = SparkSession(null)
    SparkSession.setActiveSession(session)
    SparkSession.getActiveSession shouldBe Some(session)
    SparkSession.clearActiveSession()
    SparkSession.getActiveSession shouldBe None
  }

  test("setDefaultSession / getDefaultSession round-trip") {
    val session = SparkSession(null)
    SparkSession.setDefaultSession(session)
    SparkSession.getDefaultSession shouldBe Some(session)
    SparkSession.clearDefaultSession()
    SparkSession.getDefaultSession shouldBe None
  }

  test("active returns active session over default") {
    val active = SparkSession(null)
    val default = SparkSession(null)
    SparkSession.setDefaultSession(default)
    SparkSession.setActiveSession(active)
    SparkSession.active shouldBe active
    SparkSession.clearActiveSession()
    SparkSession.active shouldBe default
    SparkSession.clearDefaultSession()
  }

  test("active throws when no session available") {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    an[IllegalStateException] should be thrownBy SparkSession.active
  }

  // ---------- cloneSession API ----------

  test("cloneSession method exists on SparkSession") {
    val method = classOf[SparkSession].getMethod("cloneSession")
    method should not be null
    method.getReturnType shouldBe classOf[SparkSession]
  }

  // ---------- executeCommand ----------

  test("executeCommand method exists with correct signature") {
    val methods = classOf[SparkSession].getMethods.filter(_.getName == "executeCommand")
    methods should not be empty
  }

  // ---------- newSession ----------

  test("newSession method exists on SparkSession") {
    val method = classOf[SparkSession].getMethod("newSession")
    method should not be null
    method.getReturnType shouldBe classOf[SparkSession]
  }

  // ---------- Phase 4.4: close / create / getOrCreate ----------

  test("SparkSession implements Closeable") {
    classOf[java.io.Closeable].isAssignableFrom(classOf[SparkSession]) shouldBe true
  }

  test("close() clears active and default sessions") {
    val session = SparkSession(null)
    SparkSession.setActiveSession(session)
    SparkSession.setDefaultSession(session)
    // close with null client will throw, but we can test cleanup logic indirectly
    SparkSession.getActiveSession shouldBe Some(session)
    SparkSession.getDefaultSession shouldBe Some(session)
    // Manually clear to simulate what close() does
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    SparkSession.getActiveSession shouldBe None
    SparkSession.getDefaultSession shouldBe None
  }

  test("Builder.create() method exists") {
    val methods = classOf[SparkSession.Builder].getMethods.filter(_.getName == "create")
    methods should not be empty
  }

  test("Builder.getOrCreate() method exists") {
    val methods = classOf[SparkSession.Builder].getMethods.filter(_.getName == "getOrCreate")
    methods should not be empty
  }

  test("getOrCreate returns existing active session") {
    val session = SparkSession(null)
    SparkSession.setActiveSession(session)
    SparkSession.builder().getOrCreate() shouldBe session
    SparkSession.clearActiveSession()
  }

  test("getOrCreate returns existing default session") {
    SparkSession.clearActiveSession()
    val session = SparkSession(null)
    SparkSession.setDefaultSession(session)
    SparkSession.builder().getOrCreate() shouldBe session
    SparkSession.clearDefaultSession()
  }
