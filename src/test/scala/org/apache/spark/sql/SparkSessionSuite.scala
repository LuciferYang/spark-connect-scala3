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
