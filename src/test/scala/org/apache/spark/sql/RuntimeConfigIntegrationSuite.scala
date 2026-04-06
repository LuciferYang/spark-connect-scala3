package org.apache.spark.sql

import org.apache.spark.sql.tags.IntegrationTest

/** Integration tests for RuntimeConfig:
  * set/get/getOption/getAll/unset/isModifiable, type overloads.
  */
@IntegrationTest
class RuntimeConfigIntegrationSuite extends IntegrationTestBase:

  private val testKey = "spark.sc3.runtime.config.test"

  override def withFixture(test: NoArgTest) =
    try super.withFixture(test)
    finally
      try spark.conf.unset(testKey)
      catch case _: Exception => ()

  // ---------------------------------------------------------------------------
  // set / get
  // ---------------------------------------------------------------------------

  test("set and get string value") {
    spark.conf.set(testKey, "hello")
    assert(spark.conf.get(testKey) == "hello")
  }

  test("get with default returns default when key missing") {
    val missing = "spark.sc3.does.not.exist.xyz"
    val result = spark.conf.get(missing, "fallback")
    assert(result == "fallback")
  }

  test("get throws when key missing and no default") {
    val missing = "spark.sc3.does.not.exist.xyz"
    intercept[Exception] {
      spark.conf.get(missing)
    }
  }

  // ---------------------------------------------------------------------------
  // set overloads (Boolean, Long)
  // ---------------------------------------------------------------------------

  test("set Boolean overload") {
    spark.conf.set(testKey, true)
    assert(spark.conf.get(testKey) == "true")
  }

  test("set Long overload") {
    spark.conf.set(testKey, 42L)
    assert(spark.conf.get(testKey) == "42")
  }

  // ---------------------------------------------------------------------------
  // getOption
  // ---------------------------------------------------------------------------

  test("getOption returns Some for existing key") {
    spark.conf.set(testKey, "value")
    assert(spark.conf.getOption(testKey) == Some("value"))
  }

  test("getOption returns None for missing key") {
    val missing = "spark.sc3.does.not.exist.xyz"
    assert(spark.conf.getOption(missing).isEmpty)
  }

  // ---------------------------------------------------------------------------
  // getAll
  // ---------------------------------------------------------------------------

  test("getAll returns map containing set key") {
    spark.conf.set(testKey, "abc")
    val all = spark.conf.getAll
    assert(all.isInstanceOf[Map[String, String]])
    assert(all.get(testKey).contains("abc"))
  }

  // ---------------------------------------------------------------------------
  // unset
  // ---------------------------------------------------------------------------

  test("unset removes previously set key") {
    spark.conf.set(testKey, "toRemove")
    assert(spark.conf.getOption(testKey).isDefined)
    spark.conf.unset(testKey)
    assert(spark.conf.getOption(testKey).isEmpty)
  }

  // ---------------------------------------------------------------------------
  // isModifiable
  // ---------------------------------------------------------------------------

  test("isModifiable returns true for runtime-settable config") {
    // spark.sql.shuffle.partitions is a well-known runtime-settable config
    assert(spark.conf.isModifiable("spark.sql.shuffle.partitions"))
  }

  test("isModifiable returns false for static config") {
    // spark.master is not modifiable at runtime
    assert(!spark.conf.isModifiable("spark.master"))
  }
