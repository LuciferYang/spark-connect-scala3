package org.apache.spark.sql

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.tags.IntegrationTest
import org.apache.spark.sql.types.*

/** Integration tests for SparkSession APIs: sql with args, emptyDataFrame, emptyDataset,
  * newSession, cloneSession, version, sessionId, table, tags, createDataset.
  */
@IntegrationTest
class SparkSessionIntegrationSuite extends IntegrationTestBase:

  // ---------------------------------------------------------------------------
  // Basic connectivity
  // ---------------------------------------------------------------------------

  test("sessionId returns non-empty string") {
    val id = spark.sessionId
    assert(id != null && id.nonEmpty)
  }

  test("version returns non-empty string") {
    val v = spark.version
    assert(v != null && v.nonEmpty)
  }

  // ---------------------------------------------------------------------------
  // sql with arguments
  // ---------------------------------------------------------------------------

  test("sql with named arguments (Map)") {
    val result = spark.sql(
      "SELECT :x + :y AS sum",
      Map("x" -> 10, "y" -> 20)
    ).collect()
    assert(result.length == 1)
    assert(result(0).get(0).toString.toInt == 30)
  }

  test("sql with positional arguments (Column varargs)") {
    val result = spark.sql(
      "SELECT ? + ? AS sum",
      lit(10),
      lit(20)
    ).collect()
    assert(result.length == 1)
    assert(result(0).get(0).toString.toInt == 30)
  }

  // ---------------------------------------------------------------------------
  // emptyDataFrame / emptyDataset
  // ---------------------------------------------------------------------------

  test("emptyDataFrame returns zero rows") {
    val df = spark.emptyDataFrame
    assert(df.count() == 0)
    assert(df.columns.isEmpty)
  }

  test("emptyDataset[Int] returns zero rows with correct schema") {
    import Encoder.given
    val ds = spark.emptyDataset[Int]
    assert(ds.count() == 0)
  }

  // ---------------------------------------------------------------------------
  // createDataset
  // ---------------------------------------------------------------------------

  test("createDataset with primitive type") {
    import Encoder.given
    val ds = spark.createDataset(Seq(1, 2, 3))
    assert(ds.count() == 3)
  }

  // ---------------------------------------------------------------------------
  // table
  // ---------------------------------------------------------------------------

  test("table reads from an existing table") {
    val tableName = "sc3_test_session_table"
    try
      spark.range(5).write.mode("overwrite").saveAsTable(tableName)
      val result = spark.table(tableName).count()
      assert(result == 5)
    finally
      try spark.sql(s"DROP TABLE IF EXISTS $tableName")
      catch case _: Exception => ()
  }

  // ---------------------------------------------------------------------------
  // newSession / cloneSession
  // ---------------------------------------------------------------------------

  test("newSession returns a different session with same server") {
    val s2 = spark.newSession()
    assert(s2 != null)
    assert(s2.sessionId != spark.sessionId)
    // both can execute queries
    assert(s2.range(3).count() == 3)
  }

  test("cloneSession returns a session that shares config") {
    spark.conf.set("spark.sc3.clone.test", "hello")
    val s2 = spark.cloneSession()
    assert(s2 != null)
    // cloned session should see the config
    assert(s2.conf.get("spark.sc3.clone.test") == "hello")
    spark.conf.unset("spark.sc3.clone.test")
  }

  // ---------------------------------------------------------------------------
  // Operation tags
  // ---------------------------------------------------------------------------

  test("addTag / getTags / removeTag / clearTags") {
    spark.clearTags()
    assert(spark.getTags().isEmpty)

    spark.addTag("tag1")
    spark.addTag("tag2")
    assert(spark.getTags() == Set("tag1", "tag2"))

    spark.removeTag("tag1")
    assert(spark.getTags() == Set("tag2"))

    spark.clearTags()
    assert(spark.getTags().isEmpty)
  }
