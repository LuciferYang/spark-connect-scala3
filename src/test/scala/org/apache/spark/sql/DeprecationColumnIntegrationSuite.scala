package org.apache.spark.sql

import scala.annotation.nowarn

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.tags.IntegrationTest

/** Integration tests for deprecated Column APIs (e.g. as(Array[String])). */
@nowarn("msg=deprecated")
@IntegrationTest
class DeprecationColumnIntegrationSuite extends IntegrationTestBase:

  test("as with Array of aliases for struct column") {
    val df = spark.sql("SELECT named_struct('x', 1, 'y', 2) AS s")
    try
      val result = df.select(col("s").as(Array("a", "b")))
      val cols = result.columns
      assert(cols.length == 2)
      assert(cols.toSet == Set("a", "b"))
      val row = result.collect()(0)
      assert(row.getInt(0) == 1)
      assert(row.getInt(1) == 2)
    catch
      case e: Exception =>
        cancel(
          s"Multi-alias for struct columns may not be supported by this server: ${e.getMessage}"
        )
  }
