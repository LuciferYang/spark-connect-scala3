package org.apache.spark.sql

import scala.annotation.nowarn

import org.apache.spark.sql.tags.IntegrationTest

/** Integration tests for deprecated KeyValueGroupedDataset APIs (typed.sumLong). */
@nowarn("msg=deprecated")
@IntegrationTest
class DeprecationKeyValueGroupedDatasetIntegrationSuite extends IntegrationTestBase:

  test("groupByKey.agg with typed column") {
    assert(classFilesUploaded)
    import Encoder.given
    import org.apache.spark.sql.expressions.scalalang.typed
    withLambdaCompat {
      val ds = spark.createDataset(Seq(("a", 1), ("a", 2), ("b", 3)))
      val grouped = ds.groupByKey(_._1)
      val result = grouped.agg(typed.sumLong(_._2.toLong)).collect()
      val map = result.toMap
      assert(map("a") == 3L)
      assert(map("b") == 3L)
    }
  }
