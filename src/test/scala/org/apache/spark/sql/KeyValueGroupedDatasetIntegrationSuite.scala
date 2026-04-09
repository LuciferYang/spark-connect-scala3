package org.apache.spark.sql

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.tags.IntegrationTest
import org.apache.spark.sql.types.*

/** Integration tests for KeyValueGroupedDataset: groupByKey, keys, count, mapGroups, flatMapGroups,
  * reduceGroups, cogroup, mapValues, flatMapSortedGroups.
  *
  * NOTE: All tests that send Scala 3 lambdas to the server (groupByKey, mapGroups, etc.) will fail
  * with `$deserializeLambda$` when connecting to a Scala 2.13 Spark server. These tests catch that
  * error and cancel gracefully.
  */
@IntegrationTest
class KeyValueGroupedDatasetIntegrationSuite extends IntegrationTestBase:

  // Record and GroupedScore are top-level in TestModels.scala
  // to avoid "inner class" errors on the remote Spark server.
  // Note: Score in EncoderSuite.scala has different fields, so we use GroupedScore here.

  private def recordDs: Dataset[Record] =
    spark.createDataset(Seq(
      Record("A", 10),
      Record("A", 20),
      Record("A", 30),
      Record("B", 40),
      Record("B", 50)
    ))

  // ---------------------------------------------------------------------------
  // groupByKey + keys
  // ---------------------------------------------------------------------------

  test("groupByKey.keys returns distinct keys") {
    assert(classFilesUploaded)
    import Encoder.given
    withLambdaCompat {
      val keys = recordDs.groupByKey(_.group).keys.collect()
      assert(keys.toSet == Set("A", "B"))
    }
  }

  // ---------------------------------------------------------------------------
  // count
  // ---------------------------------------------------------------------------

  test("groupByKey.count returns (key, count) pairs") {
    assert(classFilesUploaded)
    import Encoder.given
    withLambdaCompat {
      val result = recordDs.groupByKey(_.group).count().collect()
      assert(result.toSet == Set(("A", 3L), ("B", 2L)))
    }
  }

  // ---------------------------------------------------------------------------
  // mapGroups
  // ---------------------------------------------------------------------------

  test("mapGroups aggregates per group") {
    assert(classFilesUploaded)
    import Encoder.given
    withLambdaCompat {
      val result = recordDs
        .groupByKey(_.group)
        .mapGroups((key, iter) => (key, iter.map(_.value).sum))
        .collect()
      assert(result.toSet == Set(("A", 60), ("B", 90)))
    }
  }

  // ---------------------------------------------------------------------------
  // flatMapGroups
  // ---------------------------------------------------------------------------

  test("flatMapGroups expands per group") {
    assert(classFilesUploaded)
    import Encoder.given
    withLambdaCompat {
      val result = recordDs
        .groupByKey(_.group)
        .flatMapGroups { (key, iter) =>
          iter.map(r => s"$key:${r.value}")
        }
        .collect()
      assert(result.length == 5)
      assert(result.toSet.contains("A:10"))
      assert(result.toSet.contains("B:50"))
    }
  }

  // ---------------------------------------------------------------------------
  // reduceGroups
  // ---------------------------------------------------------------------------

  test("reduceGroups reduces within each group") {
    assert(classFilesUploaded)
    import Encoder.given
    withLambdaCompat {
      val result = recordDs
        .groupByKey(_.group)
        .mapValues(_.value)
        .reduceGroups(_ + _)
        .collect()
      val map = result.toMap
      assert(map("A") == 60)
      assert(map("B") == 90)
    }
  }

  // ---------------------------------------------------------------------------
  // mapValues
  // ---------------------------------------------------------------------------

  test("mapValues transforms values then mapGroups") {
    assert(classFilesUploaded)
    import Encoder.given
    withLambdaCompat {
      val result = recordDs
        .groupByKey(_.group)
        .mapValues(_.value)
        .mapGroups((key, iter) => (key, iter.sum))
        .collect()
      assert(result.toSet == Set(("A", 60), ("B", 90)))
    }
  }

  // ---------------------------------------------------------------------------
  // flatMapSortedGroups
  // ---------------------------------------------------------------------------

  test("flatMapSortedGroups produces sorted iteration within group") {
    assert(classFilesUploaded)
    import Encoder.given
    withLambdaCompat {
      val result = recordDs
        .groupByKey(_.group)
        .flatMapSortedGroups(col("value").desc) { (key, iter) =>
          val values = iter.map(_.value).toList
          Seq(s"$key:${values.mkString(",")}")
        }
        .collect()
      // A group sorted desc: 30,20,10
      assert(result.toSet.contains("A:30,20,10"), s"Expected descending sort, got: ${result.toSet}")
      assert(result.toSet.contains("B:50,40"))
    }
  }

  // ---------------------------------------------------------------------------
  // cogroup
  // ---------------------------------------------------------------------------

  test("cogroup joins two grouped datasets") {
    assert(classFilesUploaded)
    import Encoder.given
    withLambdaCompat {
      val scores = spark.createDataset(Seq(
        GroupedScore("A", 1.0),
        GroupedScore("B", 2.0)
      ))
      val result = recordDs
        .groupByKey(_.group)
        .cogroup(scores.groupByKey(_.group)) { (key, records, scoresIter) =>
          val sum = records.map(_.value).sum
          val scoreVal = scoresIter.map(_.score).sum
          Seq(s"$key:$sum:$scoreVal")
        }
        .collect()
      assert(result.toSet.contains("A:60:1.0"))
      assert(result.toSet.contains("B:90:2.0"))
    }
  }

  // ---------------------------------------------------------------------------
  // agg with TypedColumn
  // ---------------------------------------------------------------------------

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

  // ---------------------------------------------------------------------------
  // keyAs
  // ---------------------------------------------------------------------------

  test("keyAs changes key encoder type") {
    assert(classFilesUploaded)
    import Encoder.given
    withLambdaCompat {
      val grouped = recordDs.groupByKey(_.group)
      // Re-type the key from String to String (identity cast) – verifies keyAs compiles and runs
      val rekeyed = grouped.keyAs[String]
      val keys = rekeyed.keys.collect()
      assert(keys.toSet == Set("A", "B"))
    }
  }
