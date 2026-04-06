package org.apache.spark.sql

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.tags.IntegrationTest
import org.apache.spark.sql.types.*

/** Integration tests for KeyValueGroupedDataset: groupByKey, keys, count, mapGroups, flatMapGroups,
  * reduceGroups, cogroup, mapValues, flatMapSortedGroups.
  */
@IntegrationTest
class KeyValueGroupedDatasetIntegrationSuite extends IntegrationTestBase:

  case class Record(group: String, value: Int) derives Encoder
  case class Score(group: String, score: Double) derives Encoder

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
    val keys = recordDs.groupByKey(_.group).keys.collect()
    assert(keys.toSet == Set("A", "B"))
  }

  // ---------------------------------------------------------------------------
  // count
  // ---------------------------------------------------------------------------

  test("groupByKey.count returns (key, count) pairs") {
    assert(classFilesUploaded)
    import Encoder.given
    val result = recordDs.groupByKey(_.group).count().collect()
    assert(result.toSet == Set(("A", 3L), ("B", 2L)))
  }

  // ---------------------------------------------------------------------------
  // mapGroups
  // ---------------------------------------------------------------------------

  test("mapGroups aggregates per group") {
    assert(classFilesUploaded)
    import Encoder.given
    val result = recordDs
      .groupByKey(_.group)
      .mapGroups((key, iter) => (key, iter.map(_.value).sum))
      .collect()
    assert(result.toSet == Set(("A", 60), ("B", 90)))
  }

  // ---------------------------------------------------------------------------
  // flatMapGroups
  // ---------------------------------------------------------------------------

  test("flatMapGroups expands per group") {
    assert(classFilesUploaded)
    import Encoder.given
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

  // ---------------------------------------------------------------------------
  // reduceGroups
  // ---------------------------------------------------------------------------

  test("reduceGroups reduces within each group") {
    assert(classFilesUploaded)
    import Encoder.given
    val result = recordDs
      .groupByKey(_.group)
      .mapValues(_.value)
      .reduceGroups(_ + _)
      .collect()
    val map = result.toMap
    assert(map("A") == 60)
    assert(map("B") == 90)
  }

  // ---------------------------------------------------------------------------
  // mapValues
  // ---------------------------------------------------------------------------

  test("mapValues transforms values then mapGroups") {
    assert(classFilesUploaded)
    import Encoder.given
    val result = recordDs
      .groupByKey(_.group)
      .mapValues(_.value)
      .mapGroups((key, iter) => (key, iter.sum))
      .collect()
    assert(result.toSet == Set(("A", 60), ("B", 90)))
  }

  // ---------------------------------------------------------------------------
  // flatMapSortedGroups
  // ---------------------------------------------------------------------------

  test("flatMapSortedGroups produces sorted iteration within group") {
    assert(classFilesUploaded)
    import Encoder.given
    val result = recordDs
      .groupByKey(_.group)
      .flatMapSortedGroups(col("value").desc) { (key, iter) =>
        val values = iter.map(_.value).toList
        Seq(s"$key:${values.mkString(",")}")
      }
      .collect()
    // A group sorted desc: 30,20,10
    assert(result.toSet.contains("A:30,20,10"))
    assert(result.toSet.contains("B:50,40"))
  }

  // ---------------------------------------------------------------------------
  // cogroup
  // ---------------------------------------------------------------------------

  test("cogroup joins two grouped datasets") {
    assert(classFilesUploaded)
    import Encoder.given
    val scores = spark.createDataset(Seq(
      Score("A", 1.0),
      Score("B", 2.0)
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
