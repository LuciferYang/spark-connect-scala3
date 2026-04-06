package org.apache.spark.sql

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.tags.IntegrationTest
import org.apache.spark.sql.types.*

/** Integration tests for typed Dataset operations:
  * flatMap, mapPartitions, reduce, foreach, foreachPartition, joinWith, toJSON, toLocalIterator,
  * as[U], transform, randomSplit.
  */
@IntegrationTest
class DatasetTypedOpsIntegrationSuite extends IntegrationTestBase:

  case class Person(name: String, age: Int) derives Encoder
  case class Dept(name: String, dept: String) derives Encoder

  private def personDs: Dataset[Person] =
    spark.createDataset(Seq(
      Person("Alice", 30),
      Person("Bob", 25),
      Person("Charlie", 35)
    ))

  // ---------------------------------------------------------------------------
  // flatMap
  // ---------------------------------------------------------------------------

  test("flatMap expands each element") {
    assert(classFilesUploaded)
    import Encoder.given
    val result = personDs.flatMap(p => Seq(p.name, p.name.toUpperCase)).collect()
    assert(result.length == 6)
    assert(result.toSet.contains("Alice"))
    assert(result.toSet.contains("ALICE"))
  }

  // ---------------------------------------------------------------------------
  // mapPartitions
  // ---------------------------------------------------------------------------

  test("mapPartitions transforms partitions") {
    assert(classFilesUploaded)
    import Encoder.given
    val result = personDs
      .mapPartitions(iter => iter.map(_.name))
      .collect()
    assert(result.toSet == Set("Alice", "Bob", "Charlie"))
  }

  // ---------------------------------------------------------------------------
  // reduce
  // ---------------------------------------------------------------------------

  test("reduce combines all elements") {
    assert(classFilesUploaded)
    import Encoder.given
    val ds = spark.createDataset(Seq(1, 2, 3, 4, 5))
    val sum = ds.reduce(_ + _)
    assert(sum == 15)
  }

  // ---------------------------------------------------------------------------
  // foreach
  // ---------------------------------------------------------------------------

  test("foreach runs without error") {
    assert(classFilesUploaded)
    // foreach is side-effect only; just verify it completes
    personDs.foreach(_ => ())
  }

  // ---------------------------------------------------------------------------
  // foreachPartition
  // ---------------------------------------------------------------------------

  test("foreachPartition runs without error") {
    assert(classFilesUploaded)
    personDs.foreachPartition((_: Iterator[Person]) => ())
  }

  // ---------------------------------------------------------------------------
  // joinWith
  // ---------------------------------------------------------------------------

  test("joinWith returns typed tuples") {
    assert(classFilesUploaded)
    val depts = spark.createDataset(Seq(
      Dept("Alice", "Eng"),
      Dept("Bob", "Sales")
    ))
    val joined = personDs.joinWith(depts, personDs("name") === depts("name"))
    val result = joined.collect()
    assert(result.length == 2)
    val alice = result.find(_._1.name == "Alice")
    assert(alice.isDefined)
    assert(alice.get._2.dept == "Eng")
  }

  // ---------------------------------------------------------------------------
  // toJSON
  // ---------------------------------------------------------------------------

  test("toJSON returns Dataset[String] with JSON rows") {
    import Encoder.given
    val jsonDs = personDs.toJSON
    val result = jsonDs.collect()
    assert(result.length == 3)
    assert(result.exists(_.contains("Alice")))
  }

  // ---------------------------------------------------------------------------
  // toLocalIterator
  // ---------------------------------------------------------------------------

  test("toLocalIterator returns all elements lazily") {
    val iter = personDs.toLocalIterator()
    try
      val collected = scala.collection.mutable.ArrayBuffer[Person]()
      while iter.hasNext do collected += iter.next()
      assert(collected.length == 3)
      assert(collected.map(_.name).toSet == Set("Alice", "Bob", "Charlie"))
    finally iter.close()
  }

  // ---------------------------------------------------------------------------
  // as[U] type conversion
  // ---------------------------------------------------------------------------

  test("as[Long] converts Dataset type") {
    import Encoder.given
    val ds = spark.range(3).as[Long]
    val result = ds.collect()
    assert(result.toSet == Set(0L, 1L, 2L))
  }

  // ---------------------------------------------------------------------------
  // transform
  // ---------------------------------------------------------------------------

  test("transform applies function to Dataset") {
    import Encoder.given
    val result = personDs.transform(_.filter(_.age > 28))
    assert(result.count() == 2)
  }

  // ---------------------------------------------------------------------------
  // randomSplit
  // ---------------------------------------------------------------------------

  test("randomSplit produces correct number of splits") {
    val splits = personDs.randomSplit(Array(0.5, 0.5), seed = 42L)
    assert(splits.length == 2)
    assert(splits(0).count() + splits(1).count() == 3)
  }

  // ---------------------------------------------------------------------------
  // collectAsList / takeAsList
  // ---------------------------------------------------------------------------

  test("collectAsList returns java List") {
    val list = personDs.collectAsList()
    assert(list.size() == 3)
  }

  test("takeAsList returns limited java List") {
    val list = personDs.takeAsList(2)
    assert(list.size() == 2)
  }

  // ---------------------------------------------------------------------------
  // first / head / take / tail
  // ---------------------------------------------------------------------------

  test("first returns one element") {
    val p = personDs.first()
    assert(p.isInstanceOf[Person])
  }

  test("head returns array of elements") {
    val arr = personDs.head(2)
    assert(arr.length == 2)
  }

  test("tail returns last elements") {
    val arr = personDs.tail(2)
    assert(arr.length == 2)
  }

