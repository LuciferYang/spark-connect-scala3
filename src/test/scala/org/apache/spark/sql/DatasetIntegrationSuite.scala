package org.apache.spark.sql

import org.apache.spark.sql.tags.IntegrationTest

/** Integration tests for typed Dataset[T] operations. */
@IntegrationTest
class DatasetIntegrationSuite extends IntegrationTestBase:

  case class Person(name: String, age: Int) derives Encoder

  test("createDataset and collect typed") {
    val ds = spark.createDataset(Seq(
      Person("Alice", 30),
      Person("Bob", 25)
    ))
    val result = ds.collect()
    assert(result.length == 2)
    assert(result(0) == Person("Alice", 30))
    assert(result(1) == Person("Bob", 25))
  }

  test("Dataset map and filter") {
    assert(classFilesUploaded)
    import Encoder.given
    withLambdaCompat {
      val ds = spark.createDataset(Seq(
        Person("Alice", 30),
        Person("Bob", 25),
        Person("Charlie", 35)
      ))
      val names = ds.filter(_.age > 28).map(_.name)
      val result = names.collect()
      assert(result.toSet == Set("Alice", "Charlie"))
    }
  }

  // The following primitive-typed Dataset tests exercise the
  // top-level adaptor closure path (Problem A). They pass against
  // the Scala 2.13 server because the user lambda only references
  // primitive values, not user-defined case class fields.

  test("Dataset[Int] map adds one") {
    assert(classFilesUploaded)
    val ds = spark.createDataset(Seq(1, 2, 3, 4, 5))
    val result = ds.map(_ + 1).collect().toSet
    assert(result == Set(2, 3, 4, 5, 6))
  }

  test("Dataset[Int] filter > 2") {
    assert(classFilesUploaded)
    val ds = spark.createDataset(Seq(1, 2, 3, 4, 5))
    val result = ds.filter(_ > 2).collect().toSet
    assert(result == Set(3, 4, 5))
  }

  test("Dataset[String] map to length") {
    assert(classFilesUploaded)
    val ds = spark.createDataset(Seq("a", "bb", "ccc"))
    val result = ds.map(_.length).collect().toSet
    assert(result == Set(1, 2, 3))
  }

  test("Dataset[Int] flatMap to range") {
    assert(classFilesUploaded)
    val ds = spark.createDataset(Seq(1, 2, 3))
    val result = ds.flatMap(n => Seq.fill(n)(n)).collect().toList.sorted
    assert(result == List(1, 2, 2, 3, 3, 3))
  }
