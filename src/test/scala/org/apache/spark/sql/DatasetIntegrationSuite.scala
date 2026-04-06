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
    val ds = spark.createDataset(Seq(
      Person("Alice", 30),
      Person("Bob", 25),
      Person("Charlie", 35)
    ))
    val names = ds.filter(_.age > 28).map(_.name)
    val result = names.collect()
    assert(result.toSet == Set("Alice", "Charlie"))
  }
