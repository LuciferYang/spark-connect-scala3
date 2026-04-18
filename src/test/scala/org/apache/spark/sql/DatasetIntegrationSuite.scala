package org.apache.spark.sql

import org.apache.spark.sql.tags.IntegrationTest
import org.apache.spark.sql.types.*

/** Integration tests for typed Dataset[T] operations. */
@IntegrationTest
class DatasetIntegrationSuite extends IntegrationTestBase:

  // Person is top-level in EncoderSuite.scala
  // to avoid "inner class" errors on the remote Spark server.

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

  // ---------------------------------------------------------------------------
  // P2: Dataset delegates — unpivot, melt, transpose, lateralJoin
  // ---------------------------------------------------------------------------

  test("unpivot(4-arg) on Dataset") {
    val rows = Seq(Row(1, 10, 20), Row(2, 30, 40))
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("v1", IntegerType),
      StructField("v2", IntegerType)
    ))
    val ds = Dataset(spark.createDataFrame(rows, schema), Encoders.row)
    val result = ds.unpivot(
      Array(Column("id")),
      Array(Column("v1"), Column("v2")),
      "variable",
      "value"
    )
    assert(result.count() == 4)
    assert(result.columns.toSet == Set("id", "variable", "value"))
  }

  test("melt(4-arg) is alias for unpivot") {
    val rows = Seq(Row(1, 10, 20))
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("v1", IntegerType),
      StructField("v2", IntegerType)
    ))
    val ds = Dataset(spark.createDataFrame(rows, schema), Encoders.row)
    val result = ds.melt(
      Array(Column("id")),
      Array(Column("v1"), Column("v2")),
      "variable",
      "value"
    )
    assert(result.count() == 2)
  }

  test("transpose on Dataset") {
    val rows = Seq(Row("r1", 1, 2), Row("r2", 3, 4))
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("a", IntegerType),
      StructField("b", IntegerType)
    ))
    val ds = Dataset(spark.createDataFrame(rows, schema), Encoders.row)
    val result = ds.transpose(Column("name"))
    assert(result.count() > 0)
  }

  test("lateralJoin delegates from Dataset") {
    val ds = spark.createDataset(Seq(1, 2, 3))
    val right = spark.range(1).toDF("x")
    val result = ds.lateralJoin(right)
    assert(result.count() == 3)
  }

  // ---------------------------------------------------------------------------
  // P3d: UnsupportedOp stubs on Dataset
  // ---------------------------------------------------------------------------

  test("rdd throws UnsupportedOperationException") {
    val ds = spark.createDataset(Seq(1, 2, 3))
    assertThrows[UnsupportedOperationException](ds.rdd)
  }

  test("toJavaRDD throws UnsupportedOperationException") {
    val ds = spark.createDataset(Seq(1, 2, 3))
    assertThrows[UnsupportedOperationException](ds.toJavaRDD)
  }

  test("javaRDD throws UnsupportedOperationException") {
    val ds = spark.createDataset(Seq(1, 2, 3))
    assertThrows[UnsupportedOperationException](ds.javaRDD)
  }
