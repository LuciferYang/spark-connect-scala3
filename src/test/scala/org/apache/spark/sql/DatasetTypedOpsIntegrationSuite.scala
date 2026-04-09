package org.apache.spark.sql

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.tags.IntegrationTest
import org.apache.spark.sql.types.*

/** Integration tests for typed Dataset operations: flatMap, mapPartitions, reduce, foreach,
  * foreachPartition, joinWith, toJSON, toLocalIterator, as[U], transform, randomSplit.
  */
@IntegrationTest
class DatasetTypedOpsIntegrationSuite extends IntegrationTestBase:

  // Person and Dept are top-level in EncoderSuite.scala / TestModels.scala
  // to avoid "inner class" errors on the remote Spark server.

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
    withLambdaCompat {
      val result = personDs.flatMap(p => Seq(p.name, p.name.toUpperCase)).collect()
      assert(result.length == 6)
      assert(result.toSet.contains("Alice"))
      assert(result.toSet.contains("ALICE"))
    }
  }

  // ---------------------------------------------------------------------------
  // mapPartitions
  // ---------------------------------------------------------------------------

  test("mapPartitions transforms partitions") {
    assert(classFilesUploaded)
    import Encoder.given
    withLambdaCompat {
      val result = personDs
        .mapPartitions(iter => iter.map(_.name))
        .collect()
      assert(result.toSet == Set("Alice", "Bob", "Charlie"))
    }
  }

  // ---------------------------------------------------------------------------
  // reduce
  // ---------------------------------------------------------------------------

  test("reduce combines all elements") {
    assert(classFilesUploaded)
    import Encoder.given
    withLambdaCompat {
      val ds = spark.createDataset(Seq(1, 2, 3, 4, 5))
      val sum = ds.reduce(_ + _)
      assert(sum == 15)
    }
  }

  // ---------------------------------------------------------------------------
  // foreach
  // ---------------------------------------------------------------------------

  test("foreach runs without error") {
    assert(classFilesUploaded)
    withLambdaCompat {
      // foreach is side-effect only; just verify it completes
      personDs.foreach(_ => ())
    }
  }

  // ---------------------------------------------------------------------------
  // foreachPartition
  // ---------------------------------------------------------------------------

  test("foreachPartition runs without error") {
    assert(classFilesUploaded)
    withLambdaCompat {
      personDs.foreachPartition((_: Iterator[Person]) => ())
    }
  }

  // ---------------------------------------------------------------------------
  // joinWith
  // ---------------------------------------------------------------------------

  test("joinWith returns typed tuples") {
    assert(classFilesUploaded)
    val persons = personDs
    val depts = spark.createDataset(Seq(
      Dept("Alice", "Eng"),
      Dept("Bob", "Sales")
    ))
    val joined = persons.joinWith(depts, persons("name") === depts("name"))
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
    assert(classFilesUploaded)
    import Encoder.given
    withLambdaCompat {
      val result = personDs.transform(_.filter(_.age > 28))
      assert(result.count() == 2)
    }
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

  // ---------------------------------------------------------------------------
  // typed select with TypedColumn
  // ---------------------------------------------------------------------------

  test("typed select with TypedColumn") {
    import Encoder.given
    val ds = spark.createDataset(Seq(1, 2, 3))
    val result = ds.select(col("value").as[Int])
    assert(result.collect().sorted.toSeq == Seq(1, 2, 3))
  }

  // ---------------------------------------------------------------------------
  // union / unionByName / intersect / except
  // ---------------------------------------------------------------------------

  test("union on typed Dataset") {
    val ds1 = spark.createDataset(Seq(Person("Alice", 30), Person("Bob", 25)))
    val ds2 = spark.createDataset(Seq(Person("Charlie", 35)))
    val result = ds1.union(ds2).collect()
    assert(result.length == 3)
    assert(result.map(_.name).toSet == Set("Alice", "Bob", "Charlie"))
  }

  test("unionByName on typed Dataset") {
    val ds1 = spark.createDataset(Seq(Person("Alice", 30)))
    val ds2 = spark.createDataset(Seq(Person("Bob", 25)))
    val result = ds1.unionByName(ds2).collect()
    assert(result.length == 2)
    assert(result.map(_.name).toSet == Set("Alice", "Bob"))
  }

  test("intersect on typed Dataset") {
    import Encoder.given
    val ds1 = spark.createDataset(Seq(1, 2, 3, 4))
    val ds2 = spark.createDataset(Seq(3, 4, 5, 6))
    val result = ds1.intersect(ds2).collect().sorted
    assert(result.toSeq == Seq(3, 4))
  }

  test("except on typed Dataset") {
    import Encoder.given
    val ds1 = spark.createDataset(Seq(1, 2, 3, 4))
    val ds2 = spark.createDataset(Seq(3, 4, 5, 6))
    val result = ds1.except(ds2).collect().sorted
    assert(result.toSeq == Seq(1, 2))
  }

  // ---------------------------------------------------------------------------
  // distinct / dropDuplicates
  // ---------------------------------------------------------------------------

  test("distinct on typed Dataset") {
    import Encoder.given
    val ds = spark.createDataset(Seq(1, 2, 2, 3, 3, 3))
    val result = ds.distinct().collect().sorted
    assert(result.toSeq == Seq(1, 2, 3))
  }

  test("dropDuplicates on typed Dataset") {
    val ds = spark.createDataset(Seq(
      Person("Alice", 30),
      Person("Alice", 30),
      Person("Bob", 25)
    ))
    val result = ds.dropDuplicates().collect()
    assert(result.length == 2)
    assert(result.map(_.name).toSet == Set("Alice", "Bob"))
  }

  test("dropDuplicates with column names on typed Dataset") {
    val ds = spark.createDataset(Seq(
      Person("Alice", 30),
      Person("Alice", 25),
      Person("Bob", 25)
    ))
    val result = ds.dropDuplicates(Seq("name")).collect()
    assert(result.length == 2)
    assert(result.map(_.name).toSet == Set("Alice", "Bob"))
  }

  // ---------------------------------------------------------------------------
  // sort / orderBy
  // ---------------------------------------------------------------------------

  test("sort on typed Dataset") {
    val ds = personDs
    val result = ds.sort(col("age").asc).collect()
    assert(result(0).name == "Bob") // age 25
    assert(result(2).name == "Charlie") // age 35
  }

  test("orderBy on typed Dataset") {
    val ds = personDs
    val result = ds.orderBy(col("age").desc).collect()
    assert(result(0).name == "Charlie") // age 35
    assert(result(2).name == "Bob") // age 25
  }

  // ---------------------------------------------------------------------------
  // sample
  // ---------------------------------------------------------------------------

  test("sample on typed Dataset") {
    import Encoder.given
    val ds = spark.createDataset((1 to 100).toSeq)
    val sampled = ds.sample(0.1, seed = 42L)
    val count = sampled.count()
    assert(count > 0)
    assert(count < 100)
  }

  test("sample withReplacement on typed Dataset") {
    import Encoder.given
    val ds = spark.createDataset((1 to 100).toSeq)
    val sampled = ds.sample(withReplacement = true, fraction = 0.5, seed = 42L)
    assert(sampled.count() > 0)
  }

  // ---------------------------------------------------------------------------
  // limit / offset
  // ---------------------------------------------------------------------------

  test("limit on typed Dataset") {
    val ds = personDs
    val result = ds.limit(2).collect()
    assert(result.length == 2)
  }

  test("offset on typed Dataset") {
    val ds = personDs.orderBy(col("age").asc)
    val result = ds.toDF().offset(1).collect()
    assert(result.length == 2)
  }

  // ---------------------------------------------------------------------------
  // repartition / coalesce
  // ---------------------------------------------------------------------------

  test("repartition on typed Dataset") {
    import Encoder.given
    val ds = spark.createDataset((1 to 100).toSeq)
    val repartitioned = ds.repartition(4)
    // No .rdd in Spark Connect; just verify data integrity
    assert(repartitioned.count() == 100)
  }

  test("coalesce on typed Dataset") {
    import Encoder.given
    val ds = spark.createDataset((1 to 100).toSeq).repartition(4)
    val coalesced = ds.coalesce(2)
    assert(coalesced.count() == 100)
  }

  // ---------------------------------------------------------------------------
  // alias
  // ---------------------------------------------------------------------------

  test("alias on typed Dataset") {
    val ds = personDs.alias("people")
    val result = ds.select(col("people.name")).collect()
    assert(result.length == 3)
  }

  // ---------------------------------------------------------------------------
  // cache / persist / unpersist
  // ---------------------------------------------------------------------------

  test("cache and unpersist on typed Dataset") {
    val ds = personDs.cache()
    // trigger caching by collecting
    assert(ds.count() == 3)
    ds.unpersist()
    // data should still be accessible after unpersist
    assert(ds.count() == 3)
  }

  test("persist on typed Dataset") {
    val ds = personDs.persist(StorageLevel.MEMORY_ONLY)
    assert(ds.count() == 3)
    ds.unpersist()
  }

  // ---------------------------------------------------------------------------
  // count / isEmpty
  // ---------------------------------------------------------------------------

  test("count on typed Dataset") {
    assert(personDs.count() == 3)
  }

  test("isEmpty on typed Dataset") {
    assert(!personDs.isEmpty)
    import Encoder.given
    val empty = spark.createDataset(Seq.empty[Int])
    assert(empty.isEmpty)
  }

  // ---------------------------------------------------------------------------
  // describe / summary
  // ---------------------------------------------------------------------------

  test("describe on typed Dataset") {
    val result = personDs.describe("age").collect()
    // describe returns count, mean, stddev, min, max
    assert(result.length == 5)
    val countRow = result.find(_.getString(0) == "count")
    assert(countRow.isDefined)
    assert(countRow.get.getString(1) == "3")
  }

  test("summary on typed Dataset") {
    val result = personDs.summary("count", "min", "max").collect()
    assert(result.length == 3)
    val countRow = result.find(_.getString(0) == "count")
    assert(countRow.isDefined)
  }

  // ---------------------------------------------------------------------------
  // show (verify no exception)
  // ---------------------------------------------------------------------------

  test("show on typed Dataset does not throw") {
    // show returns Unit; just verify it completes without error
    personDs.show()
    personDs.show(2)
    personDs.show(2, 0)
    personDs.show(2, 20, vertical = false)
  }

  // ---------------------------------------------------------------------------
  // toDF / toDF(colNames*)
  // ---------------------------------------------------------------------------

  test("toDF on typed Dataset") {
    val df = personDs.toDF()
    assert(df.columns.toSet == Set("name", "age"))
    assert(df.count() == 3)
  }

  test("toDF with column names on typed Dataset") {
    val df = personDs.toDF("n", "a")
    assert(df.columns.toSeq == Seq("n", "a"))
    assert(df.count() == 3)
  }

  // ---------------------------------------------------------------------------
  // explain (verify no exception)
  // ---------------------------------------------------------------------------

  test("explain on typed Dataset does not throw") {
    personDs.explain()
    personDs.explain(true)
    personDs.explain("simple")
  }

  // ---------------------------------------------------------------------------
  // createTempView / createOrReplaceTempView
  // ---------------------------------------------------------------------------

  test("createTempView on typed Dataset") {
    val viewName = "test_typed_temp_view"
    personDs.createTempView(viewName)
    try
      val result = spark.sql(s"SELECT * FROM $viewName").count()
      assert(result == 3)
    finally
      spark.sql(s"DROP VIEW IF EXISTS $viewName").collect()
  }

  test("createOrReplaceTempView on typed Dataset") {
    val viewName = "test_typed_replace_view"
    personDs.createOrReplaceTempView(viewName)
    assert(spark.sql(s"SELECT * FROM $viewName").count() == 3)
    // replace with different data
    import Encoder.given
    spark.createDataset(Seq(Person("Zara", 40))).createOrReplaceTempView(viewName)
    assert(spark.sql(s"SELECT * FROM $viewName").count() == 1)
    spark.sql(s"DROP VIEW IF EXISTS $viewName").collect()
  }

  // ---------------------------------------------------------------------------
  // write accessor
  // ---------------------------------------------------------------------------

  test("write on typed Dataset returns DataFrameWriter") {
    val writer = personDs.write
    assert(writer != null)
    assert(writer.isInstanceOf[DataFrameWriter])
  }

  // ---------------------------------------------------------------------------
  // columns / dtypes / schema / printSchema
  // ---------------------------------------------------------------------------

  test("columns on typed Dataset") {
    val cols = personDs.columns
    assert(cols.toSet == Set("name", "age"))
  }

  test("dtypes on typed Dataset") {
    val dtypes = personDs.dtypes
    assert(dtypes.length == 2)
    val dtypeMap = dtypes.toMap
    assert(dtypeMap("name") == "StringType" || dtypeMap("name").toLowerCase.contains("string"))
    assert(dtypeMap("age") == "IntegerType" || dtypeMap("age").toLowerCase.contains("int"))
  }

  test("schema on typed Dataset") {
    val schema = personDs.schema
    assert(schema.fieldNames.toSet == Set("name", "age"))
  }

  test("printSchema on typed Dataset does not throw") {
    personDs.printSchema()
    personDs.printSchema(2)
  }
