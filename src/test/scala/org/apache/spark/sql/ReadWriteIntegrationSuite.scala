package org.apache.spark.sql

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.tags.IntegrationTest
import org.apache.spark.sql.types.*

import java.nio.file.{Files, Path}
import java.util.Comparator

/** Integration tests for DataFrameReader and DataFrameWriter: csv/json/parquet/orc/text round-trip,
  * write modes, partitionBy, saveAsTable, insertInto.
  */
@IntegrationTest
class ReadWriteIntegrationSuite extends IntegrationTestBase:

  /** Create a temp directory and ensure cleanup after use. */
  private def withTempDir(f: Path => Unit): Unit =
    val dir = Files.createTempDirectory("sc3_rw_test_")
    try f(dir)
    finally
      if Files.exists(dir) then
        Files.walk(dir)
          .sorted(Comparator.reverseOrder())
          .forEach(p => Files.deleteIfExists(p))

  /** Create a simple test DataFrame with id (Long), name (String), value (Double). */
  private def testDf: DataFrame =
    val rows = Seq(
      Row(1L, "alice", 10.0),
      Row(2L, "bob", 20.0),
      Row(3L, "charlie", 30.0)
    )
    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType),
      StructField("value", DoubleType)
    ))
    spark.createDataFrame(rows, schema)

  // ---------------------------------------------------------------------------
  // Parquet round-trip
  // ---------------------------------------------------------------------------

  test("write and read parquet with convenience methods") {
    withTempDir { dir =>
      val path = dir.resolve("data.parquet").toString
      testDf.write.parquet(path)
      val result = spark.read.parquet(path).orderBy(col("id")).collect()
      assert(result.length == 3)
      assert(result(0).getLong(0) == 1L)
      assert(result(2).getString(1) == "charlie")
    }
  }

  test("write and read parquet with format/save/load") {
    withTempDir { dir =>
      val path = dir.resolve("data").toString
      testDf.write.format("parquet").save(path)
      val result = spark.read.format("parquet").load(path).count()
      assert(result == 3)
    }
  }

  test("read.load defaults to parquet") {
    withTempDir { dir =>
      val path = dir.resolve("data").toString
      testDf.write.parquet(path)
      val result = spark.read.load(path).count()
      assert(result == 3)
    }
  }

  // ---------------------------------------------------------------------------
  // JSON round-trip
  // ---------------------------------------------------------------------------

  test("write and read json") {
    withTempDir { dir =>
      val path = dir.resolve("data.json").toString
      testDf.write.json(path)
      val result = spark.read.json(path).orderBy(col("id")).collect()
      assert(result.length == 3)
      assert(result(1).getString(1) == "bob")
    }
  }

  // ---------------------------------------------------------------------------
  // CSV round-trip
  // ---------------------------------------------------------------------------

  test("write and read csv with header") {
    withTempDir { dir =>
      val path = dir.resolve("data.csv").toString
      testDf.write.option("header", "true").csv(path)
      val result = spark.read
        .option("header", true)
        .option("inferSchema", "true")
        .csv(path)
        .orderBy(col("id"))
        .collect()
      assert(result.length == 3)
      assert(result(0).getString(1) == "alice")
    }
  }

  test("read csv with explicit schema (DDL string)") {
    withTempDir { dir =>
      val path = dir.resolve("data.csv").toString
      testDf.write.option("header", "true").csv(path)
      val result = spark.read
        .schema("id LONG, name STRING, value DOUBLE")
        .option("header", "true")
        .csv(path)
        .orderBy(col("id"))
        .collect()
      assert(result.length == 3)
      assert(result(2).getLong(0) == 3L)
    }
  }

  test("read csv with explicit schema (StructType)") {
    withTempDir { dir =>
      val path = dir.resolve("data.csv").toString
      testDf.write.option("header", "true").csv(path)
      val schema = StructType(Seq(
        StructField("id", LongType),
        StructField("name", StringType),
        StructField("value", DoubleType)
      ))
      val result = spark.read
        .schema(schema)
        .option("header", "true")
        .csv(path)
        .count()
      assert(result == 3)
    }
  }

  // ---------------------------------------------------------------------------
  // ORC round-trip
  // ---------------------------------------------------------------------------

  test("write and read orc") {
    withTempDir { dir =>
      val path = dir.resolve("data.orc").toString
      testDf.write.orc(path)
      val result = spark.read.orc(path).orderBy(col("id")).collect()
      assert(result.length == 3)
      assert(result(0).getLong(0) == 1L)
    }
  }

  // ---------------------------------------------------------------------------
  // Text round-trip
  // ---------------------------------------------------------------------------

  test("write and read text") {
    withTempDir { dir =>
      val path = dir.resolve("data.text").toString
      // text format requires a single string column
      val textDf = spark.createDataFrame(
        Seq(Row("hello"), Row("world"), Row("spark")),
        StructType(Seq(StructField("value", StringType)))
      )
      textDf.write.text(path)
      val result = spark.read.text(path).orderBy(col("value")).collect()
      assert(result.length == 3)
      assert(result(0).getString(0) == "hello")
      assert(result(2).getString(0) == "world")
    }
  }

  // ---------------------------------------------------------------------------
  // Write modes
  // ---------------------------------------------------------------------------

  test("mode overwrite replaces existing data") {
    withTempDir { dir =>
      val path = dir.resolve("data").toString
      testDf.write.parquet(path)
      // overwrite with fewer rows
      val small = spark.createDataFrame(
        Seq(Row(99L, "z", 0.0)),
        testDf.schema
      )
      small.write.mode("overwrite").parquet(path)
      val result = spark.read.parquet(path).collect()
      assert(result.length == 1)
      assert(result(0).getLong(0) == 99L)
    }
  }

  test("mode append adds to existing data") {
    withTempDir { dir =>
      val path = dir.resolve("data").toString
      testDf.write.parquet(path)
      testDf.write.mode("append").parquet(path)
      val result = spark.read.parquet(path).count()
      assert(result == 6)
    }
  }

  test("mode ignore skips write if path exists") {
    withTempDir { dir =>
      val path = dir.resolve("data").toString
      testDf.write.parquet(path)
      val small = spark.createDataFrame(
        Seq(Row(99L, "z", 0.0)),
        testDf.schema
      )
      small.write.mode("ignore").parquet(path)
      // original data should remain unchanged
      val result = spark.read.parquet(path).count()
      assert(result == 3)
    }
  }

  test("mode error throws on existing path") {
    withTempDir { dir =>
      val path = dir.resolve("data").toString
      testDf.write.parquet(path)
      val caught = intercept[Exception] {
        testDf.write.mode("error").parquet(path)
      }
      assert(caught != null)
    }
  }

  test("mode with SaveMode enum") {
    withTempDir { dir =>
      val path = dir.resolve("data").toString
      testDf.write.mode(SaveMode.Overwrite).parquet(path)
      assert(spark.read.parquet(path).count() == 3)
      testDf.write.mode(SaveMode.Append).parquet(path)
      assert(spark.read.parquet(path).count() == 6)
    }
  }

  // ---------------------------------------------------------------------------
  // partitionBy
  // ---------------------------------------------------------------------------

  test("partitionBy writes partitioned data") {
    withTempDir { dir =>
      val path = dir.resolve("data").toString
      val rows = Seq(
        Row(1L, "A", 10.0),
        Row(2L, "B", 20.0),
        Row(3L, "A", 30.0)
      )
      val schema = StructType(Seq(
        StructField("id", LongType),
        StructField("group", StringType),
        StructField("value", DoubleType)
      ))
      val df = spark.createDataFrame(rows, schema)
      df.write.partitionBy("group").parquet(path)
      // read back and verify all data is present
      val result = spark.read.parquet(path).orderBy(col("id")).collect()
      assert(result.length == 3)
      // verify group column is still available after reading partitioned data
      val groups = spark.read.parquet(path)
        .select(col("group")).distinct().orderBy(col("group")).collect()
      assert(groups.map(_.getString(0)).toSeq == Seq("A", "B"))
    }
  }

  // ---------------------------------------------------------------------------
  // saveAsTable / insertInto / table
  // ---------------------------------------------------------------------------

  test("saveAsTable and read.table round-trip") {
    val tableName = "sc3_test_save_as_table"
    try
      testDf.write.mode("overwrite").saveAsTable(tableName)
      val result = spark.read.table(tableName).orderBy(col("id")).collect()
      assert(result.length == 3)
      assert(result(0).getLong(0) == 1L)
    finally
      try spark.sql(s"DROP TABLE IF EXISTS $tableName")
      catch case _: Exception => ()
  }

  test("insertInto appends to existing table") {
    val tableName = "sc3_test_insert_into"
    try
      testDf.write.mode("overwrite").saveAsTable(tableName)
      testDf.write.insertInto(tableName)
      val result = spark.read.table(tableName).count()
      assert(result == 6)
    finally
      try spark.sql(s"DROP TABLE IF EXISTS $tableName")
      catch case _: Exception => ()
  }

  // ---------------------------------------------------------------------------
  // Reader option overloads
  // ---------------------------------------------------------------------------

  test("reader option overloads (Boolean, Long, Double)") {
    withTempDir { dir =>
      val path = dir.resolve("data.csv").toString
      testDf.write.option("header", "true").csv(path)
      // Boolean overload
      val r1 = spark.read.option("header", true).option("inferSchema", true)
        .csv(path).count()
      assert(r1 == 3)
      // Long overload (maxColumns)
      val r2 = spark.read.option("header", true).option("maxColumns", 100L)
        .csv(path).count()
      assert(r2 == 3)
      // Double overload (samplingRatio)
      val r3 = spark.read.option("header", true).option("samplingRatio", 1.0)
        .csv(path).count()
      assert(r3 == 3)
    }
  }

  test("reader options(Map) batch setter") {
    withTempDir { dir =>
      val path = dir.resolve("data.csv").toString
      testDf.write.option("header", "true").csv(path)
      val result = spark.read
        .options(Map("header" -> "true", "inferSchema" -> "true"))
        .csv(path).count()
      assert(result == 3)
    }
  }

  // ---------------------------------------------------------------------------
  // Writer option overloads
  // ---------------------------------------------------------------------------

  test("writer option overloads (Boolean, Long, Double)") {
    withTempDir { dir =>
      val path = dir.resolve("data.csv").toString
      testDf.write
        .option("header", true)
        .option("maxRecordsPerFile", 1000L)
        .option("emptyValue", 0.0)
        .csv(path)
      assert(spark.read.option("header", true).csv(path).count() == 3)
    }
  }

  test("writer options(Map) batch setter") {
    withTempDir { dir =>
      val path = dir.resolve("data.csv").toString
      testDf.write
        .options(Map("header" -> "true"))
        .csv(path)
      assert(spark.read.option("header", "true").csv(path).count() == 3)
    }
  }

  // ---------------------------------------------------------------------------
  // DataFrameWriterV2
  // ---------------------------------------------------------------------------

  test("writeTo.create and append") {
    val tableName = "testcat.sc3_test_writer_v2_create"
    try
      testDf.writeTo(tableName).create()
      val r1 = spark.read.table(tableName).count()
      assert(r1 == 3)
      testDf.writeTo(tableName).append()
      val r2 = spark.read.table(tableName).count()
      assert(r2 == 6)
    finally
      try spark.sql(s"DROP TABLE IF EXISTS $tableName")
      catch case _: Exception => ()
  }

  test("writeTo.createOrReplace") {
    val tableName = "testcat.sc3_test_writer_v2_cor"
    try
      testDf.writeTo(tableName).createOrReplace()
      assert(spark.read.table(tableName).count() == 3)
      // createOrReplace again with different data
      val small = spark.createDataFrame(
        Seq(Row(99L, "z", 0.0)),
        testDf.schema
      )
      small.writeTo(tableName).createOrReplace()
      assert(spark.read.table(tableName).count() == 1)
    finally
      try spark.sql(s"DROP TABLE IF EXISTS $tableName")
      catch case _: Exception => ()
  }
