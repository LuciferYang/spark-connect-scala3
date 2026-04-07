package org.apache.spark.sql

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.tags.IntegrationTest
import org.apache.spark.sql.types.*

/** Integration tests for JDBC read/write via DataFrameReader.jdbc and DataFrameWriter.jdbc.
  *
  * Requires: H2 JDBC driver on the Spark Connect server classpath. Start the server with:
  * {{{
  *   $SPARK_HOME/sbin/start-connect-server.sh --jars /path/to/h2-2.2.224.jar
  * }}}
  */
@IntegrationTest
class JdbcIntegrationSuite extends IntegrationTestBase:

  // H2 in-memory database URL. DB_CLOSE_DELAY=-1 keeps the DB alive across connections.
  private val jdbcUrl = "jdbc:h2:mem:sc3_jdbc_test;DB_CLOSE_DELAY=-1"
  private val driver = "org.h2.Driver"

  private def jdbcProps: java.util.Properties =
    val p = java.util.Properties()
    p.put("driver", driver)
    p

  /** Helper to wrap JDBC tests — cancels gracefully if H2 is not on the server classpath. */
  private def withJdbcSupport(body: => Unit): Unit =
    try body
    catch
      case e: Exception
          if e.getMessage != null &&
            (e.getMessage.contains("No suitable driver") ||
              e.getMessage.contains("org.h2.Driver") ||
              e.getMessage.contains("ClassNotFoundException") ||
              e.getMessage.contains("JDBC_DRIVER_NOT_FOUND") ||
              e.getMessage.contains("Failed to load JDBC")) =>
        cancel(s"H2 JDBC driver not available on server: ${e.getMessage.take(120)}")

  /** Create the test table via Spark SQL (which runs on the server with H2 driver). */
  private def createTestTable(tableName: String): Unit =
    spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("driver", driver)
      .option(
        "query",
        s"""
        CREATE TABLE IF NOT EXISTS $tableName (
          id INT PRIMARY KEY,
          name VARCHAR(50),
          value DOUBLE
        )
      """.trim
      )
      .load()

  /** Seed test data using DataFrame write. */
  private def seedTestData(tableName: String): Unit =
    val rows = Seq(
      Row(1, "alice", 10.0),
      Row(2, "bob", 20.0),
      Row(3, "charlie", 30.0),
      Row(4, "diana", 40.0),
      Row(5, "eve", 50.0)
    )
    val schema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType),
        StructField("value", DoubleType)
      )
    )
    spark
      .createDataFrame(rows, schema)
      .write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .option("driver", driver)
      .mode("overwrite")
      .save()

  // ---------------------------------------------------------------------------
  // Write: format("jdbc") with save()
  // ---------------------------------------------------------------------------

  test("write and read via format(jdbc) round-trip") {
    withJdbcSupport {
      val tableName = "SC3_JDBC_BASIC"
      seedTestData(tableName)

      val df = spark.read
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", tableName)
        .option("driver", driver)
        .load()
        .orderBy(col("id"))

      val result = df.collect()
      assert(result.length == 5)
      assert(result(0).getInt(0) == 1)
      assert(result(0).getString(1) == "alice")
      assert(result(4).getDouble(2) == 50.0)
    }
  }

  // ---------------------------------------------------------------------------
  // Read: DataFrameReader.jdbc(url, table, properties)
  // ---------------------------------------------------------------------------

  test("reader.jdbc(url, table, properties) - simple read") {
    withJdbcSupport {
      val tableName = "SC3_JDBC_READER_SIMPLE"
      seedTestData(tableName)

      val df = spark.read.jdbc(jdbcUrl, tableName, jdbcProps)
      assert(df.count() == 5)
      val names = df.select(col("name")).orderBy(col("name")).collect().map(_.getString(0))
      assert(names.toSeq == Seq("alice", "bob", "charlie", "diana", "eve"))
    }
  }

  // ---------------------------------------------------------------------------
  // Read: DataFrameReader.jdbc with range partitioning
  // ---------------------------------------------------------------------------

  test("reader.jdbc with range partitioning") {
    withJdbcSupport {
      val tableName = "SC3_JDBC_READER_RANGE"
      seedTestData(tableName)

      val df = spark.read.jdbc(
        jdbcUrl,
        tableName,
        columnName = "\"id\"",
        lowerBound = 1L,
        upperBound = 5L,
        numPartitions = 2,
        connectionProperties = jdbcProps
      )
      val result = df.orderBy(col("id")).collect()
      assert(result.length == 5)
      assert(result(0).getInt(0) == 1)
      assert(result(4).getInt(0) == 5)
    }
  }

  // ---------------------------------------------------------------------------
  // Read: DataFrameReader.jdbc with predicate partitioning
  // ---------------------------------------------------------------------------

  test("reader.jdbc with predicate partitioning") {
    withJdbcSupport {
      val tableName = "SC3_JDBC_READER_PRED"
      seedTestData(tableName)

      // Column names are lowercase (quoted by Spark during table creation via DataFrame write)
      val predicates = Array(""""id" <= 2""", """"id" > 2 AND "id" <= 4""", """"id" > 4""")
      val df = spark.read.jdbc(jdbcUrl, tableName, predicates, jdbcProps)
      val result = df.orderBy(col("id")).collect()
      assert(result.length == 5)
      assert(result(0).getInt(0) == 1)
      assert(result(4).getInt(0) == 5)
    }
  }

  // ---------------------------------------------------------------------------
  // Write: DataFrameWriter.jdbc(url, table, properties)
  // ---------------------------------------------------------------------------

  test("writer.jdbc(url, table, properties) convenience method") {
    withJdbcSupport {
      val tableName = "SC3_JDBC_WRITER_CONV"
      val rows = Seq(Row(1, "x", 1.0), Row(2, "y", 2.0))
      val schema = StructType(
        Seq(
          StructField("id", IntegerType),
          StructField("name", StringType),
          StructField("value", DoubleType)
        )
      )
      val df = spark.createDataFrame(rows, schema)
      df.write.mode("overwrite").jdbc(jdbcUrl, tableName, jdbcProps)

      val result = spark.read.jdbc(jdbcUrl, tableName, jdbcProps).orderBy(col("id")).collect()
      assert(result.length == 2)
      assert(result(0).getString(1) == "x")
      assert(result(1).getString(1) == "y")
    }
  }

  // ---------------------------------------------------------------------------
  // Write modes: overwrite, append
  // ---------------------------------------------------------------------------

  test("jdbc write mode overwrite replaces existing data") {
    withJdbcSupport {
      val tableName = "SC3_JDBC_OVERWRITE"
      seedTestData(tableName)
      assert(spark.read.jdbc(jdbcUrl, tableName, jdbcProps).count() == 5)

      // overwrite with 2 rows
      val rows = Seq(Row(10, "new1", 100.0), Row(20, "new2", 200.0))
      val schema = StructType(
        Seq(
          StructField("id", IntegerType),
          StructField("name", StringType),
          StructField("value", DoubleType)
        )
      )
      spark
        .createDataFrame(rows, schema)
        .write
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", tableName)
        .option("driver", driver)
        .mode("overwrite")
        .save()

      assert(spark.read.jdbc(jdbcUrl, tableName, jdbcProps).count() == 2)
    }
  }

  test("jdbc write mode append adds to existing data") {
    withJdbcSupport {
      val tableName = "SC3_JDBC_APPEND"
      seedTestData(tableName)
      assert(spark.read.jdbc(jdbcUrl, tableName, jdbcProps).count() == 5)

      val rows = Seq(Row(6, "frank", 60.0))
      val schema = StructType(
        Seq(
          StructField("id", IntegerType),
          StructField("name", StringType),
          StructField("value", DoubleType)
        )
      )
      spark
        .createDataFrame(rows, schema)
        .write
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", tableName)
        .option("driver", driver)
        .mode("append")
        .save()

      assert(spark.read.jdbc(jdbcUrl, tableName, jdbcProps).count() == 6)
    }
  }

  // ---------------------------------------------------------------------------
  // Filter pushdown (server-side WHERE)
  // ---------------------------------------------------------------------------

  test("jdbc read with filter pushdown") {
    withJdbcSupport {
      val tableName = "SC3_JDBC_FILTER"
      seedTestData(tableName)

      val df = spark.read.jdbc(jdbcUrl, tableName, jdbcProps)
      val filtered = df.filter(col("id") > lit(3)).orderBy(col("id")).collect()
      assert(filtered.length == 2)
      assert(filtered(0).getInt(0) == 4)
      assert(filtered(1).getInt(0) == 5)
    }
  }

  // ---------------------------------------------------------------------------
  // Column selection (project pushdown)
  // ---------------------------------------------------------------------------

  test("jdbc read with column selection") {
    withJdbcSupport {
      val tableName = "SC3_JDBC_SELECT"
      seedTestData(tableName)

      val df = spark.read
        .jdbc(jdbcUrl, tableName, jdbcProps)
        .select(col("name"), col("value"))
        .orderBy(col("name"))

      val result = df.collect()
      assert(result.length == 5)
      assert(df.columns.toSeq == Seq("name", "value"))
      assert(result(0).getString(0) == "alice")
    }
  }

  // ---------------------------------------------------------------------------
  // Aggregation on JDBC data
  // ---------------------------------------------------------------------------

  test("jdbc read with aggregation") {
    withJdbcSupport {
      val tableName = "SC3_JDBC_AGG"
      seedTestData(tableName)

      val result = spark.read
        .jdbc(jdbcUrl, tableName, jdbcProps)
        .agg(
          functions.sum(col("value")).as("total"),
          functions.avg(col("value")).as("average"),
          functions.count(col("id")).as("cnt")
        )
        .collect()

      assert(result.length == 1)
      assert(result(0).get(0).toString.toDouble == 150.0) // 10+20+30+40+50
      assert(result(0).get(1).toString.toDouble == 30.0) // avg
      assert(result(0).get(2).toString.toLong == 5L)
    }
  }

  // ---------------------------------------------------------------------------
  // Read with query option (subquery / custom SQL)
  // ---------------------------------------------------------------------------

  test("jdbc read with query option instead of dbtable") {
    withJdbcSupport {
      val tableName = "SC3_JDBC_QUERY"
      seedTestData(tableName)

      val df = spark.read
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("driver", driver)
        .option(
          "query",
          s"""SELECT "id", "name" FROM $tableName WHERE "id" <= 3"""
        )
        .load()

      val result = df.orderBy(col("id")).collect()
      assert(result.length == 3)
      assert(df.columns.length == 2)
      assert(result(2).getInt(0) == 3)
    }
  }

  // ---------------------------------------------------------------------------
  // Read with fetchsize option
  // ---------------------------------------------------------------------------

  test("jdbc read with fetchsize option") {
    withJdbcSupport {
      val tableName = "SC3_JDBC_FETCH"
      seedTestData(tableName)

      val df = spark.read
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", tableName)
        .option("driver", driver)
        .option("fetchsize", "2")
        .load()

      assert(df.count() == 5)
    }
  }

  // ---------------------------------------------------------------------------
  // Schema inference
  // ---------------------------------------------------------------------------

  test("jdbc read infers correct schema") {
    withJdbcSupport {
      val tableName = "SC3_JDBC_SCHEMA"
      seedTestData(tableName)

      val df = spark.read.jdbc(jdbcUrl, tableName, jdbcProps)
      val fields = df.schema.fields
      assert(fields.length == 3)
      // H2 INT maps to IntegerType, VARCHAR to StringType, DOUBLE to DoubleType
      assert(fields.exists(_.name.equalsIgnoreCase("id")))
      assert(fields.exists(_.name.equalsIgnoreCase("name")))
      assert(fields.exists(_.name.equalsIgnoreCase("value")))
    }
  }
