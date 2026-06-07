package org.apache.spark.sql

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SQLContextSuite extends AnyFunSuite with Matchers:

  test("SparkSession exposes SQLContext facade") {
    val spark = SparkSession(null)
    val sqlContext = spark.sqlContext

    sqlContext.sparkSession shouldBe spark
    sqlContext.emptyDataFrame.relation.hasLocalRelation shouldBe true
    sqlContext.range(3).relation should not be null
  }

  test("SQLContext delegates createDataFrame and SQL") {
    val spark = SparkSession.builder().remote("sc://localhost:15002").build()
    val sqlContext = SQLContext.getOrCreate(spark)
    val schema = StructType(Seq(StructField("id", IntegerType)))

    sqlContext.createDataFrame(Seq(Row(1)), schema).relation.hasLocalRelation shouldBe true
    sqlContext.sql("SELECT 1").relation.hasSql shouldBe true
  }

  test("SQLContext exposes session-bound implicits") {
    val spark = SparkSession(null)
    val sqlContext = spark.sqlContext
    import sqlContext.implicits.*

    Seq(1, 2, 3).toDS.toDF().relation.hasLocalRelation shouldBe true
  }

  test("SQLContext sparkContext is unsupported for Connect") {
    val sqlContext = SparkSession(null).sqlContext

    an[UnsupportedOperationException] should be thrownBy sqlContext.sparkContext
  }
