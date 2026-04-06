package org.apache.spark.sql

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.tags.IntegrationTest
import org.apache.spark.sql.types.*

/** Integration tests for UDF and UDAF. */
@IntegrationTest
class UdfIntegrationSuite extends IntegrationTestBase:

  // ---------------------------------------------------------------------------
  // UDF (User-Defined Functions)
  // ---------------------------------------------------------------------------

  test("UDF register and use in SQL") {
    assert(classFilesUploaded)
    val addOne = udf((x: Int) => x + 1)
    spark.udf.register("add_one", addOne)

    val df = spark.range(5)
    df.createOrReplaceTempView("numbers")

    val result = spark.sql("SELECT add_one(CAST(id AS INT)) AS result FROM numbers")
      .collect()
    assert(result.length == 5)
    assert(result(0).get(0).toString.toInt == 1) // 0 + 1
    assert(result(4).get(0).toString.toInt == 5) // 4 + 1
  }

  test("UDF applied to columns directly") {
    assert(classFilesUploaded)
    val doubler = udf((x: Int) => x * 2)
    val rows = Seq(Row(1), Row(2), Row(3))
    val schema = StructType(Seq(StructField("value", IntegerType)))
    val df = spark.createDataFrame(rows, schema)

    val result = df.select(doubler(col("value")).as("doubled")).collect()
    assert(result.length == 3)
    assert(result(0).get(0).toString.toInt == 2)
    assert(result(1).get(0).toString.toInt == 4)
    assert(result(2).get(0).toString.toInt == 6)
  }

  test("UDF with multiple arguments") {
    assert(classFilesUploaded)
    val concat = udf((a: String, b: String) => s"$a-$b")
    val rows = Seq(
      Row("hello", "world"),
      Row("foo", "bar")
    )
    val schema = StructType(Seq(
      StructField("a", StringType),
      StructField("b", StringType)
    ))
    val df = spark.createDataFrame(rows, schema)

    val result = df.select(concat(col("a"), col("b")).as("merged")).collect()
    assert(result.length == 2)
    assert(result(0).getString(0) == "hello-world")
    assert(result(1).getString(0) == "foo-bar")
  }

  // ---------------------------------------------------------------------------
  // UDAF (User-Defined Aggregate Functions)
  // ---------------------------------------------------------------------------

  /** A simple Long-sum aggregator. */
  object LongSumAggregator extends Aggregator[Long, Long, Long]:
    def zero: Long = 0L
    def reduce(b: Long, a: Long): Long = b + a
    def merge(b1: Long, b2: Long): Long = b1 + b2
    def finish(reduction: Long): Long = reduction
    def bufferEncoder: Encoder[Long] = Encoders.scalaLong
    def outputEncoder: Encoder[Long] = Encoders.scalaLong

  /** A Long-max aggregator. */
  object LongMaxAggregator extends Aggregator[Long, Long, Long]:
    def zero: Long = Long.MinValue
    def reduce(b: Long, a: Long): Long = math.max(b, a)
    def merge(b1: Long, b2: Long): Long = math.max(b1, b2)
    def finish(reduction: Long): Long = reduction
    def bufferEncoder: Encoder[Long] = Encoders.scalaLong
    def outputEncoder: Encoder[Long] = Encoders.scalaLong

  test("UDAF applied to columns directly") {
    assert(classFilesUploaded)
    val mySum = udaf(LongSumAggregator, Encoders.scalaLong)
    val df = spark.range(1, 11) // 1..10
    val result = df.agg(mySum(col("id")).as("total")).collect()
    assert(result.length == 1)
    assert(result(0).getLong(0) == 55L) // sum 1..10 = 55
  }

  test("UDAF register and use in SQL") {
    assert(classFilesUploaded)
    val mySum = udaf(LongSumAggregator, Encoders.scalaLong)
    spark.udf.register("my_long_sum", mySum)

    spark.range(1, 6).createOrReplaceTempView("udaf_test") // 1..5
    val result = spark.sql("SELECT my_long_sum(id) AS total FROM udaf_test").collect()
    assert(result.length == 1)
    assert(result(0).getLong(0) == 15L) // sum 1..5 = 15
  }

  test("UDAF with groupBy") {
    assert(classFilesUploaded)
    val myMax = udaf(LongMaxAggregator, Encoders.scalaLong)
    val rows = Seq(
      Row("A", 10L),
      Row("A", 20L),
      Row("B", 30L),
      Row("B", 5L)
    )
    val schema = StructType(
      Seq(
        StructField("group", StringType),
        StructField("value", LongType)
      )
    )
    val df = spark.createDataFrame(rows, schema)
    val result = df
      .groupBy(col("group"))
      .agg(myMax(col("value")).as("max_val"))
      .orderBy(col("group"))
      .collect()
    assert(result.length == 2)
    assert(result(0).getString(0) == "A")
    assert(result(0).getLong(1) == 20L)
    assert(result(1).getString(0) == "B")
    assert(result(1).getLong(1) == 30L)
  }
