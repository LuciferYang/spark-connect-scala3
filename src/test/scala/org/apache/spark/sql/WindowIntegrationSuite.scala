package org.apache.spark.sql

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.tags.IntegrationTest
import org.apache.spark.sql.types.*

/** Integration tests for Window functions:
  * rowsBetween, rangeBetween, rank, dense_rank, lead, lag, ntile,
  * percent_rank, cume_dist, first_value, last_value, nth_value.
  */
@IntegrationTest
class WindowIntegrationSuite extends IntegrationTestBase:

  private def windowTestDf: DataFrame =
    val rows = Seq(
      Row("A", 1, 10.0),
      Row("A", 2, 20.0),
      Row("A", 3, 30.0),
      Row("B", 1, 40.0),
      Row("B", 2, 50.0)
    )
    val schema = StructType(Seq(
      StructField("group", StringType),
      StructField("seq", IntegerType),
      StructField("value", DoubleType)
    ))
    spark.createDataFrame(rows, schema)

  // ---------------------------------------------------------------------------
  // rowsBetween / rangeBetween
  // ---------------------------------------------------------------------------

  test("rowsBetween with unbounded preceding to current row (running sum)") {
    val w = Window.partitionBy(col("group")).orderBy(col("seq"))
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val result = windowTestDf
      .select(col("group"), col("seq"), sum(col("value")).over(w).as("running_sum"))
      .orderBy(col("group"), col("seq")).collect()
    // A: seq1=10, seq2=30, seq3=60
    assert(result(0).get(2).toString.toDouble == 10.0)
    assert(result(1).get(2).toString.toDouble == 30.0)
    assert(result(2).get(2).toString.toDouble == 60.0)
    // B: seq1=40, seq2=90
    assert(result(3).get(2).toString.toDouble == 40.0)
    assert(result(4).get(2).toString.toDouble == 90.0)
  }

  test("rowsBetween sliding window (-1 to +1)") {
    val w = Window.partitionBy(col("group")).orderBy(col("seq"))
      .rowsBetween(-1, 1)
    val result = windowTestDf
      .select(col("group"), col("seq"), avg(col("value")).over(w).as("avg3"))
      .filter(col("group") === lit("A"))
      .orderBy(col("seq")).collect()
    // seq1: avg(10,20)=15, seq2: avg(10,20,30)=20, seq3: avg(20,30)=25
    assert(result(0).get(2).toString.toDouble == 15.0)
    assert(result(1).get(2).toString.toDouble == 20.0)
    assert(result(2).get(2).toString.toDouble == 25.0)
  }

  test("rangeBetween unbounded preceding to current row") {
    val w = Window.partitionBy(col("group")).orderBy(col("seq"))
      .rangeBetween(Window.unboundedPreceding, Window.currentRow)
    val result = windowTestDf
      .select(col("group"), col("seq"), sum(col("value")).over(w).as("range_sum"))
      .filter(col("group") === lit("A"))
      .orderBy(col("seq")).collect()
    assert(result(0).get(2).toString.toDouble == 10.0)
    assert(result(1).get(2).toString.toDouble == 30.0)
    assert(result(2).get(2).toString.toDouble == 60.0)
  }

  // ---------------------------------------------------------------------------
  // Ranking functions
  // ---------------------------------------------------------------------------

  test("rank and dense_rank") {
    val rows = Seq(
      Row("A", 100), Row("A", 200), Row("A", 200), Row("A", 300)
    )
    val schema = StructType(Seq(
      StructField("grp", StringType),
      StructField("score", IntegerType)
    ))
    val df = spark.createDataFrame(rows, schema)
    val w = Window.partitionBy(col("grp")).orderBy(col("score"))
    val result = df.select(
      col("score"),
      rank().over(w).as("rnk"),
      dense_rank().over(w).as("drnk")
    ).orderBy(col("score"), col("rnk")).collect()
    // score=100: rank=1, dense=1
    assert(result(0).getInt(1) == 1)
    assert(result(0).getInt(2) == 1)
    // score=200 (two): rank=2, dense=2
    assert(result(1).getInt(1) == 2)
    assert(result(1).getInt(2) == 2)
    // score=300: rank=4 (not 3!), dense=3
    assert(result(3).getInt(1) == 4)
    assert(result(3).getInt(2) == 3)
  }

  test("row_number") {
    val w = Window.partitionBy(col("group")).orderBy(col("seq"))
    val result = windowTestDf
      .select(col("group"), col("seq"), row_number().over(w).as("rn"))
      .filter(col("group") === lit("A"))
      .orderBy(col("seq")).collect()
    assert(result(0).getInt(2) == 1)
    assert(result(1).getInt(2) == 2)
    assert(result(2).getInt(2) == 3)
  }

  test("ntile") {
    val w = Window.partitionBy(col("group")).orderBy(col("seq"))
    val result = windowTestDf
      .select(col("group"), col("seq"), ntile(2).over(w).as("tile"))
      .filter(col("group") === lit("A"))
      .orderBy(col("seq")).collect()
    // 3 rows into 2 tiles: [1, 1, 2]
    assert(result(0).getInt(2) == 1)
    assert(result(1).getInt(2) == 1)
    assert(result(2).getInt(2) == 2)
  }

  // ---------------------------------------------------------------------------
  // lead / lag
  // ---------------------------------------------------------------------------

  test("lead and lag") {
    val w = Window.partitionBy(col("group")).orderBy(col("seq"))
    val result = windowTestDf
      .select(
        col("group"), col("seq"), col("value"),
        lead(col("value"), 1).over(w).as("next_val"),
        lag(col("value"), 1).over(w).as("prev_val")
      )
      .filter(col("group") === lit("A"))
      .orderBy(col("seq")).collect()
    // seq=1: next=20, prev=null
    assert(result(0).get(3).toString.toDouble == 20.0)
    assert(result(0).isNullAt(4))
    // seq=2: next=30, prev=10
    assert(result(1).get(3).toString.toDouble == 30.0)
    assert(result(1).get(4).toString.toDouble == 10.0)
    // seq=3: next=null, prev=20
    assert(result(2).isNullAt(3))
    assert(result(2).get(4).toString.toDouble == 20.0)
  }

  // ---------------------------------------------------------------------------
  // percent_rank / cume_dist
  // ---------------------------------------------------------------------------

  test("percent_rank and cume_dist") {
    val rows = Seq(Row(1), Row(2), Row(3), Row(4))
    val schema = StructType(Seq(StructField("x", IntegerType)))
    val df = spark.createDataFrame(rows, schema)
    val w = Window.orderBy(col("x"))
    val result = df.select(
      col("x"),
      percent_rank().over(w).as("pr"),
      cume_dist().over(w).as("cd")
    ).orderBy(col("x")).collect()
    // x=1: percent_rank=0.0, cume_dist=0.25
    assert(result(0).getDouble(1) == 0.0)
    assert(result(0).getDouble(2) == 0.25)
    // x=4: percent_rank=1.0, cume_dist=1.0
    assert(result(3).getDouble(1) == 1.0)
    assert(result(3).getDouble(2) == 1.0)
  }

  // ---------------------------------------------------------------------------
  // first_value / last_value / nth_value
  // ---------------------------------------------------------------------------

  test("first_value and last_value") {
    val w = Window.partitionBy(col("group")).orderBy(col("seq"))
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val result = windowTestDf
      .select(
        col("group"), col("seq"),
        first_value(col("value")).over(w).as("first"),
        last_value(col("value")).over(w).as("last")
      )
      .filter(col("group") === lit("A"))
      .orderBy(col("seq")).collect()
    // all rows see first=10, last=30
    result.foreach { r =>
      assert(r.get(2).toString.toDouble == 10.0)
      assert(r.get(3).toString.toDouble == 30.0)
    }
  }

  test("nth_value") {
    val w = Window.partitionBy(col("group")).orderBy(col("seq"))
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val result = windowTestDf
      .select(col("group"), col("seq"), nth_value(col("value"), 2).over(w).as("second"))
      .filter(col("group") === lit("A"))
      .orderBy(col("seq")).collect()
    // second value in A partition is 20.0
    result.foreach(r => assert(r.get(2).toString.toDouble == 20.0))
  }
