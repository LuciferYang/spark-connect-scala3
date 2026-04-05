package org.apache.spark.sql

import org.apache.spark.connect.proto.*
import org.scalatest.funsuite.AnyFunSuite

class DataFrameWriterV2Suite extends AnyFunSuite:

  /** Minimal SparkSession stub — just enough to construct DataFrameWriterV2. */
  private def dummyDf: DataFrame =
    val rel = Relation.newBuilder()
      .setRange(Range.newBuilder().setStart(0).setEnd(10).setStep(1).build())
      .build()
    // We only need the relation to build the proto; no actual server needed.
    DataFrame(null, rel)

  test("writeTo returns DataFrameWriterV2") {
    val df = dummyDf
    val writer = DataFrameWriterV2("test_table", df)
    assert(writer != null)
  }

  test("chained configuration methods compile and return same instance") {
    val df = dummyDf
    val writer = DataFrameWriterV2("test_table", df)
    val result = writer
      .using("parquet")
      .option("key1", "value1")
      .option("key2", true)
      .option("key3", 42L)
      .option("key4", 3.14)
      .options(Map("a" -> "b"))
      .tableProperty("prop", "val")
    assert(result eq writer, "All config methods should return the same instance")
  }

  test("partitionedBy accepts Column varargs") {
    val df = dummyDf
    val writer = DataFrameWriterV2("test_table", df)
    val result = writer.partitionedBy(Column("year"), Column("month"))
    assert(result eq writer)
  }

  test("clusterBy accepts String varargs") {
    val df = dummyDf
    val writer = DataFrameWriterV2("test_table", df)
    val result = writer.clusterBy("col1", "col2", "col3")
    assert(result eq writer)
  }
