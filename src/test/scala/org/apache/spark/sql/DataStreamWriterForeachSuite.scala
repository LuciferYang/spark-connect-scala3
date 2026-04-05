package org.apache.spark.sql

import org.apache.spark.connect.proto.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataStreamWriterForeachSuite extends AnyFunSuite with Matchers:

  private def session: SparkSession =
    SparkSession.builder().remote("sc://localhost:15002").build()

  private def dummyStreamDf: DataFrame =
    val spark = session
    spark.readStream.format("rate").load()

  test("foreachBatch sets foreach_batch in proto") {
    val writer = DataStreamWriter(dummyStreamDf)
    val func: (DataFrame, Long) => Unit = (df, id) => ()
    val proto = writer.foreachBatch(func).buildWriteStreamOp().build()
    proto.hasForeachBatch shouldBe true
    proto.getForeachBatch.hasScalaFunction shouldBe true
    proto.getForeachBatch.getScalaFunction.getPayload.size() should be > 0
  }

  test("foreach sets foreach_writer in proto") {
    val writer = DataStreamWriter(dummyStreamDf)
    val myWriter = new ForeachWriter[Row]:
      def open(partitionId: Long, epochId: Long): Boolean = true
      def process(value: Row): Unit = ()
      def close(errorOrNull: Throwable): Unit = ()
    val proto = writer.foreach(myWriter).buildWriteStreamOp().build()
    proto.hasForeachWriter shouldBe true
    proto.getForeachWriter.hasScalaFunction shouldBe true
    proto.getForeachWriter.getScalaFunction.getPayload.size() should be > 0
  }

  test("foreachBatch preserves other builder fields") {
    val func: (DataFrame, Long) => Unit = (df, id) => ()
    val writer = DataStreamWriter(dummyStreamDf)
    val proto = writer
      .format("console")
      .outputMode("append")
      .queryName("test")
      .foreachBatch(func)
      .buildWriteStreamOp().build()
    proto.getFormat shouldBe "console"
    proto.getOutputMode shouldBe "append"
    proto.getQueryName shouldBe "test"
    proto.hasForeachBatch shouldBe true
  }

  test("foreachBatch proto has outputType and nullable") {
    val func: (DataFrame, Long) => Unit = (df, id) => ()
    val writer = DataStreamWriter(dummyStreamDf)
    val proto = writer.foreachBatch(func).buildWriteStreamOp().build()
    val scalaFunc = proto.getForeachBatch.getScalaFunction
    scalaFunc.hasOutputType shouldBe true
    scalaFunc.getNullable shouldBe true
  }
