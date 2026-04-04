package org.apache.spark.sql

import org.apache.spark.connect.proto.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataStreamReaderSuite extends AnyFunSuite with Matchers:

  private def session: SparkSession =
    SparkSession.builder().remote("sc://localhost:15002").build()

  private def extractRead(df: DataFrame): Read =
    df.relation.getRead

  test("load sets isStreaming to true") {
    val spark = session
    val df = spark.readStream.format("rate").load()
    val read = extractRead(df)
    read.getIsStreaming shouldBe true
    read.hasDataSource shouldBe true
    read.getDataSource.getFormat shouldBe "rate"
  }

  test("load with path sets isStreaming and path") {
    val spark = session
    val df = spark.readStream.format("json").load("/data/stream")
    val read = extractRead(df)
    read.getIsStreaming shouldBe true
    read.getDataSource.getFormat shouldBe "json"
    read.getDataSource.getPathsList should have size 1
    read.getDataSource.getPaths(0) shouldBe "/data/stream"
  }

  test("schema sets schema string") {
    val spark = session
    val df = spark.readStream.format("csv").schema("name STRING, age INT").load()
    val read = extractRead(df)
    read.getIsStreaming shouldBe true
    read.getDataSource.getSchema shouldBe "name STRING, age INT"
  }

  test("option and options set options") {
    val spark = session
    val df = spark.readStream
      .format("kafka")
      .option("subscribe", "topic1")
      .options(Map("kafka.bootstrap.servers" -> "localhost:9092"))
      .load()
    val read = extractRead(df)
    read.getIsStreaming shouldBe true
    val opts = read.getDataSource.getOptionsMap
    opts.get("subscribe") shouldBe "topic1"
    opts.get("kafka.bootstrap.servers") shouldBe "localhost:9092"
  }

  test("table sets isStreaming with named table") {
    val spark = session
    val df = spark.readStream.table("my_table")
    val read = extractRead(df)
    read.getIsStreaming shouldBe true
    read.hasNamedTable shouldBe true
    read.getNamedTable.getUnparsedIdentifier shouldBe "my_table"
  }

  test("default format is empty when not specified") {
    val spark = session
    val df = spark.readStream.load()
    val read = extractRead(df)
    read.getIsStreaming shouldBe true
    read.getDataSource.getFormat shouldBe ""
  }

  test("planId is assigned") {
    val spark = session
    val df = spark.readStream.format("rate").load()
    df.relation.hasCommon shouldBe true
  }
