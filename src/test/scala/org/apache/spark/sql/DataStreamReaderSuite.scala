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

  // ---------- P0 API: format convenience methods ----------

  test("json(path) sets format to json and path") {
    val spark = session
    val df = spark.readStream.json("/data/stream.json")
    val read = extractRead(df)
    read.getIsStreaming shouldBe true
    read.getDataSource.getFormat shouldBe "json"
    read.getDataSource.getPaths(0) shouldBe "/data/stream.json"
  }

  test("csv(path) sets format to csv and path") {
    val spark = session
    val df = spark.readStream.csv("/data/stream.csv")
    val read = extractRead(df)
    read.getDataSource.getFormat shouldBe "csv"
    read.getDataSource.getPaths(0) shouldBe "/data/stream.csv"
  }

  test("parquet(path) sets format to parquet and path") {
    val spark = session
    val df = spark.readStream.parquet("/data/stream.parquet")
    val read = extractRead(df)
    read.getDataSource.getFormat shouldBe "parquet"
    read.getDataSource.getPaths(0) shouldBe "/data/stream.parquet"
  }

  test("orc(path) sets format to orc and path") {
    val spark = session
    val df = spark.readStream.orc("/data/stream.orc")
    val read = extractRead(df)
    read.getDataSource.getFormat shouldBe "orc"
    read.getDataSource.getPaths(0) shouldBe "/data/stream.orc"
  }

  test("text(path) sets format to text and path") {
    val spark = session
    val df = spark.readStream.text("/data/stream.txt")
    val read = extractRead(df)
    read.getDataSource.getFormat shouldBe "text"
    read.getDataSource.getPaths(0) shouldBe "/data/stream.txt"
  }

  test("xml(path) sets format to xml and path") {
    val spark = session
    val df = spark.readStream.xml("/data/stream.xml")
    val read = extractRead(df)
    read.getDataSource.getFormat shouldBe "xml"
    read.getDataSource.getPaths(0) shouldBe "/data/stream.xml"
  }

  test("textFile(path) sets format to text and selects value column") {
    val spark = session
    val df = spark.readStream.textFile("/data/stream.txt")
    val rel = df.relation
    rel.hasProject shouldBe true
  }

  test("typed option overloads set options correctly") {
    val spark = session
    val df = spark.readStream
      .format("kafka")
      .option("boolOpt", true)
      .option("longOpt", 42L)
      .option("doubleOpt", 3.14)
      .load()
    val opts = extractRead(df).getDataSource.getOptionsMap
    opts.get("boolOpt") shouldBe "true"
    opts.get("longOpt") shouldBe "42"
    opts.get("doubleOpt") shouldBe "3.14"
  }

  test("schema(StructType) sets DDL schema string") {
    val spark = session
    val schema = types.StructType(Seq(
      types.StructField("name", types.StringType),
      types.StructField("age", types.IntegerType)
    ))
    val df = spark.readStream.format("csv").schema(schema).load()
    val read = extractRead(df)
    read.getDataSource.getSchema should not be empty
  }
