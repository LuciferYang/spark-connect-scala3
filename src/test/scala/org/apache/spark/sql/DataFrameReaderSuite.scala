package org.apache.spark.sql

import org.apache.spark.connect.proto.*
import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.*

/** Proto-only tests for DataFrameReader — no live Spark Connect server needed. */
class DataFrameReaderSuite extends AnyFunSuite with Matchers:

  private def stubSession: SparkSession = SparkSession(null)

  // ---------------------------------------------------------------------------
  // format()
  // ---------------------------------------------------------------------------

  test("default format is parquet") {
    val df = stubSession.read.load()
    val ds = df.relation.getRead.getDataSource
    ds.getFormat shouldBe "parquet"
  }

  test("format sets the source") {
    val df = stubSession.read.format("json").load()
    val ds = df.relation.getRead.getDataSource
    ds.getFormat shouldBe "json"
  }

  test("format can be changed multiple times") {
    val df = stubSession.read.format("json").format("csv").load()
    val ds = df.relation.getRead.getDataSource
    ds.getFormat shouldBe "csv"
  }

  // ---------------------------------------------------------------------------
  // option() / options()
  // ---------------------------------------------------------------------------

  test("option(key, String) adds to options map") {
    val df = stubSession.read.option("key1", "value1").load()
    val ds = df.relation.getRead.getDataSource
    ds.getOptionsMap.asScala shouldBe Map("key1" -> "value1")
  }

  test("option(key, Boolean) converts to string") {
    val df = stubSession.read.option("header", true).load()
    val ds = df.relation.getRead.getDataSource
    ds.getOptionsMap.get("header") shouldBe "true"
  }

  test("option(key, Long) converts to string") {
    val df = stubSession.read.option("maxRecords", 100L).load()
    val ds = df.relation.getRead.getDataSource
    ds.getOptionsMap.get("maxRecords") shouldBe "100"
  }

  test("option(key, Double) converts to string") {
    val df = stubSession.read.option("threshold", 0.5).load()
    val ds = df.relation.getRead.getDataSource
    ds.getOptionsMap.get("threshold") shouldBe "0.5"
  }

  test("options(Map) merges options") {
    val df = stubSession.read
      .option("a", "1")
      .options(Map("b" -> "2", "c" -> "3"))
      .load()
    val ds = df.relation.getRead.getDataSource
    ds.getOptionsMap.asScala shouldBe Map("a" -> "1", "b" -> "2", "c" -> "3")
  }

  test("later option overrides earlier") {
    val df = stubSession.read
      .option("key", "old")
      .option("key", "new")
      .load()
    val ds = df.relation.getRead.getDataSource
    ds.getOptionsMap.get("key") shouldBe "new"
  }

  // ---------------------------------------------------------------------------
  // schema()
  // ---------------------------------------------------------------------------

  test("schema(String) sets user schema") {
    val df = stubSession.read.schema("id BIGINT, name STRING").load()
    val ds = df.relation.getRead.getDataSource
    ds.getSchema shouldBe "id BIGINT, name STRING"
  }

  test("schema(StructType) sets DDL schema") {
    val st = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType)
    ))
    val df = stubSession.read.schema(st).load()
    val ds = df.relation.getRead.getDataSource
    ds.getSchema shouldBe st.toDDL
  }

  // ---------------------------------------------------------------------------
  // load()
  // ---------------------------------------------------------------------------

  test("load() with no path sets no paths") {
    val df = stubSession.read.load()
    val ds = df.relation.getRead.getDataSource
    ds.getPathsCount shouldBe 0
  }

  test("load(path) sets single path") {
    val df = stubSession.read.load("/data/input")
    val ds = df.relation.getRead.getDataSource
    ds.getPathsList.asScala shouldBe Seq("/data/input")
  }

  test("load(Seq) sets multiple paths") {
    val df = stubSession.read.load(Seq("/path1", "/path2"))
    val ds = df.relation.getRead.getDataSource
    ds.getPathsList.asScala shouldBe Seq("/path1", "/path2")
  }

  // ---------------------------------------------------------------------------
  // table()
  // ---------------------------------------------------------------------------

  test("table delegates to SparkSession.table") {
    val df = stubSession.read.table("my_table")
    df.relation.hasRead shouldBe true
    df.relation.getRead.hasNamedTable shouldBe true
    df.relation.getRead.getNamedTable.getUnparsedIdentifier shouldBe "my_table"
  }

  // ---------------------------------------------------------------------------
  // Convenience read methods
  // ---------------------------------------------------------------------------

  test("json sets format and paths") {
    val df = stubSession.read.json("/data.json")
    val ds = df.relation.getRead.getDataSource
    ds.getFormat shouldBe "json"
    ds.getPathsList.asScala shouldBe Seq("/data.json")
  }

  test("parquet sets format and paths") {
    val df = stubSession.read.parquet("/data.parquet")
    val ds = df.relation.getRead.getDataSource
    ds.getFormat shouldBe "parquet"
    ds.getPathsList.asScala shouldBe Seq("/data.parquet")
  }

  test("orc sets format and paths") {
    val df = stubSession.read.orc("/data.orc")
    val ds = df.relation.getRead.getDataSource
    ds.getFormat shouldBe "orc"
    ds.getPathsList.asScala shouldBe Seq("/data.orc")
  }

  test("csv sets format and paths") {
    val df = stubSession.read.csv("/data.csv")
    val ds = df.relation.getRead.getDataSource
    ds.getFormat shouldBe "csv"
    ds.getPathsList.asScala shouldBe Seq("/data.csv")
  }

  test("text sets format and paths") {
    val df = stubSession.read.text("/data.txt")
    val ds = df.relation.getRead.getDataSource
    ds.getFormat shouldBe "text"
    ds.getPathsList.asScala shouldBe Seq("/data.txt")
  }

  test("convenience method with multiple paths") {
    val df = stubSession.read.json("/a.json", "/b.json")
    val ds = df.relation.getRead.getDataSource
    ds.getFormat shouldBe "json"
    ds.getPathsList.asScala shouldBe Seq("/a.json", "/b.json")
  }

  // ---------------------------------------------------------------------------
  // Chaining
  // ---------------------------------------------------------------------------

  test("chaining format, option, schema, load(path)") {
    val df = stubSession.read
      .format("csv")
      .option("header", true)
      .option("delimiter", ";")
      .schema("id INT, name STRING")
      .load("/data.csv")
    val ds = df.relation.getRead.getDataSource
    ds.getFormat shouldBe "csv"
    ds.getOptionsMap.get("header") shouldBe "true"
    ds.getOptionsMap.get("delimiter") shouldBe ";"
    ds.getSchema shouldBe "id INT, name STRING"
    ds.getPathsList.asScala shouldBe Seq("/data.csv")
  }

  // ---------------------------------------------------------------------------
  // Read produces valid Relation
  // ---------------------------------------------------------------------------

  test("load produces a relation with Read and DataSource") {
    val df = stubSession.read.format("parquet").load("/path")
    df.relation.hasRead shouldBe true
    df.relation.getRead.hasDataSource shouldBe true
  }

  test("load sets plan id via session") {
    val session = stubSession
    val df = session.read.load()
    df.relation.hasCommon shouldBe true
    df.relation.getCommon.getPlanId should be >= 0L
  }

  // ---------------------------------------------------------------------------
  // P1: xml convenience method
  // ---------------------------------------------------------------------------

  test("xml sets format to xml") {
    val df = stubSession.read.xml("/data.xml")
    val ds = df.relation.getRead.getDataSource
    ds.getFormat shouldBe "xml"
    ds.getPathsList.asScala shouldBe Seq("/data.xml")
  }

  test("xml with multiple paths") {
    val df = stubSession.read.xml("/a.xml", "/b.xml")
    val ds = df.relation.getRead.getDataSource
    ds.getFormat shouldBe "xml"
    ds.getPathsList.asScala shouldBe Seq("/a.xml", "/b.xml")
  }

  // ---------------------------------------------------------------------------
  // P1: textFile convenience method
  // ---------------------------------------------------------------------------

  test("textFile selects value column") {
    val df = stubSession.read.textFile("/data.txt")
    // textFile wraps text().select("value"), so the relation should be a Project
    val rel = df.relation
    rel.hasProject shouldBe true
    val proj = rel.getProject
    proj.getExpressionsCount shouldBe 1
    val expr = proj.getExpressions(0)
    expr.hasUnresolvedAttribute shouldBe true
    expr.getUnresolvedAttribute.getUnparsedIdentifier shouldBe "value"
    // The input to the project should be a Read with text format
    proj.getInput.hasRead shouldBe true
    proj.getInput.getRead.getDataSource.getFormat shouldBe "text"
  }
