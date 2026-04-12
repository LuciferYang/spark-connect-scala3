package org.apache.spark.sql

import scala.annotation.nowarn

import org.apache.spark.connect.proto.{Catalog as ProtoCatalog, StorageLevel as _, *}
import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.*

/** Tests for deprecated createExternalTable API (delegates to createTable). */
@nowarn("msg=deprecated")
class DeprecationCatalogSuite extends AnyFunSuite with Matchers:

  private def testSession: SparkSession = SparkSession(null)

  private def testCatalog: Catalog = Catalog(testSession)

  private def extractCatalog(df: DataFrame): ProtoCatalog =
    df.relation.getCatalog

  test("createExternalTable delegates to createTable") {
    val cat1 = extractCatalog(testCatalog.createExternalTable("t", "/p"))
    cat1.hasCreateTable shouldBe true
    cat1.getCreateTable.getTableName shouldBe "t"
    cat1.getCreateTable.getOptionsMap.get("path") shouldBe "/p"
  }

  test("createExternalTable with path and source") {
    val cat = extractCatalog(testCatalog.createExternalTable("t", "/p", "csv"))
    cat.hasCreateTable shouldBe true
    cat.getCreateTable.getTableName shouldBe "t"
    cat.getCreateTable.getOptionsMap.get("path") shouldBe "/p"
    cat.getCreateTable.getSource shouldBe "csv"
  }

  test("createExternalTable with source and options") {
    val opts = Map("header" -> "true")
    val cat = extractCatalog(testCatalog.createExternalTable("t", "csv", opts))
    cat.hasCreateTable shouldBe true
    cat.getCreateTable.getTableName shouldBe "t"
    cat.getCreateTable.getSource shouldBe "csv"
    cat.getCreateTable.getOptionsMap.asScala shouldBe opts
  }

  test("createExternalTable with source, schema, and options") {
    val schema = StructType(Seq(StructField("id", IntegerType)))
    val opts = Map("header" -> "true")
    val cat = extractCatalog(testCatalog.createExternalTable("t", "csv", schema, opts))
    cat.hasCreateTable shouldBe true
    val ct = cat.getCreateTable
    ct.getTableName shouldBe "t"
    ct.getSource shouldBe "csv"
    ct.hasSchema shouldBe true
    ct.getOptionsMap.asScala shouldBe opts
  }
