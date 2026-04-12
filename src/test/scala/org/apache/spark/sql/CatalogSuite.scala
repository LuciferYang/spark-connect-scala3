package org.apache.spark.sql

import org.apache.spark.connect.proto.{Catalog as ProtoCatalog, StorageLevel as _, *}
import org.apache.spark.sql.connect.client.DataTypeProtoConverter
import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.*

class CatalogSuite extends AnyFunSuite with Matchers:

  private def testSession: SparkSession = SparkSession(null)

  private def testCatalog: Catalog = Catalog(testSession)

  /** Extract the ProtoCatalog from a DataFrame produced by a catalog method. */
  private def extractCatalog(df: DataFrame): ProtoCatalog =
    df.relation.getCatalog

  // ---------------------------------------------------------------------------
  // List operations — proto-backed
  // ---------------------------------------------------------------------------

  test("listTables with dbName and pattern") {
    val cat = extractCatalog(testCatalog.listTables("mydb", "tbl*"))
    cat.hasListTables shouldBe true
    cat.getListTables.getDbName shouldBe "mydb"
    cat.getListTables.getPattern shouldBe "tbl*"
  }

  test("listFunctions with dbName and pattern") {
    val cat = extractCatalog(testCatalog.listFunctions("mydb", "fn*"))
    cat.hasListFunctions shouldBe true
    cat.getListFunctions.getDbName shouldBe "mydb"
    cat.getListFunctions.getPattern shouldBe "fn*"
  }

  // ---------------------------------------------------------------------------
  // Get operations — proto-backed
  // ---------------------------------------------------------------------------

  test("getFunction with dbName") {
    val cat = extractCatalog(testCatalog.getFunction("my_fn", "mydb"))
    cat.hasGetFunction shouldBe true
    cat.getGetFunction.getFunctionName shouldBe "my_fn"
    cat.getGetFunction.getDbName shouldBe "mydb"
  }

  // ---------------------------------------------------------------------------
  // Cache management
  // ---------------------------------------------------------------------------

  test("cacheTable with storageLevel") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setCacheTable(
      CacheTable.newBuilder()
        .setTableName("t")
        .setStorageLevel(StorageLevel.MEMORY_AND_DISK.toProto)
        .build()
    ))
    val cat = extractCatalog(df)
    cat.hasCacheTable shouldBe true
    cat.getCacheTable.getTableName shouldBe "t"
    cat.getCacheTable.hasStorageLevel shouldBe true
    val sl = cat.getCacheTable.getStorageLevel
    sl.getUseDisk shouldBe true
    sl.getUseMemory shouldBe true
    sl.getDeserialized shouldBe true
  }

  // ---------------------------------------------------------------------------
  // Create table
  // ---------------------------------------------------------------------------

  test("createTable with tableName and path") {
    val cat = extractCatalog(testCatalog.createTable("t1", "/data/path"))
    cat.hasCreateTable shouldBe true
    cat.getCreateTable.getTableName shouldBe "t1"
    cat.getCreateTable.getOptionsMap.get("path") shouldBe "/data/path"
    cat.getCreateTable.getSource shouldBe "parquet"
  }

  test("createTable with tableName, path, and source") {
    val cat = extractCatalog(testCatalog.createTable("t1", "/data/path", "parquet"))
    cat.hasCreateTable shouldBe true
    cat.getCreateTable.getTableName shouldBe "t1"
    cat.getCreateTable.getOptionsMap.get("path") shouldBe "/data/path"
    cat.getCreateTable.getSource shouldBe "parquet"
  }

  test("createTable with source and options") {
    val opts = Map("path" -> "/data/path", "mergeSchema" -> "true")
    val cat = extractCatalog(testCatalog.createTable("t1", "parquet", opts))
    cat.hasCreateTable shouldBe true
    cat.getCreateTable.getTableName shouldBe "t1"
    cat.getCreateTable.getSource shouldBe "parquet"
    cat.getCreateTable.getOptionsMap.asScala shouldBe opts
  }

  test("createTable with source, schema, and options") {
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType)
    ))
    val opts = Map("path" -> "/data")
    val cat = extractCatalog(testCatalog.createTable("t1", "parquet", schema, opts))
    cat.hasCreateTable shouldBe true
    val ct = cat.getCreateTable
    ct.getTableName shouldBe "t1"
    ct.getSource shouldBe "parquet"
    ct.hasSchema shouldBe true
    ct.getOptionsMap.asScala shouldBe opts
    val parsedSchema = DataTypeProtoConverter.fromProto(ct.getSchema)
    parsedSchema shouldBe schema
  }

  test("createTable with source, description, schema, and options") {
    val schema = StructType(Seq(StructField("x", LongType)))
    val opts = Map("compression" -> "snappy")
    val cat = extractCatalog(
      testCatalog.createTable("t1", "orc", "my table desc", schema, opts)
    )
    val ct = cat.getCreateTable
    ct.getTableName shouldBe "t1"
    ct.getSource shouldBe "orc"
    ct.getDescription shouldBe "my table desc"
    ct.hasSchema shouldBe true
    ct.getOptionsMap.asScala shouldBe opts
  }

  // ---------------------------------------------------------------------------
  // Create external table (deprecated, delegates to createTable)
  // ---------------------------------------------------------------------------

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

  // ---------------------------------------------------------------------------
  // Proto-based sanity checks (upstream proto only)
  // ---------------------------------------------------------------------------

  test("listDatabases with no args") {
    val cat = extractCatalog(testCatalog.listDatabases())
    cat.hasListDatabases shouldBe true
    cat.getListDatabases.hasPattern shouldBe false
  }

  test("listDatabases with pattern") {
    val cat = extractCatalog(testCatalog.listDatabases("db*"))
    cat.getListDatabases.getPattern shouldBe "db*"
  }

  test("listTables with no args") {
    val cat = extractCatalog(testCatalog.listTables())
    cat.hasListTables shouldBe true
  }

  test("listTables with dbName") {
    val cat = extractCatalog(testCatalog.listTables("mydb"))
    cat.getListTables.getDbName shouldBe "mydb"
  }

  test("listFunctions with no args") {
    val cat = extractCatalog(testCatalog.listFunctions())
    cat.hasListFunctions shouldBe true
  }

  test("listFunctions with dbName") {
    val cat = extractCatalog(testCatalog.listFunctions("mydb"))
    cat.getListFunctions.getDbName shouldBe "mydb"
  }

  test("listCatalogs with no args") {
    val cat = extractCatalog(testCatalog.listCatalogs())
    cat.hasListCatalogs shouldBe true
  }

  test("listCatalogs with pattern") {
    val cat = extractCatalog(testCatalog.listCatalogs("c*"))
    cat.getListCatalogs.getPattern shouldBe "c*"
  }

  test("getDatabase") {
    val cat = extractCatalog(testCatalog.getDatabase("mydb"))
    cat.hasGetDatabase shouldBe true
    cat.getGetDatabase.getDbName shouldBe "mydb"
  }

  test("getTable single arg") {
    val cat = extractCatalog(testCatalog.getTable("t1"))
    cat.hasGetTable shouldBe true
    cat.getGetTable.getTableName shouldBe "t1"
  }

  test("getTable with dbName") {
    val cat = extractCatalog(testCatalog.getTable("t1", "mydb"))
    cat.getGetTable.getTableName shouldBe "t1"
    cat.getGetTable.getDbName shouldBe "mydb"
  }

  test("getFunction single arg") {
    val cat = extractCatalog(testCatalog.getFunction("fn1"))
    cat.hasGetFunction shouldBe true
    cat.getGetFunction.getFunctionName shouldBe "fn1"
  }

  test("listColumns single arg") {
    val cat = extractCatalog(testCatalog.listColumns("t1"))
    cat.hasListColumns shouldBe true
    cat.getListColumns.getTableName shouldBe "t1"
  }

  test("listColumns with dbName") {
    val cat = extractCatalog(testCatalog.listColumns("t1", "mydb"))
    cat.getListColumns.getTableName shouldBe "t1"
    cat.getListColumns.getDbName shouldBe "mydb"
  }

  // ---------------------------------------------------------------------------
  // Proto construction for collect()-dependent methods
  // ---------------------------------------------------------------------------

  test("currentDatabase builds CurrentDatabase proto") {
    val df = testCatalog.catalogDf(_.setCurrentDatabase(CurrentDatabase.getDefaultInstance))
    extractCatalog(df).hasCurrentDatabase shouldBe true
  }

  test("setCurrentDatabase builds SetCurrentDatabase proto") {
    val df = testCatalog.catalogDf(_.setSetCurrentDatabase(
      SetCurrentDatabase.newBuilder().setDbName("testdb").build()
    ))
    val cat = extractCatalog(df)
    cat.hasSetCurrentDatabase shouldBe true
    cat.getSetCurrentDatabase.getDbName shouldBe "testdb"
  }

  test("currentCatalog builds CurrentCatalog proto") {
    val df = testCatalog.catalogDf(_.setCurrentCatalog(CurrentCatalog.getDefaultInstance))
    extractCatalog(df).hasCurrentCatalog shouldBe true
  }

  test("setCurrentCatalog builds SetCurrentCatalog proto") {
    val df = testCatalog.catalogDf(_.setSetCurrentCatalog(
      SetCurrentCatalog.newBuilder().setCatalogName("my_catalog").build()
    ))
    val cat = extractCatalog(df)
    cat.hasSetCurrentCatalog shouldBe true
    cat.getSetCurrentCatalog.getCatalogName shouldBe "my_catalog"
  }

  test("databaseExists builds DatabaseExists proto") {
    val df = testCatalog.catalogDf(_.setDatabaseExists(
      DatabaseExists.newBuilder().setDbName("testdb").build()
    ))
    val cat = extractCatalog(df)
    cat.hasDatabaseExists shouldBe true
    cat.getDatabaseExists.getDbName shouldBe "testdb"
  }

  test("tableExists builds TableExists proto") {
    val df = testCatalog.catalogDf(_.setTableExists(
      TableExists.newBuilder().setTableName("t1").build()
    ))
    extractCatalog(df).hasTableExists shouldBe true
    extractCatalog(df).getTableExists.getTableName shouldBe "t1"
  }

  test("tableExists with dbName builds TableExists proto") {
    val df = testCatalog.catalogDf(_.setTableExists(
      TableExists.newBuilder().setTableName("t1").setDbName("mydb").build()
    ))
    val cat = extractCatalog(df)
    cat.getTableExists.getTableName shouldBe "t1"
    cat.getTableExists.getDbName shouldBe "mydb"
  }

  test("functionExists builds FunctionExists proto") {
    val df = testCatalog.catalogDf(_.setFunctionExists(
      FunctionExists.newBuilder().setFunctionName("my_fn").build()
    ))
    extractCatalog(df).hasFunctionExists shouldBe true
    extractCatalog(df).getFunctionExists.getFunctionName shouldBe "my_fn"
  }

  test("functionExists with dbName builds FunctionExists proto") {
    val df = testCatalog.catalogDf(_.setFunctionExists(
      FunctionExists.newBuilder().setFunctionName("my_fn").setDbName("mydb").build()
    ))
    val cat = extractCatalog(df)
    cat.getFunctionExists.getFunctionName shouldBe "my_fn"
    cat.getFunctionExists.getDbName shouldBe "mydb"
  }

  test("isCached builds IsCached proto") {
    val df = testCatalog.catalogDf(_.setIsCached(
      IsCached.newBuilder().setTableName("t1").build()
    ))
    extractCatalog(df).hasIsCached shouldBe true
    extractCatalog(df).getIsCached.getTableName shouldBe "t1"
  }

  test("cacheTable builds CacheTable proto") {
    val df = testCatalog.catalogDf(_.setCacheTable(
      CacheTable.newBuilder().setTableName("t1").build()
    ))
    val cat = extractCatalog(df)
    cat.hasCacheTable shouldBe true
    cat.getCacheTable.getTableName shouldBe "t1"
    cat.getCacheTable.hasStorageLevel shouldBe false
  }

  test("uncacheTable builds UncacheTable proto") {
    val df = testCatalog.catalogDf(_.setUncacheTable(
      UncacheTable.newBuilder().setTableName("t1").build()
    ))
    extractCatalog(df).hasUncacheTable shouldBe true
    extractCatalog(df).getUncacheTable.getTableName shouldBe "t1"
  }

  test("clearCache builds ClearCache proto") {
    val df = testCatalog.catalogDf(_.setClearCache(ClearCache.getDefaultInstance))
    extractCatalog(df).hasClearCache shouldBe true
  }

  test("dropTempView builds DropTempView proto") {
    val df = testCatalog.catalogDf(_.setDropTempView(
      DropTempView.newBuilder().setViewName("temp_view").build()
    ))
    val cat = extractCatalog(df)
    cat.hasDropTempView shouldBe true
    cat.getDropTempView.getViewName shouldBe "temp_view"
  }

  test("dropGlobalTempView builds DropGlobalTempView proto") {
    val df = testCatalog.catalogDf(_.setDropGlobalTempView(
      DropGlobalTempView.newBuilder().setViewName("global_temp_view").build()
    ))
    val cat = extractCatalog(df)
    cat.hasDropGlobalTempView shouldBe true
    cat.getDropGlobalTempView.getViewName shouldBe "global_temp_view"
  }

  test("refreshTable builds RefreshTable proto") {
    val df = testCatalog.catalogDf(_.setRefreshTable(
      RefreshTable.newBuilder().setTableName("my_table").build()
    ))
    extractCatalog(df).hasRefreshTable shouldBe true
    extractCatalog(df).getRefreshTable.getTableName shouldBe "my_table"
  }

  test("refreshByPath builds RefreshByPath proto") {
    val df = testCatalog.catalogDf(_.setRefreshByPath(
      RefreshByPath.newBuilder().setPath("/data/path").build()
    ))
    extractCatalog(df).hasRefreshByPath shouldBe true
    extractCatalog(df).getRefreshByPath.getPath shouldBe "/data/path"
  }

  test("recoverPartitions builds RecoverPartitions proto") {
    val df = testCatalog.catalogDf(_.setRecoverPartitions(
      RecoverPartitions.newBuilder().setTableName("my_table").build()
    ))
    extractCatalog(df).hasRecoverPartitions shouldBe true
    extractCatalog(df).getRecoverPartitions.getTableName shouldBe "my_table"
  }

  // ---------------------------------------------------------------------------
  // Structural tests
  // ---------------------------------------------------------------------------

  test("catalogDf sets plan ID and Relation common") {
    val df = testCatalog.catalogDf(_.setListDatabases(ListDatabases.getDefaultInstance))
    val rel = df.relation
    rel.hasCommon shouldBe true
    rel.getCommon.hasPlanId shouldBe true
    rel.hasCatalog shouldBe true
  }

  test("each catalogDf call increments plan ID") {
    val session = testSession
    val catalog = Catalog(session)
    val df1 = catalog.catalogDf(_.setListDatabases(ListDatabases.getDefaultInstance))
    val df2 = catalog.catalogDf(_.setListTables(ListTables.getDefaultInstance))
    df2.relation.getCommon.getPlanId shouldBe >(df1.relation.getCommon.getPlanId)
  }

  test("SparkSession.catalog returns a Catalog instance") {
    testSession.catalog shouldBe a[Catalog]
  }

  test("multiple catalog operations on same session produce unique plan IDs") {
    val session = testSession
    val catalog = Catalog(session)
    val df1 = catalog.listDatabases()
    val df2 = catalog.listTables()
    val df3 = catalog.listFunctions()
    val ids = Seq(
      df1.relation.getCommon.getPlanId,
      df2.relation.getCommon.getPlanId,
      df3.relation.getCommon.getPlanId
    )
    ids.distinct.size shouldBe 3
  }
