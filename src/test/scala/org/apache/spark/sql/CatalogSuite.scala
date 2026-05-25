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
    val cat = extractCatalog(testCatalog.listTables("mydb", "tbl*").toDF())
    cat.hasListTables shouldBe true
    cat.getListTables.getDbName shouldBe "mydb"
    cat.getListTables.getPattern shouldBe "tbl*"
  }

  test("listFunctions with dbName and pattern") {
    val cat = extractCatalog(testCatalog.listFunctions("mydb", "fn*").toDF())
    cat.hasListFunctions shouldBe true
    cat.getListFunctions.getDbName shouldBe "mydb"
    cat.getListFunctions.getPattern shouldBe "fn*"
  }

  // ---------------------------------------------------------------------------
  // Get operations — proto-backed
  //
  // Get methods are eager (call `.head()`); unit tests cannot run RPC, so we
  // exercise the proto-construction path directly via `catalogDf` instead of
  // calling `getX(...)` (which would attempt to fetch).
  // ---------------------------------------------------------------------------

  test("getFunction with dbName") {
    val df = testCatalog.catalogDf(_.setGetFunction(
      GetFunction.newBuilder().setFunctionName("my_fn").setDbName("mydb").build()
    ))
    val cat = extractCatalog(df)
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

  // The 2-arg `createTable(tableName, path)` form now reads `spark.sql.sources.default`
  // from session config (matching upstream); it is exercised end-to-end in
  // CatalogIntegrationSuite. The 3-arg form below covers the proto-construction path.

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
  // Proto-based sanity checks (upstream proto only)
  // ---------------------------------------------------------------------------

  test("listDatabases with no args") {
    val cat = extractCatalog(testCatalog.listDatabases().toDF())
    cat.hasListDatabases shouldBe true
    cat.getListDatabases.hasPattern shouldBe false
  }

  test("listDatabases with pattern") {
    val cat = extractCatalog(testCatalog.listDatabases("db*").toDF())
    cat.getListDatabases.getPattern shouldBe "db*"
  }

  test("listTables with no args") {
    val cat = extractCatalog(testCatalog.listTables().toDF())
    cat.hasListTables shouldBe true
  }

  test("listTables with dbName") {
    val cat = extractCatalog(testCatalog.listTables("mydb").toDF())
    cat.getListTables.getDbName shouldBe "mydb"
  }

  test("listFunctions with no args") {
    val cat = extractCatalog(testCatalog.listFunctions().toDF())
    cat.hasListFunctions shouldBe true
  }

  test("listFunctions with dbName") {
    val cat = extractCatalog(testCatalog.listFunctions("mydb").toDF())
    cat.getListFunctions.getDbName shouldBe "mydb"
  }

  test("listCatalogs with no args") {
    val cat = extractCatalog(testCatalog.listCatalogs().toDF())
    cat.hasListCatalogs shouldBe true
  }

  test("listCatalogs with pattern") {
    val cat = extractCatalog(testCatalog.listCatalogs("c*").toDF())
    cat.getListCatalogs.getPattern shouldBe "c*"
  }

  test("getDatabase") {
    val df = testCatalog.catalogDf(_.setGetDatabase(
      GetDatabase.newBuilder().setDbName("mydb").build()
    ))
    val cat = extractCatalog(df)
    cat.hasGetDatabase shouldBe true
    cat.getGetDatabase.getDbName shouldBe "mydb"
  }

  test("getTable single arg") {
    val df = testCatalog.catalogDf(_.setGetTable(
      GetTable.newBuilder().setTableName("t1").build()
    ))
    val cat = extractCatalog(df)
    cat.hasGetTable shouldBe true
    cat.getGetTable.getTableName shouldBe "t1"
  }

  test("getTable with dbName") {
    val df = testCatalog.catalogDf(_.setGetTable(
      GetTable.newBuilder().setTableName("t1").setDbName("mydb").build()
    ))
    val cat = extractCatalog(df)
    cat.getGetTable.getTableName shouldBe "t1"
    cat.getGetTable.getDbName shouldBe "mydb"
  }

  test("getFunction single arg") {
    val df = testCatalog.catalogDf(_.setGetFunction(
      GetFunction.newBuilder().setFunctionName("fn1").build()
    ))
    val cat = extractCatalog(df)
    cat.hasGetFunction shouldBe true
    cat.getGetFunction.getFunctionName shouldBe "fn1"
  }

  test("listColumns single arg") {
    val cat = extractCatalog(testCatalog.listColumns("t1").toDF())
    cat.hasListColumns shouldBe true
    cat.getListColumns.getTableName shouldBe "t1"
  }

  test("listColumns with dbName") {
    val cat = extractCatalog(testCatalog.listColumns("mydb", "t1").toDF())
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

  test("catalogExists method exists with correct signature") {
    val method = classOf[Catalog].getMethod("catalogExists", classOf[String])
    method should not be null
    method.getReturnType shouldBe classOf[Boolean]
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
    val df1 = catalog.listDatabases().toDF()
    val df2 = catalog.listTables().toDF()
    val df3 = catalog.listFunctions().toDF()
    val ids = Seq(
      df1.relation.getCommon.getPlanId,
      df2.relation.getCommon.getPlanId,
      df3.relation.getCommon.getPlanId
    )
    ids.distinct.size shouldBe 3
  }

  // --- R3-4: SQL injection prevention tests ---

  test("listViews escapes single quotes in pattern") {
    val df = testCatalog.listViews("mydb", "test' OR '1'='1")
    val query = df.relation.getSql.getQuery
    query should not include "test' OR"
    query should include("test'' OR ''1''=''1")
  }

  test("quoteIdent escapes backticks in identifier") {
    val df = testCatalog.listPartitions("table`; DROP TABLE foo; --")
    val query = df.relation.getSql.getQuery
    query should include("`table``; DROP TABLE foo; --`")
    query should not include "table`;"
  }

  test("quoteIdent quotes each part of multi-part name") {
    val df = testCatalog.listPartitions("db.table")
    val query = df.relation.getSql.getQuery
    query should include("`db`.`table`")
  }

  test("quoteIdent rejects empty string") {
    intercept[IllegalArgumentException] {
      testCatalog.listPartitions("")
    }
  }

  test("quoteIdent handles trailing dot (split -1 preserves empty trailing parts)") {
    val df = testCatalog.listPartitions("db.")
    val query = df.relation.getSql.getQuery
    query should include("`db`.``")
  }

  test("escapeSqlLiteral escapes backslashes (Spark parser processes escape sequences)") {
    val df = testCatalog.listViews("mydb", "path\\to\\file")
    val query = df.relation.getSql.getQuery
    query should include("path\\\\to\\\\file")
  }

  // ---------------------------------------------------------------------------
  // R47: getCreateTableString puts AS SERDE after the table name (grammar fix)
  // ---------------------------------------------------------------------------

  test("buildShowCreateTableSql places AS SERDE after the identifier (R47)") {
    val cat = testCatalog
    cat.buildShowCreateTableSql("`hiveTbl`", asSerde = true) shouldBe
      "SHOW CREATE TABLE `hiveTbl` AS SERDE"
  }

  test("buildShowCreateTableSql omits AS SERDE when asSerde is false") {
    val cat = testCatalog
    cat.buildShowCreateTableSql("`tbl`", asSerde = false) shouldBe
      "SHOW CREATE TABLE `tbl`"
  }
