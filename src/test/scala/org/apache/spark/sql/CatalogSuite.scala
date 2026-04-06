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
  // List operations — new overloads
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

  test("listCachedTables") {
    val cat = extractCatalog(testCatalog.listCachedTables())
    cat.hasListCachedTables shouldBe true
  }

  test("listPartitions") {
    val cat = extractCatalog(testCatalog.listPartitions("my_table"))
    cat.hasListPartitions shouldBe true
    cat.getListPartitions.getTableName shouldBe "my_table"
  }

  test("listViews no args") {
    val cat = extractCatalog(testCatalog.listViews())
    cat.hasListViews shouldBe true
    cat.getListViews.hasDbName shouldBe false
    cat.getListViews.hasPattern shouldBe false
  }

  test("listViews with dbName") {
    val cat = extractCatalog(testCatalog.listViews("mydb"))
    cat.hasListViews shouldBe true
    cat.getListViews.getDbName shouldBe "mydb"
    cat.getListViews.hasPattern shouldBe false
  }

  test("listViews with dbName and pattern") {
    val cat = extractCatalog(testCatalog.listViews("mydb", "v*"))
    cat.hasListViews shouldBe true
    cat.getListViews.getDbName shouldBe "mydb"
    cat.getListViews.getPattern shouldBe "v*"
  }

  // ---------------------------------------------------------------------------
  // Get operations — new overloads and methods
  // ---------------------------------------------------------------------------

  test("getFunction with dbName") {
    val cat = extractCatalog(testCatalog.getFunction("my_fn", "mydb"))
    cat.hasGetFunction shouldBe true
    cat.getGetFunction.getFunctionName shouldBe "my_fn"
    cat.getGetFunction.getDbName shouldBe "mydb"
  }

  test("getTableProperties") {
    val cat = extractCatalog(testCatalog.getTableProperties("my_table"))
    cat.hasGetTableProperties shouldBe true
    cat.getGetTableProperties.getTableName shouldBe "my_table"
  }

  // ---------------------------------------------------------------------------
  // Cache management — new overload
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
    // Verify schema roundtrip
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

  // ---------------------------------------------------------------------------
  // Database management
  // ---------------------------------------------------------------------------

  test("createDatabase") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setCreateDatabase(
      CreateDatabase.newBuilder()
        .setDbName("testdb")
        .setIfNotExists(true)
        .putAllProperties(Map("k" -> "v").asJava)
        .build()
    ))
    val cat = extractCatalog(df)
    cat.hasCreateDatabase shouldBe true
    cat.getCreateDatabase.getDbName shouldBe "testdb"
    cat.getCreateDatabase.getIfNotExists shouldBe true
    cat.getCreateDatabase.getPropertiesMap.asScala shouldBe Map("k" -> "v")
  }

  test("dropDatabase") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setDropDatabase(
      DropDatabase.newBuilder()
        .setDbName("testdb")
        .setIfExists(true)
        .setCascade(true)
        .build()
    ))
    val cat = extractCatalog(df)
    cat.hasDropDatabase shouldBe true
    cat.getDropDatabase.getDbName shouldBe "testdb"
    cat.getDropDatabase.getIfExists shouldBe true
    cat.getDropDatabase.getCascade shouldBe true
  }

  // ---------------------------------------------------------------------------
  // Drop table / view
  // ---------------------------------------------------------------------------

  test("dropTable") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setDropTable(
      DropTable.newBuilder()
        .setTableName("my_table")
        .setIfExists(true)
        .setPurge(true)
        .build()
    ))
    val cat = extractCatalog(df)
    cat.hasDropTable shouldBe true
    cat.getDropTable.getTableName shouldBe "my_table"
    cat.getDropTable.getIfExists shouldBe true
    cat.getDropTable.getPurge shouldBe true
  }

  test("dropView") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setDropView(
      DropView.newBuilder()
        .setViewName("my_view")
        .setIfExists(true)
        .build()
    ))
    val cat = extractCatalog(df)
    cat.hasDropView shouldBe true
    cat.getDropView.getViewName shouldBe "my_view"
    cat.getDropView.getIfExists shouldBe true
  }

  // ---------------------------------------------------------------------------
  // Truncate / Analyze
  // ---------------------------------------------------------------------------

  test("truncateTable") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setTruncateTable(
      TruncateTable.newBuilder().setTableName("my_table").build()
    ))
    val cat = extractCatalog(df)
    cat.hasTruncateTable shouldBe true
    cat.getTruncateTable.getTableName shouldBe "my_table"
  }

  test("analyzeTable") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setAnalyzeTable(
      AnalyzeTable.newBuilder()
        .setTableName("my_table")
        .setNoScan(true)
        .build()
    ))
    val cat = extractCatalog(df)
    cat.hasAnalyzeTable shouldBe true
    cat.getAnalyzeTable.getTableName shouldBe "my_table"
    cat.getAnalyzeTable.getNoScan shouldBe true
  }

  // ---------------------------------------------------------------------------
  // getCreateTableString
  // ---------------------------------------------------------------------------

  test("getCreateTableString builds correct proto") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setGetCreateTableString(
      GetCreateTableString.newBuilder()
        .setTableName("my_table")
        .setAsSerde(true)
        .build()
    ))
    val cat = extractCatalog(df)
    cat.hasGetCreateTableString shouldBe true
    cat.getGetCreateTableString.getTableName shouldBe "my_table"
    cat.getGetCreateTableString.getAsSerde shouldBe true
  }

  // ---------------------------------------------------------------------------
  // Existing methods — basic sanity checks
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
  // Coverage boost: proto-only tests via catalogDf for methods that call .collect()
  // ---------------------------------------------------------------------------

  test("currentDatabase builds CurrentDatabase proto") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setCurrentDatabase(CurrentDatabase.getDefaultInstance))
    val cat = extractCatalog(df)
    cat.hasCurrentDatabase shouldBe true
  }

  test("setCurrentDatabase builds SetCurrentDatabase proto") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setSetCurrentDatabase(
      SetCurrentDatabase.newBuilder().setDbName("testdb").build()
    ))
    val cat = extractCatalog(df)
    cat.hasSetCurrentDatabase shouldBe true
    cat.getSetCurrentDatabase.getDbName shouldBe "testdb"
  }

  test("currentCatalog builds CurrentCatalog proto") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setCurrentCatalog(CurrentCatalog.getDefaultInstance))
    val cat = extractCatalog(df)
    cat.hasCurrentCatalog shouldBe true
  }

  test("setCurrentCatalog builds SetCurrentCatalog proto") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setSetCurrentCatalog(
      SetCurrentCatalog.newBuilder().setCatalogName("my_catalog").build()
    ))
    val cat = extractCatalog(df)
    cat.hasSetCurrentCatalog shouldBe true
    cat.getSetCurrentCatalog.getCatalogName shouldBe "my_catalog"
  }

  test("databaseExists builds DatabaseExists proto") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setDatabaseExists(
      DatabaseExists.newBuilder().setDbName("testdb").build()
    ))
    val cat = extractCatalog(df)
    cat.hasDatabaseExists shouldBe true
    cat.getDatabaseExists.getDbName shouldBe "testdb"
  }

  test("tableExists builds TableExists proto") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setTableExists(
      TableExists.newBuilder().setTableName("t1").build()
    ))
    val cat = extractCatalog(df)
    cat.hasTableExists shouldBe true
    cat.getTableExists.getTableName shouldBe "t1"
  }

  test("tableExists with dbName builds TableExists proto") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setTableExists(
      TableExists.newBuilder().setTableName("t1").setDbName("mydb").build()
    ))
    val cat = extractCatalog(df)
    cat.hasTableExists shouldBe true
    cat.getTableExists.getTableName shouldBe "t1"
    cat.getTableExists.getDbName shouldBe "mydb"
  }

  test("functionExists builds FunctionExists proto") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setFunctionExists(
      FunctionExists.newBuilder().setFunctionName("my_fn").build()
    ))
    val cat = extractCatalog(df)
    cat.hasFunctionExists shouldBe true
    cat.getFunctionExists.getFunctionName shouldBe "my_fn"
  }

  test("functionExists with dbName builds FunctionExists proto") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setFunctionExists(
      FunctionExists.newBuilder().setFunctionName("my_fn").setDbName("mydb").build()
    ))
    val cat = extractCatalog(df)
    cat.hasFunctionExists shouldBe true
    cat.getFunctionExists.getFunctionName shouldBe "my_fn"
    cat.getFunctionExists.getDbName shouldBe "mydb"
  }

  test("isCached builds IsCached proto") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setIsCached(
      IsCached.newBuilder().setTableName("t1").build()
    ))
    val cat = extractCatalog(df)
    cat.hasIsCached shouldBe true
    cat.getIsCached.getTableName shouldBe "t1"
  }

  test("cacheTable builds CacheTable proto") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setCacheTable(
      CacheTable.newBuilder().setTableName("t1").build()
    ))
    val cat = extractCatalog(df)
    cat.hasCacheTable shouldBe true
    cat.getCacheTable.getTableName shouldBe "t1"
    cat.getCacheTable.hasStorageLevel shouldBe false
  }

  test("uncacheTable builds UncacheTable proto") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setUncacheTable(
      UncacheTable.newBuilder().setTableName("t1").build()
    ))
    val cat = extractCatalog(df)
    cat.hasUncacheTable shouldBe true
    cat.getUncacheTable.getTableName shouldBe "t1"
  }

  test("clearCache builds ClearCache proto") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setClearCache(ClearCache.getDefaultInstance))
    val cat = extractCatalog(df)
    cat.hasClearCache shouldBe true
  }

  test("dropTempView builds DropTempView proto") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setDropTempView(
      DropTempView.newBuilder().setViewName("temp_view").build()
    ))
    val cat = extractCatalog(df)
    cat.hasDropTempView shouldBe true
    cat.getDropTempView.getViewName shouldBe "temp_view"
  }

  test("dropGlobalTempView builds DropGlobalTempView proto") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setDropGlobalTempView(
      DropGlobalTempView.newBuilder().setViewName("global_temp_view").build()
    ))
    val cat = extractCatalog(df)
    cat.hasDropGlobalTempView shouldBe true
    cat.getDropGlobalTempView.getViewName shouldBe "global_temp_view"
  }

  test("refreshTable builds RefreshTable proto") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setRefreshTable(
      RefreshTable.newBuilder().setTableName("my_table").build()
    ))
    val cat = extractCatalog(df)
    cat.hasRefreshTable shouldBe true
    cat.getRefreshTable.getTableName shouldBe "my_table"
  }

  test("refreshByPath builds RefreshByPath proto") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setRefreshByPath(
      RefreshByPath.newBuilder().setPath("/data/path").build()
    ))
    val cat = extractCatalog(df)
    cat.hasRefreshByPath shouldBe true
    cat.getRefreshByPath.getPath shouldBe "/data/path"
  }

  test("recoverPartitions builds RecoverPartitions proto") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setRecoverPartitions(
      RecoverPartitions.newBuilder().setTableName("my_table").build()
    ))
    val cat = extractCatalog(df)
    cat.hasRecoverPartitions shouldBe true
    cat.getRecoverPartitions.getTableName shouldBe "my_table"
  }

  // ---------------------------------------------------------------------------
  // Coverage boost: deprecated createExternalTable overloads
  // ---------------------------------------------------------------------------

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
  // Coverage boost: catalogDf generates plan IDs and Relation structure
  // ---------------------------------------------------------------------------

  test("catalogDf sets plan ID and Relation common") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setListDatabases(ListDatabases.getDefaultInstance))
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
    val id1 = df1.relation.getCommon.getPlanId
    val id2 = df2.relation.getCommon.getPlanId
    id2 shouldBe >(id1)
  }

  // ---------------------------------------------------------------------------
  // Coverage boost: catalog is obtained via session.catalog
  // ---------------------------------------------------------------------------

  test("SparkSession.catalog returns a Catalog instance") {
    val session = testSession
    val cat = session.catalog
    cat shouldBe a[Catalog]
  }

  // ---------------------------------------------------------------------------
  // Coverage boost: invoke actual Catalog methods that return DataFrame (no .collect())
  // These exercise the method body without needing a server.
  // ---------------------------------------------------------------------------

  // --- listDatabases overloads (method invocation, not raw catalogDf) ---

  test("listDatabases() method returns DataFrame with ListDatabases proto") {
    val cat = testCatalog.listDatabases()
    extractCatalog(cat).hasListDatabases shouldBe true
    extractCatalog(cat).getListDatabases.hasPattern shouldBe false
  }

  test("listDatabases(pattern) method returns DataFrame with ListDatabases proto") {
    val cat = testCatalog.listDatabases("test*")
    extractCatalog(cat).hasListDatabases shouldBe true
    extractCatalog(cat).getListDatabases.getPattern shouldBe "test*"
  }

  // --- listTables overloads ---

  test("listTables() method returns DataFrame with ListTables proto") {
    val cat = testCatalog.listTables()
    extractCatalog(cat).hasListTables shouldBe true
  }

  test("listTables(dbName) method returns DataFrame with ListTables proto") {
    val cat = testCatalog.listTables("db1")
    extractCatalog(cat).hasListTables shouldBe true
    extractCatalog(cat).getListTables.getDbName shouldBe "db1"
  }

  test("listTables(dbName, pattern) method returns DataFrame with ListTables proto") {
    val cat = testCatalog.listTables("db1", "t*")
    extractCatalog(cat).hasListTables shouldBe true
    extractCatalog(cat).getListTables.getDbName shouldBe "db1"
    extractCatalog(cat).getListTables.getPattern shouldBe "t*"
  }

  // --- listFunctions overloads ---

  test("listFunctions() method returns DataFrame with ListFunctions proto") {
    val cat = testCatalog.listFunctions()
    extractCatalog(cat).hasListFunctions shouldBe true
  }

  test("listFunctions(dbName) method returns DataFrame with ListFunctions proto") {
    val cat = testCatalog.listFunctions("db1")
    extractCatalog(cat).hasListFunctions shouldBe true
    extractCatalog(cat).getListFunctions.getDbName shouldBe "db1"
  }

  test("listFunctions(dbName, pattern) method returns DataFrame with correct proto") {
    val cat = testCatalog.listFunctions("db1", "fn*")
    val pc = extractCatalog(cat)
    pc.hasListFunctions shouldBe true
    pc.getListFunctions.getDbName shouldBe "db1"
    pc.getListFunctions.getPattern shouldBe "fn*"
  }

  // --- listCatalogs overloads ---

  test("listCatalogs() method returns DataFrame with ListCatalogs proto") {
    val cat = testCatalog.listCatalogs()
    extractCatalog(cat).hasListCatalogs shouldBe true
  }

  test("listCatalogs(pattern) method returns DataFrame with ListCatalogs proto") {
    val cat = testCatalog.listCatalogs("cat*")
    extractCatalog(cat).hasListCatalogs shouldBe true
    extractCatalog(cat).getListCatalogs.getPattern shouldBe "cat*"
  }

  // --- listColumns overloads ---

  test("listColumns(tableName) method returns DataFrame with ListColumns proto") {
    val cat = testCatalog.listColumns("my_table")
    extractCatalog(cat).hasListColumns shouldBe true
    extractCatalog(cat).getListColumns.getTableName shouldBe "my_table"
  }

  test("listColumns(tableName, dbName) method returns DataFrame with ListColumns proto") {
    val cat = testCatalog.listColumns("my_table", "my_db")
    val pc = extractCatalog(cat)
    pc.hasListColumns shouldBe true
    pc.getListColumns.getTableName shouldBe "my_table"
    pc.getListColumns.getDbName shouldBe "my_db"
  }

  // --- listCachedTables ---

  test("listCachedTables() method returns DataFrame with ListCachedTables proto") {
    val cat = testCatalog.listCachedTables()
    extractCatalog(cat).hasListCachedTables shouldBe true
  }

  // --- listPartitions ---

  test("listPartitions(tableName) method returns DataFrame with ListPartitions proto") {
    val cat = testCatalog.listPartitions("tbl")
    val pc = extractCatalog(cat)
    pc.hasListPartitions shouldBe true
    pc.getListPartitions.getTableName shouldBe "tbl"
  }

  // --- listViews overloads ---

  test("listViews() method returns DataFrame with ListViews proto") {
    val cat = testCatalog.listViews()
    extractCatalog(cat).hasListViews shouldBe true
  }

  test("listViews(dbName) method returns DataFrame with ListViews proto") {
    val cat = testCatalog.listViews("db1")
    val pc = extractCatalog(cat)
    pc.hasListViews shouldBe true
    pc.getListViews.getDbName shouldBe "db1"
  }

  test("listViews(dbName, pattern) method returns DataFrame with correct proto") {
    val cat = testCatalog.listViews("db1", "v*")
    val pc = extractCatalog(cat)
    pc.hasListViews shouldBe true
    pc.getListViews.getDbName shouldBe "db1"
    pc.getListViews.getPattern shouldBe "v*"
  }

  // --- getDatabase / getTable / getFunction (method invocation) ---

  test("getDatabase(dbName) method returns DataFrame with GetDatabase proto") {
    val cat = testCatalog.getDatabase("my_db")
    val pc = extractCatalog(cat)
    pc.hasGetDatabase shouldBe true
    pc.getGetDatabase.getDbName shouldBe "my_db"
  }

  test("getTable(tableName) method returns DataFrame with GetTable proto") {
    val cat = testCatalog.getTable("my_tbl")
    val pc = extractCatalog(cat)
    pc.hasGetTable shouldBe true
    pc.getGetTable.getTableName shouldBe "my_tbl"
  }

  test("getTable(tableName, dbName) method returns DataFrame with GetTable proto") {
    val cat = testCatalog.getTable("my_tbl", "my_db")
    val pc = extractCatalog(cat)
    pc.hasGetTable shouldBe true
    pc.getGetTable.getTableName shouldBe "my_tbl"
    pc.getGetTable.getDbName shouldBe "my_db"
  }

  test("getFunction(functionName) method returns DataFrame with GetFunction proto") {
    val cat = testCatalog.getFunction("fn1")
    val pc = extractCatalog(cat)
    pc.hasGetFunction shouldBe true
    pc.getGetFunction.getFunctionName shouldBe "fn1"
  }

  test("getFunction(functionName, dbName) method returns DataFrame with GetFunction proto") {
    val cat = testCatalog.getFunction("fn1", "my_db")
    val pc = extractCatalog(cat)
    pc.hasGetFunction shouldBe true
    pc.getGetFunction.getFunctionName shouldBe "fn1"
    pc.getGetFunction.getDbName shouldBe "my_db"
  }

  test("getTableProperties(tableName) method returns DataFrame with GetTableProperties proto") {
    val cat = testCatalog.getTableProperties("my_tbl")
    val pc = extractCatalog(cat)
    pc.hasGetTableProperties shouldBe true
    pc.getGetTableProperties.getTableName shouldBe "my_tbl"
  }

  // --- createTable overloads (method invocation) ---

  test("createTable(tableName, path) method returns correct proto") {
    val df = testCatalog.createTable("t", "/path/to/data")
    val ct = extractCatalog(df).getCreateTable
    ct.getTableName shouldBe "t"
    ct.getOptionsMap.get("path") shouldBe "/path/to/data"
    ct.getSource shouldBe "parquet"
  }

  test("createTable(tableName, path, source) method returns correct proto") {
    val df = testCatalog.createTable("t", "/path", "parquet")
    val ct = extractCatalog(df).getCreateTable
    ct.getTableName shouldBe "t"
    ct.getOptionsMap.get("path") shouldBe "/path"
    ct.getSource shouldBe "parquet"
  }

  test("createTable(tableName, source, options) method returns correct proto") {
    val df = testCatalog.createTable("t", "csv", Map("header" -> "true"))
    val ct = extractCatalog(df).getCreateTable
    ct.getTableName shouldBe "t"
    ct.getSource shouldBe "csv"
    ct.getOptionsMap.asScala shouldBe Map("header" -> "true")
  }

  test("createTable(tableName, source, schema, options) method returns correct proto") {
    val schema = StructType(Seq(StructField("id", IntegerType)))
    val df = testCatalog.createTable("t", "json", schema, Map("opt" -> "val"))
    val ct = extractCatalog(df).getCreateTable
    ct.getTableName shouldBe "t"
    ct.getSource shouldBe "json"
    ct.hasSchema shouldBe true
    ct.getOptionsMap.asScala shouldBe Map("opt" -> "val")
  }

  test(
    "createTable(tableName, source, description, schema, options) method returns correct proto"
  ) {
    val schema = StructType(Seq(StructField("x", LongType)))
    val df = testCatalog.createTable("t", "orc", "desc", schema, Map("k" -> "v"))
    val ct = extractCatalog(df).getCreateTable
    ct.getTableName shouldBe "t"
    ct.getSource shouldBe "orc"
    ct.getDescription shouldBe "desc"
    ct.hasSchema shouldBe true
    ct.getOptionsMap.asScala shouldBe Map("k" -> "v")
  }

  // --- createExternalTable overloads (method invocation, deprecated) ---

  test("createExternalTable(tableName, path) delegates to createTable") {
    val df = testCatalog.createExternalTable("t", "/p")
    val ct = extractCatalog(df).getCreateTable
    ct.getTableName shouldBe "t"
    ct.getOptionsMap.get("path") shouldBe "/p"
  }

  test("createExternalTable(tableName, path, source) delegates to createTable") {
    val df = testCatalog.createExternalTable("t", "/p", "csv")
    val ct = extractCatalog(df).getCreateTable
    ct.getTableName shouldBe "t"
    ct.getOptionsMap.get("path") shouldBe "/p"
    ct.getSource shouldBe "csv"
  }

  test("createExternalTable(tableName, source, options) delegates to createTable") {
    val df = testCatalog.createExternalTable("t", "csv", Map("k" -> "v"))
    val ct = extractCatalog(df).getCreateTable
    ct.getTableName shouldBe "t"
    ct.getSource shouldBe "csv"
    ct.getOptionsMap.asScala shouldBe Map("k" -> "v")
  }

  test("createExternalTable(tableName, source, schema, options) delegates to createTable") {
    val schema = StructType(Seq(StructField("id", IntegerType)))
    val df = testCatalog.createExternalTable("t", "csv", schema, Map("k" -> "v"))
    val ct = extractCatalog(df).getCreateTable
    ct.getTableName shouldBe "t"
    ct.getSource shouldBe "csv"
    ct.hasSchema shouldBe true
    ct.getOptionsMap.asScala shouldBe Map("k" -> "v")
  }

  // ---------------------------------------------------------------------------
  // Proto-only tests for methods that call .collect() -- build DataFrame and verify proto
  // These test the proto construction of every collect()-dependent method
  // ---------------------------------------------------------------------------

  test("setCurrentDatabase builds SetCurrentDatabase proto via method") {
    val catalog = testCatalog
    // We can't call setCurrentDatabase directly (it calls .collect()),
    // but we can test the proto generation path
    val df = catalog.catalogDf(_.setSetCurrentDatabase(
      SetCurrentDatabase.newBuilder().setDbName("mydb").build()
    ))
    extractCatalog(df).hasSetCurrentDatabase shouldBe true
    extractCatalog(df).getSetCurrentDatabase.getDbName shouldBe "mydb"
  }

  test("setCurrentCatalog builds SetCurrentCatalog proto via method") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setSetCurrentCatalog(
      SetCurrentCatalog.newBuilder().setCatalogName("my_catalog").build()
    ))
    extractCatalog(df).hasSetCurrentCatalog shouldBe true
    extractCatalog(df).getSetCurrentCatalog.getCatalogName shouldBe "my_catalog"
  }

  // --- createDatabase proto via catalogDf ---

  test("createDatabase builds CreateDatabase proto with default params") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setCreateDatabase(
      CreateDatabase.newBuilder()
        .setDbName("db1")
        .setIfNotExists(false)
        .build()
    ))
    val cd = extractCatalog(df).getCreateDatabase
    cd.getDbName shouldBe "db1"
    cd.getIfNotExists shouldBe false
    cd.getPropertiesCount shouldBe 0
  }

  // --- dropDatabase proto via catalogDf ---

  test("dropDatabase builds DropDatabase proto with default params") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setDropDatabase(
      DropDatabase.newBuilder()
        .setDbName("db1")
        .setIfExists(false)
        .setCascade(false)
        .build()
    ))
    val dd = extractCatalog(df).getDropDatabase
    dd.getDbName shouldBe "db1"
    dd.getIfExists shouldBe false
    dd.getCascade shouldBe false
  }

  // --- dropTable proto via catalogDf ---

  test("dropTable builds DropTable proto with default params") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setDropTable(
      DropTable.newBuilder()
        .setTableName("t1")
        .setIfExists(false)
        .setPurge(false)
        .build()
    ))
    val dt = extractCatalog(df).getDropTable
    dt.getTableName shouldBe "t1"
    dt.getIfExists shouldBe false
    dt.getPurge shouldBe false
  }

  // --- dropView proto via catalogDf ---

  test("dropView builds DropView proto with default params") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setDropView(
      DropView.newBuilder()
        .setViewName("v1")
        .setIfExists(false)
        .build()
    ))
    val dv = extractCatalog(df).getDropView
    dv.getViewName shouldBe "v1"
    dv.getIfExists shouldBe false
  }

  // --- getCreateTableString proto via catalogDf ---

  test("getCreateTableString builds proto with asSerde=false") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setGetCreateTableString(
      GetCreateTableString.newBuilder()
        .setTableName("t1")
        .setAsSerde(false)
        .build()
    ))
    val gcts = extractCatalog(df).getGetCreateTableString
    gcts.getTableName shouldBe "t1"
    gcts.getAsSerde shouldBe false
  }

  // --- analyzeTable proto via catalogDf ---

  test("analyzeTable builds proto with noScan=false") {
    val catalog = testCatalog
    val df = catalog.catalogDf(_.setAnalyzeTable(
      AnalyzeTable.newBuilder()
        .setTableName("t1")
        .setNoScan(false)
        .build()
    ))
    val at = extractCatalog(df).getAnalyzeTable
    at.getTableName shouldBe "t1"
    at.getNoScan shouldBe false
  }

  // --- multiple catalogDf invocations on same catalog use separate plan IDs ---

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
