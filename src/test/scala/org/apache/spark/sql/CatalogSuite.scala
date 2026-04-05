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
    cat.getCreateTable.getPath shouldBe "/data/path"
    cat.getCreateTable.hasSource shouldBe false
  }

  test("createTable with tableName, path, and source") {
    val cat = extractCatalog(testCatalog.createTable("t1", "/data/path", "parquet"))
    cat.hasCreateTable shouldBe true
    cat.getCreateTable.getTableName shouldBe "t1"
    cat.getCreateTable.getPath shouldBe "/data/path"
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
    cat1.getCreateTable.getPath shouldBe "/p"
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
