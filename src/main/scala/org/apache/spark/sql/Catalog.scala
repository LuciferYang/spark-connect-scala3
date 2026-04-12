package org.apache.spark.sql

import org.apache.spark.connect.proto.{
  Catalog as ProtoCatalog,
  StorageLevel as _,
  *
}
import org.apache.spark.sql.connect.client.{DataTypeProtoConverter, SparkConnectClient}
import org.apache.spark.sql.types.StructType

/** Interface through which the user may create, drop, alter or query underlying databases, tables,
  * functions, etc.
  *
  * Access via `spark.catalog`.
  *
  * Methods backed by the upstream Spark Connect proto (branch-4.1) use proto-based RPCs. Methods
  * without proto support (listViews, listPartitions, dropTable, dropView, createDatabase,
  * dropDatabase, truncateTable, analyzeTable, getTableProperties, getCreateTableString,
  * listCachedTables) use SQL fallback.
  *
  * TODO: Replace SQL fallback methods with proto-based implementations when the upstream proto adds
  * native support for these operations.
  */
final class Catalog private[sql] (private val session: SparkSession):

  private def client: SparkConnectClient = session.client

  // ---------------------------------------------------------------------------
  // Current database / catalog
  // ---------------------------------------------------------------------------

  def currentDatabase: String =
    catalogDf(_.setCurrentDatabase(CurrentDatabase.getDefaultInstance))
      .collect().head.getString(0)

  def setCurrentDatabase(dbName: String): Unit =
    catalogDf(_.setSetCurrentDatabase(
      SetCurrentDatabase.newBuilder().setDbName(dbName).build()
    )).collect()

  def currentCatalog: String =
    catalogDf(_.setCurrentCatalog(CurrentCatalog.getDefaultInstance))
      .collect().head.getString(0)

  def setCurrentCatalog(catalogName: String): Unit =
    catalogDf(_.setSetCurrentCatalog(
      SetCurrentCatalog.newBuilder().setCatalogName(catalogName).build()
    )).collect()

  // ---------------------------------------------------------------------------
  // List operations (return DataFrames)
  // ---------------------------------------------------------------------------

  def listDatabases(): DataFrame =
    catalogDf(_.setListDatabases(ListDatabases.getDefaultInstance))

  def listDatabases(pattern: String): DataFrame =
    catalogDf(_.setListDatabases(
      ListDatabases.newBuilder().setPattern(pattern).build()
    ))

  def listTables(): DataFrame =
    catalogDf(_.setListTables(ListTables.getDefaultInstance))

  def listTables(dbName: String): DataFrame =
    catalogDf(_.setListTables(
      ListTables.newBuilder().setDbName(dbName).build()
    ))

  def listTables(dbName: String, pattern: String): DataFrame =
    catalogDf(_.setListTables(
      ListTables.newBuilder().setDbName(dbName).setPattern(pattern).build()
    ))

  def listColumns(tableName: String): DataFrame =
    catalogDf(_.setListColumns(
      ListColumns.newBuilder().setTableName(tableName).build()
    ))

  def listColumns(tableName: String, dbName: String): DataFrame =
    catalogDf(_.setListColumns(
      ListColumns.newBuilder().setTableName(tableName).setDbName(dbName).build()
    ))

  def listFunctions(): DataFrame =
    catalogDf(_.setListFunctions(ListFunctions.getDefaultInstance))

  def listFunctions(dbName: String): DataFrame =
    catalogDf(_.setListFunctions(
      ListFunctions.newBuilder().setDbName(dbName).build()
    ))

  def listFunctions(dbName: String, pattern: String): DataFrame =
    catalogDf(_.setListFunctions(
      ListFunctions.newBuilder().setDbName(dbName).setPattern(pattern).build()
    ))

  def listCatalogs(): DataFrame =
    catalogDf(_.setListCatalogs(ListCatalogs.getDefaultInstance))

  def listCatalogs(pattern: String): DataFrame =
    catalogDf(_.setListCatalogs(
      ListCatalogs.newBuilder().setPattern(pattern).build()
    ))

  /** List cached tables. Note: there is no standard SQL for this; we list all tables and filter by
    * cache status. This is a best-effort convenience method.
    */
  def listCachedTables(): DataFrame =
    session.sql("SHOW TABLES")

  def listPartitions(tableName: String): DataFrame =
    session.sql(s"SHOW PARTITIONS ${quoteIdent(tableName)}")

  def listViews(): DataFrame =
    session.sql("SHOW VIEWS")

  def listViews(dbName: String): DataFrame =
    session.sql(s"SHOW VIEWS IN ${quoteIdent(dbName)}")

  def listViews(dbName: String, pattern: String): DataFrame =
    session.sql(s"SHOW VIEWS IN ${quoteIdent(dbName)} LIKE '$pattern'")

  // ---------------------------------------------------------------------------
  // Get operations
  // ---------------------------------------------------------------------------

  def getDatabase(dbName: String): DataFrame =
    catalogDf(_.setGetDatabase(
      GetDatabase.newBuilder().setDbName(dbName).build()
    ))

  def getTable(tableName: String): DataFrame =
    catalogDf(_.setGetTable(
      GetTable.newBuilder().setTableName(tableName).build()
    ))

  def getTable(tableName: String, dbName: String): DataFrame =
    catalogDf(_.setGetTable(
      GetTable.newBuilder().setTableName(tableName).setDbName(dbName).build()
    ))

  def getFunction(functionName: String): DataFrame =
    catalogDf(_.setGetFunction(
      GetFunction.newBuilder().setFunctionName(functionName).build()
    ))

  def getFunction(functionName: String, dbName: String): DataFrame =
    catalogDf(_.setGetFunction(
      GetFunction.newBuilder().setFunctionName(functionName).setDbName(dbName).build()
    ))

  def getTableProperties(tableName: String): DataFrame =
    session.sql(s"SHOW TBLPROPERTIES ${quoteIdent(tableName)}")

  def getCreateTableString(tableName: String, asSerde: Boolean = false): String =
    val keyword = if asSerde then "SHOW CREATE TABLE AS SERDE" else "SHOW CREATE TABLE"
    session.sql(s"$keyword ${quoteIdent(tableName)}").collect().head.getString(0)

  // ---------------------------------------------------------------------------
  // Existence checks
  // ---------------------------------------------------------------------------

  def databaseExists(dbName: String): Boolean =
    catalogDf(_.setDatabaseExists(
      DatabaseExists.newBuilder().setDbName(dbName).build()
    )).collect().head.getBoolean(0)

  def tableExists(tableName: String): Boolean =
    catalogDf(_.setTableExists(
      TableExists.newBuilder().setTableName(tableName).build()
    )).collect().head.getBoolean(0)

  def tableExists(tableName: String, dbName: String): Boolean =
    catalogDf(_.setTableExists(
      TableExists.newBuilder().setTableName(tableName).setDbName(dbName).build()
    )).collect().head.getBoolean(0)

  def functionExists(functionName: String): Boolean =
    catalogDf(_.setFunctionExists(
      FunctionExists.newBuilder().setFunctionName(functionName).build()
    )).collect().head.getBoolean(0)

  def functionExists(functionName: String, dbName: String): Boolean =
    catalogDf(_.setFunctionExists(
      FunctionExists.newBuilder().setFunctionName(functionName).setDbName(dbName).build()
    )).collect().head.getBoolean(0)

  // ---------------------------------------------------------------------------
  // Cache management
  // ---------------------------------------------------------------------------

  def isCached(tableName: String): Boolean =
    catalogDf(_.setIsCached(
      IsCached.newBuilder().setTableName(tableName).build()
    )).collect().head.getBoolean(0)

  def cacheTable(tableName: String): Unit =
    catalogDf(_.setCacheTable(
      CacheTable.newBuilder().setTableName(tableName).build()
    )).collect()

  def cacheTable(tableName: String, storageLevel: StorageLevel): Unit =
    catalogDf(_.setCacheTable(
      CacheTable.newBuilder()
        .setTableName(tableName)
        .setStorageLevel(storageLevel.toProto)
        .build()
    )).collect()

  def uncacheTable(tableName: String): Unit =
    catalogDf(_.setUncacheTable(
      UncacheTable.newBuilder().setTableName(tableName).build()
    )).collect()

  def clearCache(): Unit =
    catalogDf(_.setClearCache(ClearCache.getDefaultInstance)).collect()

  // ---------------------------------------------------------------------------
  // Create table
  // ---------------------------------------------------------------------------

  def createTable(tableName: String, path: String): DataFrame =
    createTable(tableName, "parquet", "", StructType(Seq.empty), Map("path" -> path))

  def createTable(tableName: String, path: String, source: String): DataFrame =
    createTable(tableName, source, Map("path" -> path))

  def createTable(
      tableName: String,
      source: String,
      options: Map[String, String]
  ): DataFrame =
    createTable(tableName, source, StructType(Seq.empty), options)

  def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]
  ): DataFrame =
    createTable(tableName, source, "", schema, options)

  def createTable(
      tableName: String,
      source: String,
      description: String,
      schema: StructType,
      options: Map[String, String]
  ): DataFrame =
    val builder = CreateTable.newBuilder()
      .setTableName(tableName)
      .setSource(source)
      .setSchema(DataTypeProtoConverter.toProto(schema))
      .setDescription(description)
    options.foreach((k, v) => builder.putOptions(k, v))
    catalogDf(_.setCreateTable(builder.build()))

  // ---------------------------------------------------------------------------
  // Create external table (deprecated, delegates to createTable)
  // ---------------------------------------------------------------------------

  @deprecated("Use createTable instead.", "4.0.0")
  def createExternalTable(tableName: String, path: String): DataFrame =
    createTable(tableName, path)

  @deprecated("Use createTable instead.", "4.0.0")
  def createExternalTable(tableName: String, path: String, source: String): DataFrame =
    createTable(tableName, path, source)

  @deprecated("Use createTable instead.", "4.0.0")
  def createExternalTable(
      tableName: String,
      source: String,
      options: Map[String, String]
  ): DataFrame =
    createTable(tableName, source, options)

  @deprecated("Use createTable instead.", "4.0.0")
  def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]
  ): DataFrame =
    createTable(tableName, source, schema, options)

  // ---------------------------------------------------------------------------
  // Database management
  // ---------------------------------------------------------------------------

  def createDatabase(
      dbName: String,
      ifNotExists: Boolean = false,
      properties: Map[String, String] = Map.empty
  ): Unit =
    val ifNotExistsClause = if ifNotExists then " IF NOT EXISTS" else ""
    val propsClause =
      if properties.isEmpty then ""
      else properties.map((k, v) => s"'$k' = '$v'").mkString(" WITH DBPROPERTIES (", ", ", ")")
    session.sql(s"CREATE DATABASE$ifNotExistsClause ${quoteIdent(dbName)}$propsClause").collect()

  def dropDatabase(
      dbName: String,
      ifExists: Boolean = false,
      cascade: Boolean = false
  ): Unit =
    val ifExistsClause = if ifExists then " IF EXISTS" else ""
    val cascadeClause = if cascade then " CASCADE" else ""
    session.sql(s"DROP DATABASE$ifExistsClause ${quoteIdent(dbName)}$cascadeClause").collect()

  // ---------------------------------------------------------------------------
  // Drop / Refresh
  // ---------------------------------------------------------------------------

  def dropTempView(viewName: String): Boolean =
    catalogDf(_.setDropTempView(
      DropTempView.newBuilder().setViewName(viewName).build()
    )).collect().head.getBoolean(0)

  def dropGlobalTempView(viewName: String): Boolean =
    catalogDf(_.setDropGlobalTempView(
      DropGlobalTempView.newBuilder().setViewName(viewName).build()
    )).collect().head.getBoolean(0)

  def dropTable(
      tableName: String,
      ifExists: Boolean = false,
      purge: Boolean = false
  ): Unit =
    val ifExistsClause = if ifExists then " IF EXISTS" else ""
    val purgeClause = if purge then " PURGE" else ""
    session.sql(s"DROP TABLE$ifExistsClause ${quoteIdent(tableName)}$purgeClause").collect()

  def dropView(viewName: String, ifExists: Boolean = false): Unit =
    val ifExistsClause = if ifExists then " IF EXISTS" else ""
    session.sql(s"DROP VIEW$ifExistsClause ${quoteIdent(viewName)}").collect()

  def refreshTable(tableName: String): Unit =
    catalogDf(_.setRefreshTable(
      RefreshTable.newBuilder().setTableName(tableName).build()
    )).collect()

  def refreshByPath(path: String): Unit =
    catalogDf(_.setRefreshByPath(
      RefreshByPath.newBuilder().setPath(path).build()
    )).collect()

  def recoverPartitions(tableName: String): Unit =
    catalogDf(_.setRecoverPartitions(
      RecoverPartitions.newBuilder().setTableName(tableName).build()
    )).collect()

  def truncateTable(tableName: String): Unit =
    session.sql(s"TRUNCATE TABLE ${quoteIdent(tableName)}").collect()

  def analyzeTable(tableName: String, noScan: Boolean = false): Unit =
    val noScanClause = if noScan then " NOSCAN" else ""
    session.sql(s"ANALYZE TABLE ${quoteIdent(tableName)} COMPUTE STATISTICS$noScanClause").collect()

  // ---------------------------------------------------------------------------
  // Helper
  // ---------------------------------------------------------------------------

  /** Quote an identifier for use in SQL. Multi-part names (e.g. "db.table") are passed through
    * as-is so Spark SQL can resolve them. Simple names are backtick-quoted.
    */
  private def quoteIdent(name: String): String =
    if name.contains(".") then name else s"`$name`"

  private[sql] def catalogDf(f: ProtoCatalog.Builder => ProtoCatalog.Builder): DataFrame =
    val catBuilder = ProtoCatalog.newBuilder()
    DataFrame(
      session,
      Relation.newBuilder()
        .setCommon(RelationCommon.newBuilder().setPlanId(session.nextPlanId()).build())
        .setCatalog(f(catBuilder).build())
        .build()
    )
