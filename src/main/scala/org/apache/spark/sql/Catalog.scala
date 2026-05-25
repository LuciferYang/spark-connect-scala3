package org.apache.spark.sql

import org.apache.spark.connect.proto.{
  Catalog as ProtoCatalog,
  StorageLevel as _,
  *
}
import org.apache.spark.sql.catalog.{
  CatalogMetadata,
  Column as CatalogColumn,
  Database,
  Function as CatalogFunction,
  Table
}
import org.apache.spark.sql.connect.client.DataTypeProtoConverter
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
  // List operations (return typed Datasets, matching upstream Catalog.scala)
  // ---------------------------------------------------------------------------

  def listDatabases(): Dataset[Database] =
    catalogDf(_.setListDatabases(ListDatabases.getDefaultInstance)).as[Database]

  def listDatabases(pattern: String): Dataset[Database] =
    catalogDf(_.setListDatabases(
      ListDatabases.newBuilder().setPattern(pattern).build()
    )).as[Database]

  def listTables(): Dataset[Table] =
    catalogDf(_.setListTables(ListTables.getDefaultInstance)).as[Table]

  def listTables(dbName: String): Dataset[Table] =
    catalogDf(_.setListTables(
      ListTables.newBuilder().setDbName(dbName).build()
    )).as[Table]

  def listTables(dbName: String, pattern: String): Dataset[Table] =
    catalogDf(_.setListTables(
      ListTables.newBuilder().setDbName(dbName).setPattern(pattern).build()
    )).as[Table]

  def listColumns(tableName: String): Dataset[CatalogColumn] =
    catalogDf(_.setListColumns(
      ListColumns.newBuilder().setTableName(tableName).build()
    )).as[CatalogColumn]

  def listColumns(dbName: String, tableName: String): Dataset[CatalogColumn] =
    catalogDf(_.setListColumns(
      ListColumns.newBuilder().setTableName(tableName).setDbName(dbName).build()
    )).as[CatalogColumn]

  def listFunctions(): Dataset[CatalogFunction] =
    catalogDf(_.setListFunctions(ListFunctions.getDefaultInstance)).as[CatalogFunction]

  def listFunctions(dbName: String): Dataset[CatalogFunction] =
    catalogDf(_.setListFunctions(
      ListFunctions.newBuilder().setDbName(dbName).build()
    )).as[CatalogFunction]

  def listFunctions(dbName: String, pattern: String): Dataset[CatalogFunction] =
    catalogDf(_.setListFunctions(
      ListFunctions.newBuilder().setDbName(dbName).setPattern(pattern).build()
    )).as[CatalogFunction]

  def listCatalogs(): Dataset[CatalogMetadata] =
    catalogDf(_.setListCatalogs(ListCatalogs.getDefaultInstance)).as[CatalogMetadata]

  def listCatalogs(pattern: String): Dataset[CatalogMetadata] =
    catalogDf(_.setListCatalogs(
      ListCatalogs.newBuilder().setPattern(pattern).build()
    )).as[CatalogMetadata]

  /** List all tables in the current database.
    *
    * '''Note''': despite the historical name, this method does NOT filter by cache status — there
    * is no standard SQL to query which tables are currently cached on the server. The underlying
    * `SHOW TABLES` returns every table, cached or not. Kept for API parity with older Spark
    * versions; callers that truly need cached-only lookup should track their own `cacheTable` /
    * `uncacheTable` calls.
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
    session.sql(s"SHOW VIEWS IN ${quoteIdent(dbName)} LIKE '${escapeSqlLiteral(pattern)}'")

  // ---------------------------------------------------------------------------
  // Get operations (return scalar typed values; eagerly fetch first row)
  // ---------------------------------------------------------------------------

  def getDatabase(dbName: String): Database =
    catalogDf(_.setGetDatabase(
      GetDatabase.newBuilder().setDbName(dbName).build()
    )).as[Database].head()

  def getTable(tableName: String): Table =
    catalogDf(_.setGetTable(
      GetTable.newBuilder().setTableName(tableName).build()
    )).as[Table].head()

  def getTable(dbName: String, tableName: String): Table =
    catalogDf(_.setGetTable(
      GetTable.newBuilder().setTableName(tableName).setDbName(dbName).build()
    )).as[Table].head()

  def getFunction(functionName: String): CatalogFunction =
    catalogDf(_.setGetFunction(
      GetFunction.newBuilder().setFunctionName(functionName).build()
    )).as[CatalogFunction].head()

  def getFunction(dbName: String, functionName: String): CatalogFunction =
    catalogDf(_.setGetFunction(
      GetFunction.newBuilder().setFunctionName(functionName).setDbName(dbName).build()
    )).as[CatalogFunction].head()

  def getTableProperties(tableName: String): DataFrame =
    session.sql(s"SHOW TBLPROPERTIES ${quoteIdent(tableName)}")

  def getCreateTableString(tableName: String, asSerde: Boolean = false): String =
    session.sql(buildShowCreateTableSql(quoteIdent(tableName), asSerde))
      .collect().head.getString(0)

  /** Build the `SHOW CREATE TABLE` SQL string for the given (already-quoted) identifier.
    *
    * `SqlBaseParser` grammar requires `(AS SERDE)?` to follow the identifier:
    * `SHOW CREATE TABLE identifierReference (AS SERDE)?`. Putting `AS SERDE` in front of the
    * identifier causes `ParseException` on the server.
    */
  private[sql] def buildShowCreateTableSql(quotedIdent: String, asSerde: Boolean): String =
    val suffix = if asSerde then " AS SERDE" else ""
    s"SHOW CREATE TABLE $quotedIdent$suffix"

  // ---------------------------------------------------------------------------
  // Existence checks
  // ---------------------------------------------------------------------------

  def catalogExists(catalogName: String): Boolean =
    listCatalogs().filter(Column("name") === catalogName).count() > 0

  def databaseExists(dbName: String): Boolean =
    catalogDf(_.setDatabaseExists(
      DatabaseExists.newBuilder().setDbName(dbName).build()
    )).collect().head.getBoolean(0)

  def tableExists(tableName: String): Boolean =
    catalogDf(_.setTableExists(
      TableExists.newBuilder().setTableName(tableName).build()
    )).collect().head.getBoolean(0)

  def tableExists(dbName: String, tableName: String): Boolean =
    catalogDf(_.setTableExists(
      TableExists.newBuilder().setTableName(tableName).setDbName(dbName).build()
    )).collect().head.getBoolean(0)

  def functionExists(functionName: String): Boolean =
    catalogDf(_.setFunctionExists(
      FunctionExists.newBuilder().setFunctionName(functionName).build()
    )).collect().head.getBoolean(0)

  def functionExists(dbName: String, functionName: String): Boolean =
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
    val source = session.conf.get("spark.sql.sources.default", "parquet")
    createTable(tableName, source, "", StructType(Seq.empty), Map("path" -> path))

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
      else
        properties
          .map((k, v) => s"'${escapeSqlLiteral(k)}' = '${escapeSqlLiteral(v)}'")
          .mkString(" WITH DBPROPERTIES (", ", ", ")")
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

  /** Quote an identifier for use in SQL. Multi-part names (e.g. "db.table") have each part
    * individually quoted. Simple names are backtick-quoted with internal backticks escaped.
    */
  private def quoteIdent(name: String): String =
    require(name != null && name.nonEmpty, "Identifier must not be null or empty")
    name.split("\\.", -1).map(part => s"`${part.replace("`", "``")}`").mkString(".")

  /** Escape a string literal for safe inclusion in SQL. Backslashes are doubled (Spark's default
    * parser processes backslash escape sequences like \n, \t, \\, \uXXXX) and single quotes are
    * doubled (standard SQL quote escaping).
    */
  private def escapeSqlLiteral(s: String): String =
    require(s != null, "SQL literal must not be null")
    s.replace("\\", "\\\\").replace("'", "''")

  private[sql] def catalogDf(f: ProtoCatalog.Builder => ProtoCatalog.Builder): DataFrame =
    val catBuilder = ProtoCatalog.newBuilder()
    DataFrame(
      session,
      Relation.newBuilder()
        .setCommon(RelationCommon.newBuilder().setPlanId(session.nextPlanId()).build())
        .setCatalog(f(catBuilder).build())
        .build()
    )
