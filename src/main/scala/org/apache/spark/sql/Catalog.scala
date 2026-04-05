package org.apache.spark.sql

import org.apache.spark.connect.proto.{
  Catalog as ProtoCatalog,
  StorageLevel as _,
  *
}
import org.apache.spark.sql.connect.client.{DataTypeProtoConverter, SparkConnectClient}
import org.apache.spark.sql.types.StructType

import scala.jdk.CollectionConverters.*

/** Interface through which the user may create, drop, alter or query underlying databases, tables,
  * functions, etc.
  *
  * Access via `spark.catalog`.
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

  def listCachedTables(): DataFrame =
    catalogDf(_.setListCachedTables(ListCachedTables.getDefaultInstance))

  def listPartitions(tableName: String): DataFrame =
    catalogDf(_.setListPartitions(
      ListPartitions.newBuilder().setTableName(tableName).build()
    ))

  def listViews(): DataFrame =
    catalogDf(_.setListViews(ListViews.getDefaultInstance))

  def listViews(dbName: String): DataFrame =
    catalogDf(_.setListViews(
      ListViews.newBuilder().setDbName(dbName).build()
    ))

  def listViews(dbName: String, pattern: String): DataFrame =
    catalogDf(_.setListViews(
      ListViews.newBuilder().setDbName(dbName).setPattern(pattern).build()
    ))

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
    catalogDf(_.setGetTableProperties(
      GetTableProperties.newBuilder().setTableName(tableName).build()
    ))

  def getCreateTableString(tableName: String, asSerde: Boolean = false): String =
    catalogDf(_.setGetCreateTableString(
      GetCreateTableString.newBuilder().setTableName(tableName).setAsSerde(asSerde).build()
    )).collect().head.getString(0)

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
    catalogDf(_.setCreateTable(
      CreateTable.newBuilder()
        .setTableName(tableName)
        .setPath(path)
        .build()
    ))

  def createTable(tableName: String, path: String, source: String): DataFrame =
    catalogDf(_.setCreateTable(
      CreateTable.newBuilder()
        .setTableName(tableName)
        .setPath(path)
        .setSource(source)
        .build()
    ))

  def createTable(
      tableName: String,
      source: String,
      options: Map[String, String]
  ): DataFrame =
    catalogDf(_.setCreateTable(
      CreateTable.newBuilder()
        .setTableName(tableName)
        .setSource(source)
        .putAllOptions(options.asJava)
        .build()
    ))

  def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]
  ): DataFrame =
    catalogDf(_.setCreateTable(
      CreateTable.newBuilder()
        .setTableName(tableName)
        .setSource(source)
        .setSchema(DataTypeProtoConverter.toProto(schema))
        .putAllOptions(options.asJava)
        .build()
    ))

  def createTable(
      tableName: String,
      source: String,
      description: String,
      schema: StructType,
      options: Map[String, String]
  ): DataFrame =
    catalogDf(_.setCreateTable(
      CreateTable.newBuilder()
        .setTableName(tableName)
        .setSource(source)
        .setDescription(description)
        .setSchema(DataTypeProtoConverter.toProto(schema))
        .putAllOptions(options.asJava)
        .build()
    ))

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
    catalogDf(_.setCreateDatabase(
      CreateDatabase.newBuilder()
        .setDbName(dbName)
        .setIfNotExists(ifNotExists)
        .putAllProperties(properties.asJava)
        .build()
    )).collect()

  def dropDatabase(
      dbName: String,
      ifExists: Boolean = false,
      cascade: Boolean = false
  ): Unit =
    catalogDf(_.setDropDatabase(
      DropDatabase.newBuilder()
        .setDbName(dbName)
        .setIfExists(ifExists)
        .setCascade(cascade)
        .build()
    )).collect()

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
    catalogDf(_.setDropTable(
      DropTable.newBuilder()
        .setTableName(tableName)
        .setIfExists(ifExists)
        .setPurge(purge)
        .build()
    )).collect()

  def dropView(viewName: String, ifExists: Boolean = false): Unit =
    catalogDf(_.setDropView(
      DropView.newBuilder()
        .setViewName(viewName)
        .setIfExists(ifExists)
        .build()
    )).collect()

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
    catalogDf(_.setTruncateTable(
      TruncateTable.newBuilder().setTableName(tableName).build()
    )).collect()

  def analyzeTable(tableName: String, noScan: Boolean = false): Unit =
    catalogDf(_.setAnalyzeTable(
      AnalyzeTable.newBuilder()
        .setTableName(tableName)
        .setNoScan(noScan)
        .build()
    )).collect()

  // ---------------------------------------------------------------------------
  // Helper
  // ---------------------------------------------------------------------------

  private[sql] def catalogDf(f: ProtoCatalog.Builder => ProtoCatalog.Builder): DataFrame =
    val catBuilder = ProtoCatalog.newBuilder()
    DataFrame(
      session,
      Relation.newBuilder()
        .setCommon(RelationCommon.newBuilder().setPlanId(session.nextPlanId()).build())
        .setCatalog(f(catBuilder).build())
        .build()
    )
