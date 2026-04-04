package org.apache.spark.sql

import org.apache.spark.connect.proto.{Catalog as ProtoCatalog, *}
import org.apache.spark.sql.connect.client.SparkConnectClient

/**
 * Interface through which the user may create, drop, alter or query underlying
 * databases, tables, functions, etc.
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

  def listCatalogs(): DataFrame =
    catalogDf(_.setListCatalogs(ListCatalogs.getDefaultInstance))

  def listCatalogs(pattern: String): DataFrame =
    catalogDf(_.setListCatalogs(
      ListCatalogs.newBuilder().setPattern(pattern).build()
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

  def uncacheTable(tableName: String): Unit =
    catalogDf(_.setUncacheTable(
      UncacheTable.newBuilder().setTableName(tableName).build()
    )).collect()

  def clearCache(): Unit =
    catalogDf(_.setClearCache(ClearCache.getDefaultInstance)).collect()

  // ---------------------------------------------------------------------------
  // Drop / Refresh
  // ---------------------------------------------------------------------------

  def dropTempView(viewName: String): Unit =
    catalogDf(_.setDropTempView(
      DropTempView.newBuilder().setViewName(viewName).build()
    )).collect()

  def dropGlobalTempView(viewName: String): Unit =
    catalogDf(_.setDropGlobalTempView(
      DropGlobalTempView.newBuilder().setViewName(viewName).build()
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

  // ---------------------------------------------------------------------------
  // Helper
  // ---------------------------------------------------------------------------

  private def catalogDf(f: ProtoCatalog.Builder => ProtoCatalog.Builder): DataFrame =
    val catBuilder = ProtoCatalog.newBuilder()
    DataFrame(session, Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(session.nextPlanId()).build())
      .setCatalog(f(catBuilder).build())
      .build())
