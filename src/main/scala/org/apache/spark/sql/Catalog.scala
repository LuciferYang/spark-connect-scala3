package org.apache.spark.sql

import org.apache.spark.connect.proto.catalog as cat
import org.apache.spark.connect.proto.relations.{Relation, RelationCommon}
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
    catalogDf(cat.Catalog.CatType.CurrentDatabase(cat.CurrentDatabase()))
      .collect().head.getString(0)

  def setCurrentDatabase(dbName: String): Unit =
    catalogDf(cat.Catalog.CatType.SetCurrentDatabase(
      cat.SetCurrentDatabase(dbName = dbName)
    )).collect()

  def currentCatalog: String =
    catalogDf(cat.Catalog.CatType.CurrentCatalog(cat.CurrentCatalog()))
      .collect().head.getString(0)

  def setCurrentCatalog(catalogName: String): Unit =
    catalogDf(cat.Catalog.CatType.SetCurrentCatalog(
      cat.SetCurrentCatalog(catalogName = catalogName)
    )).collect()

  // ---------------------------------------------------------------------------
  // List operations (return DataFrames)
  // ---------------------------------------------------------------------------

  def listDatabases(): DataFrame =
    catalogDf(cat.Catalog.CatType.ListDatabases(cat.ListDatabases()))

  def listDatabases(pattern: String): DataFrame =
    catalogDf(cat.Catalog.CatType.ListDatabases(
      cat.ListDatabases(pattern = Some(pattern))
    ))

  def listTables(): DataFrame =
    catalogDf(cat.Catalog.CatType.ListTables(cat.ListTables()))

  def listTables(dbName: String): DataFrame =
    catalogDf(cat.Catalog.CatType.ListTables(
      cat.ListTables(dbName = Some(dbName))
    ))

  def listColumns(tableName: String): DataFrame =
    catalogDf(cat.Catalog.CatType.ListColumns(
      cat.ListColumns(tableName = tableName)
    ))

  def listColumns(tableName: String, dbName: String): DataFrame =
    catalogDf(cat.Catalog.CatType.ListColumns(
      cat.ListColumns(tableName = tableName, dbName = Some(dbName))
    ))

  def listFunctions(): DataFrame =
    catalogDf(cat.Catalog.CatType.ListFunctions(cat.ListFunctions()))

  def listFunctions(dbName: String): DataFrame =
    catalogDf(cat.Catalog.CatType.ListFunctions(
      cat.ListFunctions(dbName = Some(dbName))
    ))

  def listCatalogs(): DataFrame =
    catalogDf(cat.Catalog.CatType.ListCatalogs(cat.ListCatalogs()))

  def listCatalogs(pattern: String): DataFrame =
    catalogDf(cat.Catalog.CatType.ListCatalogs(
      cat.ListCatalogs(pattern = Some(pattern))
    ))

  // ---------------------------------------------------------------------------
  // Get operations
  // ---------------------------------------------------------------------------

  def getDatabase(dbName: String): DataFrame =
    catalogDf(cat.Catalog.CatType.GetDatabase(
      cat.GetDatabase(dbName = dbName)
    ))

  def getTable(tableName: String): DataFrame =
    catalogDf(cat.Catalog.CatType.GetTable(
      cat.GetTable(tableName = tableName)
    ))

  def getTable(tableName: String, dbName: String): DataFrame =
    catalogDf(cat.Catalog.CatType.GetTable(
      cat.GetTable(tableName = tableName, dbName = Some(dbName))
    ))

  def getFunction(functionName: String): DataFrame =
    catalogDf(cat.Catalog.CatType.GetFunction(
      cat.GetFunction(functionName = functionName)
    ))

  // ---------------------------------------------------------------------------
  // Existence checks
  // ---------------------------------------------------------------------------

  def databaseExists(dbName: String): Boolean =
    catalogDf(cat.Catalog.CatType.DatabaseExists(
      cat.DatabaseExists(dbName = dbName)
    )).collect().head.getBoolean(0)

  def tableExists(tableName: String): Boolean =
    catalogDf(cat.Catalog.CatType.TableExists(
      cat.TableExists(tableName = tableName)
    )).collect().head.getBoolean(0)

  def tableExists(tableName: String, dbName: String): Boolean =
    catalogDf(cat.Catalog.CatType.TableExists(
      cat.TableExists(tableName = tableName, dbName = Some(dbName))
    )).collect().head.getBoolean(0)

  def functionExists(functionName: String): Boolean =
    catalogDf(cat.Catalog.CatType.FunctionExists(
      cat.FunctionExists(functionName = functionName)
    )).collect().head.getBoolean(0)

  def functionExists(functionName: String, dbName: String): Boolean =
    catalogDf(cat.Catalog.CatType.FunctionExists(
      cat.FunctionExists(functionName = functionName, dbName = Some(dbName))
    )).collect().head.getBoolean(0)

  // ---------------------------------------------------------------------------
  // Cache management
  // ---------------------------------------------------------------------------

  def isCached(tableName: String): Boolean =
    catalogDf(cat.Catalog.CatType.IsCached(
      cat.IsCached(tableName = tableName)
    )).collect().head.getBoolean(0)

  def cacheTable(tableName: String): Unit =
    catalogDf(cat.Catalog.CatType.CacheTable(
      cat.CacheTable(tableName = tableName)
    )).collect()

  def uncacheTable(tableName: String): Unit =
    catalogDf(cat.Catalog.CatType.UncacheTable(
      cat.UncacheTable(tableName = tableName)
    )).collect()

  def clearCache(): Unit =
    catalogDf(cat.Catalog.CatType.ClearCache(cat.ClearCache())).collect()

  // ---------------------------------------------------------------------------
  // Drop / Refresh
  // ---------------------------------------------------------------------------

  def dropTempView(viewName: String): Unit =
    catalogDf(cat.Catalog.CatType.DropTempView(
      cat.DropTempView(viewName = viewName)
    )).collect()

  def dropGlobalTempView(viewName: String): Unit =
    catalogDf(cat.Catalog.CatType.DropGlobalTempView(
      cat.DropGlobalTempView(viewName = viewName)
    )).collect()

  def refreshTable(tableName: String): Unit =
    catalogDf(cat.Catalog.CatType.RefreshTable(
      cat.RefreshTable(tableName = tableName)
    )).collect()

  def refreshByPath(path: String): Unit =
    catalogDf(cat.Catalog.CatType.RefreshByPath(
      cat.RefreshByPath(path = path)
    )).collect()

  def recoverPartitions(tableName: String): Unit =
    catalogDf(cat.Catalog.CatType.RecoverPartitions(
      cat.RecoverPartitions(tableName = tableName)
    )).collect()

  // ---------------------------------------------------------------------------
  // Helper
  // ---------------------------------------------------------------------------

  private def catalogDf(catType: cat.Catalog.CatType): DataFrame =
    DataFrame(session, Relation(
      common = Some(RelationCommon(planId = Some(session.nextPlanId()))),
      relType = Relation.RelType.Catalog(cat.Catalog(catType = catType))
    ))
