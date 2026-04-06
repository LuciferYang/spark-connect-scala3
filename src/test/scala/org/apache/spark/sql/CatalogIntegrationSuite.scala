package org.apache.spark.sql

import org.apache.spark.sql.tags.IntegrationTest

/** Integration tests for Catalog operations. */
@IntegrationTest
class CatalogIntegrationSuite extends IntegrationTestBase:

  test("catalog listDatabases") {
    val dbs = spark.catalog.listDatabases()
    val rows = dbs.collect()
    assert(rows.nonEmpty)
    println(s"Found ${rows.length} database(s)")
  }

  test("catalog listTables") {
    val tables = spark.catalog.listTables()
    val rows = tables.collect()
    // May be empty if no tables exist, but should not throw
    println(s"Found ${rows.length} table(s) in default database")
  }

  test("catalog listFunctions") {
    val funcs = spark.catalog.listFunctions()
    val rows = funcs.collect()
    assert(rows.nonEmpty, "Built-in functions should always be present")
    println(s"Found ${rows.length} function(s)")
  }

  test("catalog listCatalogs") {
    val catalogs = spark.catalog.listCatalogs()
    val rows = catalogs.collect()
    assert(rows.nonEmpty, "At least one catalog should exist")
    println(s"Found ${rows.length} catalog(s)")
  }

  test("catalog currentDatabase and setCurrentDatabase") {
    val original = spark.catalog.currentDatabase
    assert(original.nonEmpty, "Current database should not be empty")
    try
      // Set to 'default' (always exists) and verify
      spark.catalog.setCurrentDatabase("default")
      assert(spark.catalog.currentDatabase == "default")
    finally
      // Restore original database
      spark.catalog.setCurrentDatabase(original)
  }

  test("catalog currentCatalog") {
    val catalog = spark.catalog.currentCatalog
    assert(catalog.nonEmpty, "Current catalog should not be empty")
    println(s"Current catalog: $catalog")
  }

  test("catalog tableExists") {
    val exists = spark.catalog.tableExists("non_existent_table_12345")
    assert(!exists, "Non-existent table should return false")
  }

  test("catalog functionExists") {
    val exists = spark.catalog.functionExists("abs")
    assert(exists, "Built-in function 'abs' should exist")

    val notExists = spark.catalog.functionExists("non_existent_func_12345")
    assert(!notExists, "Non-existent function should return false")
  }

  test("catalog databaseExists") {
    val exists = spark.catalog.databaseExists("default")
    assert(exists, "The 'default' database should always exist")

    val notExists = spark.catalog.databaseExists("non_existent_db_12345")
    assert(!notExists, "Non-existent database should return false")
  }

  test("catalog createTable and dropTable") {
    val tableName = "test_catalog_create_table_integration"
    val tempDir = java.nio.file.Files.createTempDirectory("spark_catalog_test")
    try
      // Write a simple parquet file so the table has data
      spark.range(5).write.mode("overwrite").parquet(tempDir.toString)
      spark.catalog.createTable(tableName, tempDir.toString, "parquet").collect()
      assert(spark.catalog.tableExists(tableName), "Table should exist after creation")
    finally
      // Use SQL to drop table -- Catalog.dropTable proto (field 28) is not yet
      // supported by released Spark servers (requires SPARK-56221).
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      // Clean up temp directory
      java.nio.file.Files.walk(tempDir).sorted(java.util.Comparator.reverseOrder())
        .forEach(java.nio.file.Files.deleteIfExists(_))
  }

  test("catalog createTempView and dropTempView") {
    val viewName = "test_catalog_temp_view_integration"
    try
      spark.range(3).createOrReplaceTempView(viewName)
      // Verify the view exists by querying it via SQL
      val result = spark.sql(s"SELECT * FROM $viewName").collect()
      assert(result.length == 3, "Temp view should contain 3 rows")
      val dropped = spark.catalog.dropTempView(viewName)
      assert(dropped, "dropTempView should return true for an existing view")
    finally
      // Ensure cleanup even if assertions fail midway
      spark.catalog.dropTempView(viewName)
  }

  test("catalog cacheTable and isCached and uncacheTable") {
    val viewName = "test_catalog_cache_integration"
    try
      spark.range(10).createOrReplaceTempView(viewName)
      assert(!spark.catalog.isCached(viewName), "View should not be cached initially")

      spark.catalog.cacheTable(viewName)
      assert(spark.catalog.isCached(viewName), "View should be cached after cacheTable")

      spark.catalog.uncacheTable(viewName)
      assert(!spark.catalog.isCached(viewName), "View should not be cached after uncacheTable")
    finally
      spark.catalog.dropTempView(viewName)
  }

  test("catalog listCachedTables via isCached") {
    // Note: Catalog.listCachedTables proto (field 27) is not yet supported by
    // released Spark servers (requires SPARK-56221). Verify caching via isCached instead.
    val viewName = "test_catalog_list_cached_integration"
    try
      spark.range(5).createOrReplaceTempView(viewName)
      spark.catalog.cacheTable(viewName)
      assert(spark.catalog.isCached(viewName), "Table should be cached after cacheTable")
    finally
      try spark.catalog.uncacheTable(viewName)
      catch case _: Exception => ()
      spark.catalog.dropTempView(viewName)
  }
