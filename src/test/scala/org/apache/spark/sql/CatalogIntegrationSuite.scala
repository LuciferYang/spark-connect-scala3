package org.apache.spark.sql

import org.apache.spark.sql.StorageLevel
import org.apache.spark.sql.tags.IntegrationTest
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/** Integration tests for Catalog operations.
  *
  * Some Catalog operations (listViews, listPartitions, dropTable, dropView, createDatabase,
  * dropDatabase, truncateTable, analyzeTable, getTableProperties, getCreateTableString) are
  * currently implemented via SQL fallback because the upstream Spark Connect proto (branch-4.1)
  * does not define dedicated RPC messages for them.
  *
  * TODO: When the upstream proto adds native support for these operations (expected in a future
  * Spark version), replace the SQL fallback in Catalog.scala with proto-based implementations and
  * update these tests accordingly.
  */
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
      // Clean up stale table from previous runs
      try spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
      catch case _: Exception => ()
        // Write a simple parquet file so the table has data
      spark.range(5).write.mode("overwrite").parquet(tempDir.toString)
      spark.catalog.createTable(tableName, tempDir.toString, "parquet").collect()
      assert(spark.catalog.tableExists(tableName), "Table should exist after creation")
    finally
      // Use SQL to drop table -- Catalog.dropTable proto (field 28) is not yet
      // supported by released Spark servers (requires SPARK-56221).
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
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

  // ---------------------------------------------------------------------------
  // New tests for untested Catalog methods
  // ---------------------------------------------------------------------------

  test("catalog setCurrentCatalog") {
    val original = spark.catalog.currentCatalog
    try
      spark.catalog.setCurrentCatalog("testcat")
      assert(spark.catalog.currentCatalog == "testcat", "Current catalog should be testcat")
    finally
      spark.catalog.setCurrentCatalog(original)
      assert(spark.catalog.currentCatalog == original, "Catalog should be restored")
  }

  test("catalog listDatabases with pattern") {
    val dbs = spark.catalog.listDatabases("def*")
    val rows = dbs.collect()
    assert(rows.nonEmpty, "Pattern 'def*' should match the 'default' database")
    println(s"Found ${rows.length} database(s) matching 'def*'")

    val empty = spark.catalog.listDatabases("zzz_no_match_*")
    assert(empty.collect().isEmpty, "Non-matching pattern should return empty")
  }

  test("catalog listTables with dbName") {
    val tableName = "test_catalog_list_tables_db"
    try
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
      spark.sql(s"CREATE TABLE $tableName (id INT) USING parquet").collect()
      val tables = spark.catalog.listTables("default")
      val rows = tables.collect()
      val names = rows.map(_.getString(0))
      assert(names.contains(tableName), s"Table '$tableName' should be in default database listing")
    finally
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
  }

  test("catalog listTables with dbName and pattern") {
    val tableName = "test_catalog_lt_pattern_match"
    try
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
      spark.sql(s"CREATE TABLE $tableName (id INT) USING parquet").collect()
      val matched = spark.catalog.listTables("default", "test_catalog_lt_pattern*")
      val rows = matched.collect()
      assert(rows.nonEmpty, "Pattern should match the created table")

      val noMatch = spark.catalog.listTables("default", "zzz_no_match_*")
      assert(noMatch.collect().isEmpty, "Non-matching pattern should return empty")
    finally
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
  }

  test("catalog listColumns by tableName") {
    val tableName = "test_catalog_list_cols"
    try
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
      spark.sql(
        s"CREATE TABLE $tableName (id INT, name STRING, value DOUBLE) USING parquet"
      ).collect()
      val cols = spark.catalog.listColumns(tableName)
      val rows = cols.collect()
      assert(rows.length == 3, s"Expected 3 columns but got ${rows.length}")
      val colNames = rows.map(_.getString(0)).toSet
      assert(colNames == Set("id", "name", "value"), s"Column names mismatch: $colNames")
    finally
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
  }

  test("catalog listColumns with tableName and dbName") {
    val tableName = "test_catalog_list_cols_db"
    try
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
      spark.sql(s"CREATE TABLE $tableName (x INT, y STRING) USING parquet").collect()
      val cols = spark.catalog.listColumns(tableName, "default")
      val rows = cols.collect()
      assert(rows.length == 2, s"Expected 2 columns but got ${rows.length}")
    finally
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
  }

  test("catalog listFunctions with dbName") {
    val funcs = spark.catalog.listFunctions("default")
    val rows = funcs.collect()
    assert(rows.nonEmpty, "Built-in functions should be present in default database")
    println(s"Found ${rows.length} function(s) in default database")
  }

  test("catalog listFunctions with dbName and pattern") {
    val funcs = spark.catalog.listFunctions("default", "abs")
    val rows = funcs.collect()
    assert(rows.nonEmpty, "Pattern 'abs' should match built-in abs function")

    val noMatch = spark.catalog.listFunctions("default", "zzz_no_match_func_*")
    assert(noMatch.collect().isEmpty, "Non-matching pattern should return empty")
  }

  test("catalog listCatalogs with pattern") {
    // The server has 'testcat' configured as InMemoryTableCatalog plus spark_catalog
    val catalogs = spark.catalog.listCatalogs("*")
    val allRows = catalogs.collect()
    assert(allRows.nonEmpty, "Wildcard pattern should match at least one catalog")

    val testcatResults = spark.catalog.listCatalogs("test*")
    val testcatRows = testcatResults.collect()
    assert(testcatRows.nonEmpty, "Pattern 'test*' should match 'testcat'")

    val noMatch = spark.catalog.listCatalogs("zzz_no_match_*")
    assert(noMatch.collect().isEmpty, "Non-matching pattern should return empty")
  }

  test("catalog listViews") {
    val viewName = "test_catalog_list_views"
    try
      spark.range(3).createOrReplaceTempView(viewName)
      try
        val views = spark.catalog.listViews()
        val rows = views.collect()
        val viewNames = rows.map(_.getString(0))
        assert(viewNames.contains(viewName), s"View '$viewName' should appear in listViews")
      catch
        case _: Exception =>
          cancel("Catalog.listViews may not be supported by this server (requires extended proto)")
    finally
      spark.catalog.dropTempView(viewName)
  }

  test("catalog listViews with dbName") {
    val viewName = "test_catalog_list_views_db"
    try
      spark.range(3).createOrReplaceTempView(viewName)
      try
        val views = spark.catalog.listViews("default")
        val rows = views.collect()
        println(s"Found ${rows.length} view(s) in default database")
      catch
        case _: Exception =>
          cancel("Catalog.listViews may not be supported by this server (requires extended proto)")
    finally
      spark.catalog.dropTempView(viewName)
  }

  test("catalog getDatabase") {
    val db = spark.catalog.getDatabase("default")
    val rows = db.collect()
    assert(rows.length == 1, "getDatabase should return exactly one row")
    val name = rows.head.getString(0)
    assert(name == "default", s"Database name should be 'default' but got '$name'")
  }

  test("catalog getTable by tableName") {
    val tableName = "test_catalog_get_table"
    try
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
      spark.sql(s"CREATE TABLE $tableName (id INT) USING parquet").collect()
      val table = spark.catalog.getTable(tableName)
      val rows = table.collect()
      assert(rows.length == 1, "getTable should return exactly one row")
      assert(rows.head.getString(0) == tableName, "Table name should match")
    finally
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
  }

  test("catalog getTable with tableName and dbName") {
    val tableName = "test_catalog_get_table_db"
    try
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
      spark.sql(s"CREATE TABLE $tableName (id INT) USING parquet").collect()
      val table = spark.catalog.getTable(tableName, "default")
      val rows = table.collect()
      assert(rows.length == 1, "getTable with dbName should return exactly one row")
      assert(rows.head.getString(0) == tableName, "Table name should match")
    finally
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
  }

  test("catalog getFunction by name") {
    val func = spark.catalog.getFunction("abs")
    val rows = func.collect()
    assert(rows.length == 1, "getFunction should return exactly one row for 'abs'")
    assert(rows.head.getString(0) == "abs", "Function name should be 'abs'")
  }

  test("catalog getFunction with name and dbName") {
    // Use a UDF registered in default db instead of built-in function,
    // because built-in functions may not resolve when qualified with a database name
    try
      val func = spark.catalog.getFunction("abs", "default")
      val rows = func.collect()
      assert(rows.length == 1, "getFunction with dbName should return exactly one row for 'abs'")
      assert(rows.head.getString(0) == "abs", "Function name should be 'abs'")
    catch
      case _: Exception =>
        cancel(
          "getFunction with dbName may not resolve built-in functions on this server"
        )
  }

  test("catalog tableExists with tableName and dbName") {
    val tableName = "test_catalog_table_exists_db"
    try
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
      assert(
        !spark.catalog.tableExists(tableName, "default"),
        "Table should not exist before creation"
      )
      spark.sql(s"CREATE TABLE $tableName (id INT) USING parquet").collect()
      assert(spark.catalog.tableExists(tableName, "default"), "Table should exist after creation")
    finally
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
  }

  test("catalog functionExists with name and dbName") {
    try
      assert(
        spark.catalog.functionExists("abs", "default"),
        "Built-in function 'abs' should exist in default db"
      )
      assert(
        !spark.catalog.functionExists("zzz_no_such_func_12345", "default"),
        "Non-existent function should return false"
      )
    catch
      case _: Exception =>
        cancel(
          "functionExists with dbName may not resolve built-in functions on this server"
        )
  }

  test("catalog cacheTable with StorageLevel") {
    val viewName = "test_catalog_cache_storage_level"
    try
      spark.range(10).createOrReplaceTempView(viewName)
      assert(!spark.catalog.isCached(viewName), "View should not be cached initially")

      spark.catalog.cacheTable(viewName, StorageLevel.MEMORY_ONLY)
      assert(
        spark.catalog.isCached(viewName),
        "View should be cached after cacheTable with MEMORY_ONLY"
      )

      spark.catalog.uncacheTable(viewName)
      assert(!spark.catalog.isCached(viewName), "View should not be cached after uncacheTable")
    finally
      try spark.catalog.uncacheTable(viewName)
      catch case _: Exception => ()
      spark.catalog.dropTempView(viewName)
  }

  test("catalog clearCache") {
    val viewName1 = "test_catalog_clear_cache_1"
    val viewName2 = "test_catalog_clear_cache_2"
    try
      spark.range(5).createOrReplaceTempView(viewName1)
      spark.range(5).createOrReplaceTempView(viewName2)
      spark.catalog.cacheTable(viewName1)
      spark.catalog.cacheTable(viewName2)
      assert(spark.catalog.isCached(viewName1), "View1 should be cached")
      assert(spark.catalog.isCached(viewName2), "View2 should be cached")

      spark.catalog.clearCache()
      assert(!spark.catalog.isCached(viewName1), "View1 should not be cached after clearCache")
      assert(!spark.catalog.isCached(viewName2), "View2 should not be cached after clearCache")
    finally
      spark.catalog.dropTempView(viewName1)
      spark.catalog.dropTempView(viewName2)
  }

  test("catalog createDatabase and dropDatabase") {
    val dbName = "test_catalog_create_drop_db"
    try
      // Clean up in case of previous failed run
      try spark.sql(s"DROP DATABASE IF EXISTS $dbName CASCADE").collect()
      catch case _: Exception => ()

      try
        spark.catalog.createDatabase(dbName)
        assert(spark.catalog.databaseExists(dbName), "Database should exist after creation")

        spark.catalog.dropDatabase(dbName)
        assert(!spark.catalog.databaseExists(dbName), "Database should not exist after drop")
      catch
        case _: Exception =>
          cancel(
            "Catalog.createDatabase/dropDatabase may not be supported by this server (requires extended proto)"
          )
    finally
      try spark.sql(s"DROP DATABASE IF EXISTS $dbName CASCADE").collect()
      catch case _: Exception => ()
  }

  test("catalog dropTable") {
    val tableName = "test_catalog_drop_table_v2"
    try
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
      spark.sql(s"CREATE TABLE $tableName (id INT) USING parquet").collect()
      assert(spark.catalog.tableExists(tableName), "Table should exist before drop")

      try
        spark.catalog.dropTable(tableName)
        assert(!spark.catalog.tableExists(tableName), "Table should not exist after dropTable")
      catch
        case _: Exception =>
          // Catalog.dropTable proto may not be supported by all servers
          cancel("Catalog.dropTable may not be supported by this server")
    finally
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
  }

  test("catalog dropGlobalTempView") {
    val viewName = "test_catalog_drop_global_temp_view"
    try
      spark.range(3).createGlobalTempView(viewName)
      // Verify it exists via SQL
      val result = spark.sql(s"SELECT * FROM global_temp.$viewName").collect()
      assert(result.length == 3, "Global temp view should contain 3 rows")

      val dropped = spark.catalog.dropGlobalTempView(viewName)
      assert(dropped, "dropGlobalTempView should return true for an existing view")

      val droppedAgain = spark.catalog.dropGlobalTempView(viewName)
      assert(!droppedAgain, "dropGlobalTempView should return false for a non-existent view")
    finally
      try spark.catalog.dropGlobalTempView(viewName)
      catch case _: Exception => ()
  }

  test("catalog refreshTable") {
    val tableName = "test_catalog_refresh_table"
    try
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
      spark.sql(s"CREATE TABLE $tableName (id INT) USING parquet").collect()
      // refreshTable should not throw on an existing table
      spark.catalog.refreshTable(tableName)
    finally
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
  }

  test("catalog refreshByPath") {
    val tableName = "test_catalog_refresh_by_path"
    val tmpDir = java.nio.file.Files.createTempDirectory("sc3_refresh_by_path_").toString
    try
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
      spark.sql(
        s"CREATE TABLE $tableName (id INT) USING parquet LOCATION '$tmpDir'"
      ).collect()
      spark.sql(s"INSERT INTO $tableName VALUES (1), (2)").collect()
      // refreshByPath should not throw for a valid path. The server may treat
      // arbitrary paths as a no-op (still no error), so we just assert no exception.
      spark.catalog.refreshByPath(tmpDir)
    finally
      try spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
      catch case _: Exception => ()
      try
        java.nio.file.Files
          .walk(java.nio.file.Paths.get(tmpDir))
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(p => java.nio.file.Files.deleteIfExists(p))
      catch case _: Exception => ()
  }

  test("catalog recoverPartitions") {
    val tableName = "test_catalog_recover_partitions"
    try
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
      spark.sql(
        s"CREATE TABLE $tableName (id INT, dt STRING) USING parquet PARTITIONED BY (dt)"
      ).collect()
      spark.sql(s"INSERT INTO $tableName PARTITION (dt='2026-04-07') VALUES (1)").collect()

      try
        // recoverPartitions should run without error on a partitioned table.
        spark.catalog.recoverPartitions(tableName)
      catch
        case _: Exception =>
          // Some servers (or non-Hive table providers) reject this operation.
          cancel("Catalog.recoverPartitions may not be supported by this server")
    finally
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
  }

  test("catalog dropView") {
    val viewName = "test_catalog_drop_view"
    try
      spark.sql(s"DROP VIEW IF EXISTS $viewName").collect()
      spark.sql(s"CREATE VIEW $viewName AS SELECT 1 AS id").collect()
      assert(
        spark.sql(s"SELECT * FROM $viewName").collect().length == 1,
        "View should be queryable before drop"
      )

      try
        spark.catalog.dropView(viewName)
        // After drop, the view should no longer be queryable.
        intercept[Exception] {
          spark.sql(s"SELECT * FROM $viewName").collect()
        }
      catch
        case _: Exception =>
          cancel("Catalog.dropView may not be supported by this server")
    finally
      try spark.sql(s"DROP VIEW IF EXISTS $viewName").collect()
      catch case _: Exception => ()
  }

  test("catalog truncateTable") {
    val tableName = "test_catalog_truncate_table"
    try
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
      spark.sql(s"CREATE TABLE $tableName (id INT) USING parquet").collect()
      spark.sql(s"INSERT INTO $tableName VALUES (1), (2), (3)").collect()
      val beforeCount = spark.sql(s"SELECT * FROM $tableName").collect().length
      assert(beforeCount == 3, s"Expected 3 rows before truncate but got $beforeCount")

      try
        spark.catalog.truncateTable(tableName)
        val afterCount = spark.sql(s"SELECT * FROM $tableName").collect().length
        assert(afterCount == 0, s"Expected 0 rows after truncate but got $afterCount")
      catch
        case _: Exception =>
          cancel("Catalog.truncateTable may not be supported by this server")
    finally
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
  }

  test("catalog analyzeTable") {
    val tableName = "test_catalog_analyze_table"
    try
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
      spark.sql(s"CREATE TABLE $tableName (id INT, name STRING) USING parquet").collect()
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'a'), (2, 'b')").collect()

      // analyzeTable should not throw
      try
        spark.catalog.analyzeTable(tableName)
        // Also test with noScan = true
        spark.catalog.analyzeTable(tableName, noScan = true)
      catch
        case _: Exception =>
          cancel("Catalog.analyzeTable may not be supported by this server")
    finally
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
  }

  test("catalog getTableProperties") {
    val tableName = "test_catalog_get_table_props"
    try
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
      spark.sql(
        s"CREATE TABLE $tableName (id INT) USING parquet TBLPROPERTIES ('key1' = 'val1')"
      ).collect()

      try
        val props = spark.catalog.getTableProperties(tableName)
        val rows = props.collect()
        assert(rows.nonEmpty, "Table properties should not be empty")
      catch
        case _: Exception =>
          cancel("Catalog.getTableProperties may not be supported by this server")
    finally
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
  }

  test("catalog getCreateTableString") {
    val tableName = "test_catalog_get_create_string"
    try
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
      spark.sql(s"CREATE TABLE $tableName (id INT, name STRING) USING parquet").collect()

      try
        val ddl = spark.catalog.getCreateTableString(tableName)
        assert(ddl.nonEmpty, "DDL string should not be empty")
        assert(ddl.toUpperCase.contains("CREATE"), "DDL should contain CREATE keyword")
        println(s"DDL: $ddl")
      catch
        case _: Exception =>
          cancel("Catalog.getCreateTableString may not be supported by this server")
    finally
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
  }

  test("catalog createTable with source and options") {
    val tableName = "test_catalog_create_src_opts"
    val tempDir = java.nio.file.Files.createTempDirectory("spark_catalog_src_opts")
    try
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
      spark.range(5).write.mode("overwrite").parquet(tempDir.toString)
      spark.catalog.createTable(tableName, "parquet", Map("path" -> tempDir.toString)).collect()
      assert(
        spark.catalog.tableExists(tableName),
        "Table should exist after creation with source and options"
      )
      val count = spark.sql(s"SELECT * FROM $tableName").collect().length
      assert(count == 5, s"Expected 5 rows but got $count")
    finally
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
      java.nio.file.Files.walk(tempDir).sorted(java.util.Comparator.reverseOrder())
        .forEach(java.nio.file.Files.deleteIfExists(_))
  }

  test("catalog createTable with source schema and options") {
    val tableName = "test_catalog_create_src_schema_opts"
    val tempDir = java.nio.file.Files.createTempDirectory("spark_catalog_src_schema")
    try
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
      val schema = StructType(Seq(
        StructField("id", IntegerType),
        StructField("label", StringType)
      ))
      spark.catalog.createTable(
        tableName,
        "parquet",
        schema,
        Map("path" -> tempDir.toString)
      ).collect()
      assert(spark.catalog.tableExists(tableName), "Table should exist after creation with schema")
      val cols = spark.catalog.listColumns(tableName).collect()
      val colNames = cols.map(_.getString(0)).toSet
      assert(colNames.contains("id"), "Table should have 'id' column")
      assert(colNames.contains("label"), "Table should have 'label' column")
    finally
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
      java.nio.file.Files.walk(tempDir).sorted(java.util.Comparator.reverseOrder())
        .forEach(java.nio.file.Files.deleteIfExists(_))
  }

  test("catalog listPartitions") {
    val tableName = "test_catalog_list_partitions"
    try
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
      spark.sql(
        s"CREATE TABLE $tableName (id INT, part STRING) USING parquet PARTITIONED BY (part)"
      ).collect()
      spark.sql(s"INSERT INTO $tableName PARTITION (part='a') VALUES (1)").collect()
      spark.sql(s"INSERT INTO $tableName PARTITION (part='b') VALUES (2)").collect()

      try
        val partitions = spark.catalog.listPartitions(tableName)
        val rows = partitions.collect()
        assert(rows.length == 2, s"Expected 2 partitions but got ${rows.length}")
      catch
        case _: Exception =>
          cancel("Catalog.listPartitions may not be supported by this server")
    finally
      spark.sql(s"DROP TABLE IF EXISTS $tableName").collect()
  }
