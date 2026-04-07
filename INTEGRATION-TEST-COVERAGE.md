# Integration Test Coverage Analysis

This document tracks integration test coverage for each public API class. Integration tests run against a live Spark Connect server (4.1.x) and validate real end-to-end behavior.

Last updated: 2026-04-07

---

## Summary

| Class | Total | Tested | Gaps | Coverage |
|-------|-------|--------|------|----------|
| TableValuedFunction | 14 | 0 | 14 | **0%** |
| SparkSession companion | 7 | 1 | 6 | 14% |
| Catalog | ~45 | 14 | ~31 | 31% |
| Dataset[T] typed | ~70 | ~25 | ~45 | 36% |
| KeyValueGroupedDataset | ~18 | 7 | ~11 | 39% |
| SparkSession.Builder | 5 | 2 | 3 | 40% |
| UserDefinedFunction | 5 | 2 | 3 | 40% |
| MergeIntoWriter | ~15 | 7 | ~8 | 47% |
| DataStreamReader | 7 | 4 | 3 | 57% |
| DataFrameWriterV2 | 14 | 8 | 6 | 57% |
| SparkSession | 30 | 18 | 12 | 60% |
| StreamingQueryManager | 8 | 5 | 3 | 63% |
| Observation | 3 | 2 | 1 | 67% |
| DataFrame | ~95 | ~75 | ~20 | 79% |
| Window | ~10 | ~8 | ~2 | 80% |
| DataStreamWriter | 11 | 9 | 2 | 82% |
| DataFrameWriter | 18 | 15 | 3 | 83% |
| StreamingQuery | 12 | 10 | 2 | 83% |
| Column | ~75 | ~65 | ~10 | 87% |
| DataFrameReader | 14 | 13 | 1 | 93% |
| RuntimeConfig | 9 | 9 | 0 | 100% |
| GroupedDataFrame | 10 | 10 | 0 | 100% |
| DataFrameNaFunctions | 9 | 9 | 0 | 100% |
| DataFrameStatFunctions | 9 | 9 | 0 | 100% |
| UDFRegistration | 1 | 1 | 0 | 100% |

---

## Detailed Gaps by Class

### 1. TableValuedFunction — 0% (14 gaps)

Test suite: **None**

All methods untested:

| Method | Notes |
|--------|-------|
| `range` (3 overloads) | start/end/step/numPartitions |
| `explode` / `explode_outer` | Array/Map column explosion |
| `posexplode` / `posexplode_outer` | With position index |
| `inline` / `inline_outer` | Array of structs to columns |
| `json_tuple` | Extract fields from JSON string |
| `stack` | Pivot rows to columns |
| `collations` | List available collations |
| `sql_keywords` | List SQL reserved keywords |
| `variant_explode` / `variant_explode_outer` | Variant type explosion |

### 2. SparkSession companion object — 14% (6 gaps)

Test suite: **SparkSessionIntegrationSuite**

Tested: `builder`

| Untested Method | Notes |
|----------------|-------|
| `getActiveSession` | Thread-local session retrieval |
| `getDefaultSession` | Global default session retrieval |
| `active` | Get or throw active session |
| `setActiveSession` | Set thread-local session |
| `clearActiveSession` | Clear thread-local session |
| `setDefaultSession` / `clearDefaultSession` | Global default management |

### 3. Catalog — 31% (31 gaps)

Test suite: **CatalogIntegrationSuite**

Tested: `listDatabases`, `listTables`, `listFunctions`, `listCatalogs`, `currentDatabase`, `setCurrentDatabase`, `currentCatalog`, `tableExists`, `functionExists`, `databaseExists`, `createTable(name, path, source)`, `dropTempView`, `cacheTable`, `isCached`, `uncacheTable`

| Untested Method | Notes |
|----------------|-------|
| `setCurrentCatalog` | Switch active catalog |
| `listDatabases(pattern)` | Filtered listing |
| `listTables(dbName)` / `listTables(dbName, pattern)` | Database-scoped listing |
| `listColumns(tableName)` / `listColumns(dbName, tableName)` | Column metadata |
| `listFunctions(dbName)` / `listFunctions(dbName, pattern)` | Database-scoped listing |
| `listCatalogs(pattern)` | Filtered catalog listing |
| `listCachedTables` | Requires SPARK-56221 on server |
| `listPartitions` | Table partition listing |
| `listViews` (all overloads) | View listing |
| `getDatabase` | Database metadata |
| `getTable(tableName)` / `getTable(dbName, tableName)` | Table metadata |
| `getFunction(name)` / `getFunction(dbName, name)` | Function metadata |
| `getTableProperties` | Table property map |
| `getCreateTableString` | DDL string |
| `tableExists(dbName, tableName)` | Two-arg overload |
| `functionExists(dbName, functionName)` | Two-arg overload |
| `cacheTable(tableName, storageLevel)` | With storage level |
| `clearCache` | Clear all cached tables |
| `createTable` (4 remaining overloads) | schema/options variants |
| `createExternalTable` (4 deprecated overloads) | Deprecated aliases |
| `createDatabase` | Create new database |
| `dropDatabase` | Drop database |
| `dropTable` | Drop managed table |
| `dropView` | Drop permanent view |
| `dropGlobalTempView` | Drop global temp view |
| `refreshTable` | Invalidate table cache |
| `refreshByPath` | Invalidate by path |
| `recoverPartitions` | Repair partitions |
| `truncateTable` | Truncate table data |
| `analyzeTable` | Compute table statistics |

### 4. Dataset[T] typed operations — 36% (45 gaps)

Test suites: **DatasetIntegrationSuite**, **DatasetTypedOpsIntegrationSuite**

Tested: `map`, `flatMap`, `filter` (typed), `mapPartitions`, `reduce`, `foreach`, `foreachPartition`, `joinWith`, `toJSON`, `toLocalIterator`, `as[U]`, `transform`, `randomSplit`, `collectAsList`, `takeAsList`, `first`, `head`, `tail`

| Untested Method | Notes |
|----------------|-------|
| `select` (typed 1-5 arity) | `select(c1: TypedColumn)` through 5-arity |
| `where` (typed predicate) | `where(func: T => Boolean)` |
| `sort` / `orderBy` (with Ordering) | Typed ordering |
| `sample` (typed overloads) | `sample(fraction)`, `sample(withReplacement, fraction, seed)` |
| `repartition` / `coalesce` (typed) | Via Dataset[T] interface |
| `sortWithinPartitions` | Typed variant |
| `observe` (both overloads) | Metrics collection on typed Dataset |
| `describe` / `summary` | Statistical summaries |
| `union` / `unionByName` | Typed set union |
| `intersect` / `intersectAll` | Typed set intersection |
| `except` / `exceptAll` | Typed set difference |
| `dropDuplicates` (typed overloads) | Dedup on typed Dataset |
| `dropDuplicatesWithinWatermark` | Streaming dedup |
| `alias` | Dataset alias |
| `cache` / `persist` / `unpersist` | Typed caching |
| `checkpoint` / `localCheckpoint` | Typed checkpointing |
| `count` / `show` / `isEmpty` | Via typed Dataset |
| `columns` / `dtypes` / `col` / `apply` | Schema accessors |
| `explain` / `printSchema` | Plan/schema display |
| `isStreaming` / `isLocal` / `inputFiles` | Dataset metadata |
| `sameSemantics` / `semanticHash` / `storageLevel` | Plan comparison |
| `toDF()` / `toDF(colNames*)` | Convert to untyped |
| `na` / `stat` / `hint` / `crossJoin` | Delegating wrappers |
| `write` / `writeTo` / `writeStream` | Output access |
| `createTempView` / `createOrReplaceTempView` | View creation |
| `createGlobalTempView` / `createOrReplaceGlobalTempView` | Global view creation |
| `withWatermark` | Streaming watermark |
| `scalar` / `exists` | Subquery (tested via SubqueryIntegrationSuite on DataFrame) |

> **Note**: Many of these are thin delegating wrappers to DataFrame. Priority should be on truly typed-specific methods: typed `select` (1-5 arity), typed `where(predicate)`, typed set operations, `observe`.

### 5. KeyValueGroupedDataset — 39% (11 gaps)

Test suite: **KeyValueGroupedDatasetIntegrationSuite**

Tested: `keys`, `count`, `mapGroups`, `flatMapGroups`, `reduceGroups`, `mapValues`, `flatMapSortedGroups`, `cogroup`

| Untested Method | Notes |
|----------------|-------|
| `keyAs` | Re-key with different encoder |
| `agg` (1-4 arity overloads) | Typed aggregation with TypedColumn |
| `mapGroupsWithState` (2 overloads) | Stateful processing |
| `flatMapGroupsWithState` (2 overloads) | Stateful flatMap |
| `transformWithState` (3 overloads) | New stateful API |

> **Note**: `mapGroupsWithState`, `flatMapGroupsWithState`, and `transformWithState` require lambda serialization which is blocked by the Scala 3/2.13 incompatibility. These should be tested with `withLambdaCompat`.

### 6. SparkSession.Builder — 40% (3 gaps)

Tested: `remote`, `build`

| Untested Method | Notes |
|----------------|-------|
| `config` | Set config key-value pair |
| `create` | Create new session (always new) |
| `getOrCreate` | Get existing or create new |

### 7. UserDefinedFunction — 40% (3 gaps)

Test suite: **UdfIntegrationSuite**

Tested: `apply` (column invocation), `name` (via register)

| Untested Method | Notes |
|----------------|-------|
| `withName` | Set UDF display name |
| `asNonNullable` | Mark UDF as non-nullable |
| `asNondeterministic` | Mark UDF as nondeterministic |

### 8. MergeIntoWriter — 47% (8 gaps)

Test suite: **WriterIntegrationSuite** (fluent chain only, no real execution)

Tested (chain only): `whenMatched`, `whenNotMatched`, `whenNotMatchedBySource`, `updateAll`, `insertAll`, `delete`, `withSchemaEvolution`

| Untested Method | Notes |
|----------------|-------|
| `merge()` | **Actual execution** — no real merge tested |
| `whenMatched(condition)` | With predicate filter |
| `whenNotMatched(condition)` | With predicate filter |
| `whenNotMatchedBySource(condition)` | With predicate filter |
| `WhenMatched.update(Map)` | Column-level update expressions |
| `WhenNotMatched.insert(Map)` | Column-level insert expressions |
| `WhenNotMatchedBySource.update(Map)` | Column-level update expressions |

> **Note**: Testing `merge()` requires a V2 table catalog (e.g., InMemoryTableCatalog configured in CI).

### 9. DataStreamReader — 57% (3 gaps)

Test suite: **StreamingReadWriteIntegrationSuite**

Tested: `format`, `option`, `schema` (DDL string), `load`

| Untested Method | Notes |
|----------------|-------|
| `options(Map)` | Bulk options |
| `load(paths: String*)` | Multi-path load |
| `table` | Read streaming table |

### 10. DataFrameWriterV2 — 57% (6 gaps)

Test suite: **ReadWriteIntegrationSuite**, **WriterIntegrationSuite**

Tested: `using`, `option`, `tableProperty`, `create`, `createOrReplace`, `append`

| Untested Method | Notes |
|----------------|-------|
| `replace` | Replace existing table |
| `overwrite` | Overwrite with condition |
| `overwritePartitions` | Dynamic partition overwrite |
| `partitionedBy` | Set partitioning |
| `clusterBy` | Set clustering |
| `options(Map)` | Bulk options |

> **Note**: Requires V2 table catalog for real execution.

### 11. SparkSession — 60% (12 gaps)

Test suite: **SparkSessionIntegrationSuite**

Tested: `sessionId`, `version`, `sql` (named args, positional args), `emptyDataFrame`, `emptyDataset`, `createDataset`, `createDataFrame`, `table`, `range`, `newSession`, `cloneSession`, `addTag`, `removeTag`, `getTags`, `clearTags`

| Untested Method | Notes |
|----------------|-------|
| `tvf` | TableValuedFunction access |
| `interruptAll` | Cancel all running operations |
| `interruptTag` | Cancel operations by tag |
| `interruptOperation` | Cancel specific operation |
| `stop` / `close` | Session shutdown |
| `executeCommand` | DeveloperApi command execution |
| `addArtifact` / `addClassDir` | Only used internally in test base |
| `registerClassFinder` | Custom class discovery |

### 12. StreamingQueryManager — 63% (3 gaps)

Test suite: **StreamingReadWriteIntegrationSuite**

Tested: `active`, `get`, `awaitAnyTermination(timeout)`, `resetTerminated`

| Untested Method | Notes |
|----------------|-------|
| `awaitAnyTermination()` | No-timeout overload (blocks forever) |
| `addListener` / `removeListener` / `listListeners` | StreamingQueryListener management |

### 13. Observation — 67% (1 gap)

Test suite: **DataFrameIntegrationSuite**

Tested: `name`, `observe` (via DataFrame)

| Untested Method | Notes |
|----------------|-------|
| `get` / `future` | Retrieve observation metrics after query completes |

### 14. DataFrame — 79% (20 gaps)

Test suite: **DataFrameIntegrationSuite**, **SubqueryIntegrationSuite**

| Untested Method | Notes |
|----------------|-------|
| `lateralJoin` (2 overloads) | Lateral subquery join |
| `dropDuplicatesWithinWatermark` (3 overloads) | Streaming-specific dedup |
| `broadcast` | Broadcast hint |
| `persist(StorageLevel)` | With explicit storage level |
| `toDF(colNames: String*)` | Rename columns |
| `createOrReplaceGlobalTempView` | Global temp view creation |
| `mergeInto` | Merge entry point (see MergeIntoWriter) |
| `show(numRows, truncate)` | Overload with both params |
| `withWatermark` | Streaming watermark |
| `metadataColumn` | Access hidden metadata columns |
| `as(alias: Symbol)` | Symbol alias overload |

### 15. Window — 80% (2 gaps)

| Untested Method | Notes |
|----------------|-------|
| `WindowSpec.orderBy(cols: Column*)` | Multi-column order |
| `Window.partitionBy(colNames: String*)` | String-based partition |

### 16. DataStreamWriter — 82% (2 gaps)

| Untested Method | Notes |
|----------------|-------|
| `options(Map)` | Bulk options |
| `foreach` | Row-level ForeachWriter (not foreachBatch) |

### 17. DataFrameWriter — 83% (3 gaps)

| Untested Method | Notes |
|----------------|-------|
| `bucketBy` | Hash bucketing |
| `sortBy` | Sort within buckets |
| `jdbc` | JDBC write (requires external DB) |

### 18. StreamingQuery — 83% (2 gaps)

| Untested Method | Notes |
|----------------|-------|
| `processAllAvailable` | Block until all available data processed |
| `explain` / `explain(extended)` | Query plan display |

### 19. Column — 87% (10 gaps)

| Untested Method | Notes |
|----------------|-------|
| `getItem` (Column key) | Dynamic key access |
| `as(alias: Symbol)` | Symbol overload |
| `as(aliases: Array[String])` | Multi-alias overload |

### 20. DataFrameReader — 93% (1 gap)

| Untested Method | Notes |
|----------------|-------|
| `load(paths: String*)` | Multi-path load |

---

## Classes with 100% Coverage

- **RuntimeConfig** — get, getOption, getAll, set (String/Boolean/Long), unset, isModifiable
- **GroupedDataFrame** — agg, count, mean, avg, max, min, sum, pivot, rollup, cube
- **DataFrameNaFunctions** — drop (all overloads), fill (all overloads), replace
- **DataFrameStatFunctions** — corr, cov, crosstab, approxQuantile, freqItems, sampleBy
- **UDFRegistration** — register

---

## Priority Recommendations

### P0 — High value, straightforward to test

1. **TableValuedFunction**: `explode`, `posexplode`, `inline`, `range`, `json_tuple`, `stack`
2. **Catalog**: `listColumns`, `getDatabase`, `getTable`, `getFunction`, `createDatabase`/`dropDatabase`, `clearCache`, `setCurrentCatalog`, `listViews`
3. **MergeIntoWriter**: Real `merge()` execution with V2 catalog
4. **DataFrameWriterV2**: `overwrite`, `overwritePartitions`, `partitionedBy`

### P1 — Moderate value

5. **SparkSession**: `interruptAll`/`interruptTag`/`interruptOperation`, `stop`
6. **KeyValueGroupedDataset**: `agg` (typed), `keyAs`
7. **DataFrame**: `lateralJoin`, `broadcast`, `persist(StorageLevel)`, `toDF(colNames*)`
8. **StreamingQueryManager**: `addListener`/`removeListener`
9. **UserDefinedFunction**: `withName`, `asNonNullable`, `asNondeterministic`

### P2 — Lower priority or blocked

10. **KeyValueGroupedDataset**: `mapGroupsWithState`, `flatMapGroupsWithState`, `transformWithState` (blocked by Scala 3/2.13 lambda incompatibility)
11. **Dataset[T] typed wrappers**: Most are thin delegations to DataFrame; typed `select` (1-5 arity) and typed set operations are the main gaps
12. **SparkSession companion**: Session lifecycle management (mostly useful for multi-session scenarios)
13. **DataFrameWriter**: `jdbc` (requires external database)
