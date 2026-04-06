# API Gap Analysis: SC3 vs Official Spark Connect Client

This document tracks feature gaps between the Spark Connect Scala 3 client (SC3) and the official Spark Connect client (`sql/connect/common/src/main/scala/org/apache/spark/sql/connect/`).

Last updated: 2026-04-06

---

## 1. SparkSession Gaps

### HIGH PRIORITY

| API | Description | Status |
|-----|-------------|--------|
| `implicits` object | Implicit conversions (`$"colName"`, `Seq[T].toDF()`, `.toDS()`) -- fundamental for idiomatic Spark Scala | RESOLVED |
| `newSession()` | Create a new independent session sharing the same connection | RESOLVED |
| `udf: UDFRegistration` | Register UDFs for use in SQL queries | RESOLVED |
| `createDataset[T: Encoder](data: Seq[T])` | Create typed Dataset from local Seq | RESOLVED |
| `readStream: DataStreamReader` | Entry point for structured streaming | RESOLVED |
| `streams: StreamingQueryManager` | Manage and monitor streaming queries | RESOLVED |
| `sql(sqlText, args: Map)` | Parameterized SQL queries (named and positional arguments) | RESOLVED |
| `cloneSession()` | Clone session via `CloneSession` RPC preserving config/views/UDFs | RESOLVED |

### MEDIUM PRIORITY

| API | Description | Status |
|-----|-------------|--------|
| `createDataFrame[A <: Product](data: Seq[A])` | Create DataFrame from case class sequence (uses TypeTag) | RESOLVED |
| `range(start, end, step, numPartitions)` | Range with custom partition count | RESOLVED |
| `emptyDataFrame(schema)` | Empty DataFrame with specified schema | RESOLVED |
| `emptyDataset[T: Encoder]` | Empty typed Dataset | RESOLVED |
| `addArtifact(path/uri/bytes)` | Upload JARs/files to the server | RESOLVED |
| `addTag(tag)` / `removeTag` / `getTags` / `clearTags` | Operation tagging | RESOLVED |
| `interruptAll()` / `interruptTag(tag)` / `interruptOperation(id)` | Cancel running operations | RESOLVED |
| `time[T](f)` | Measure execution time | RESOLVED |
| `tvf: TableValuedFunction` | Access table-valued functions (explode, inline, posexplode, json_tuple, stack, etc.) | RESOLVED |
| `executeCommand(runner, command, options)` | Execute arbitrary commands via `ExecuteExternalCommand` proto | RESOLVED |
| `getOrCreate()` on Builder | Reuse existing session | RESOLVED |

### LOW PRIORITY

| API | Description | Status |
|-----|-------------|--------|
| `active` / `getActiveSession` / `getDefaultSession` | Static session management | RESOLVED |
| `setActiveSession` / `clearActiveSession` / `setDefaultSession` / `clearDefaultSession` | Thread-local session binding | RESOLVED |
| `withActive[T](block)` | Execute block with this session as active | RESOLVED |
| `conf.getAll` / `conf.unset` / `conf.isModifiable` | Additional config operations | RESOLVED |

---

## 2. DataFrame / Dataset Gaps

### HIGH PRIORITY

| API | Description | Status |
|-----|-------------|--------|
| `toDF()` / `toDF(colNames: String*)` | Convert typed Dataset back to untyped / rename columns | RESOLVED |
| `cache()` / `persist()` / `persist(StorageLevel)` | Cache DataFrame with specific storage level | RESOLVED |
| `unpersist()` / `unpersist(blocking)` | Remove from cache | RESOLVED |
| `storageLevel` | Query current caching level via `AnalyzePlan` RPC | RESOLVED |
| `checkpoint()` / `checkpoint(eager)` | Reliable checkpoint to HDFS | RESOLVED |
| `localCheckpoint()` | Local (non-replicated) checkpoint | RESOLVED |
| `toLocalIterator()` | Memory-efficient row-by-row lazy streaming Iterator | RESOLVED |
| `foreach(f)` / `foreachPartition(f)` | Apply side-effect function to each row/partition | RESOLVED |
| `tail(n: Int)` | Get last N rows | RESOLVED |
| `toJSON` | Convert each row to JSON string via `to_json(struct(*))` | RESOLVED |
| `inputFiles` | Return source file paths via `AnalyzePlan` RPC | RESOLVED |
| `isLocal` | Whether the Dataset can be computed locally via `AnalyzePlan.IsLocal` RPC | RESOLVED |
| `withColumnsRenamed(Map)` | Batch-rename multiple columns at once | RESOLVED |
| `transform(f: DataFrame => DataFrame)` | Fluent pipeline transformation | RESOLVED |
| `observe(name, expr, exprs*)` | Observe metrics during execution | RESOLVED |
| `hint(name, params*)` | Optimizer hint (e.g., broadcast) | RESOLVED |
| `withWatermark(eventTime, delay)` | Streaming watermark | RESOLVED |
| `withMetadata(col, metadata)` | Attach metadata to a column via `Alias.metadata` JSON string | RESOLVED |
| `writeStream` | Return a `DataStreamWriter` for streaming sinks | RESOLVED |
| `writeTo(table)` | Return `DataFrameWriterV2` for table-aware writes | RESOLVED |
| `mergeInto(table, condition)` | Return `MergeIntoWriter` | RESOLVED |
| `stat` | Return `DataFrameStatFunctions` (corr, cov, freqItems, crosstab, sampleBy) | RESOLVED |
| `apply(colName)` / `col(colName)` | Access column by name using `df("col")` syntax | RESOLVED |
| `collectAsList()` / `takeAsList(n)` | Java-friendly `java.util.List` collection methods | RESOLVED |

### MEDIUM PRIORITY

| API | Description | Status |
|-----|-------------|--------|
| `isStreaming` | Check if the plan involves streaming sources | RESOLVED |
| `intersectAll(other)` / `exceptAll(other)` | Set operations preserving duplicates | RESOLVED |
| `repartition(numPartitions, cols*)` | Hash repartition by columns | RESOLVED |
| `repartitionByRange(numPartitions, cols*)` | Range-based repartitioning with sort-order expressions | RESOLVED |
| `sortWithinPartitions(cols*)` | Sort within each partition | RESOLVED |
| `randomSplit(weights, seed)` | Split dataset into multiple datasets by weight | RESOLVED |
| `unpivot()` / `melt()` | Rotate columns to rows | RESOLVED |
| `transpose()` | Transpose rows and columns via `Transpose` proto | RESOLVED |
| `sameSemantics(other)` / `semanticHash()` | Semantic equivalence checking via `AnalyzePlan` RPC | RESOLVED |
| `dropDuplicatesWithinWatermark()` | Streaming dedup within watermark (3 overloads) via `Deduplicate.within_watermark` proto | RESOLVED |
| `groupingSets(sets, cols*)` | GROUPING SETS aggregation via `Aggregate.GroupingSets` proto | RESOLVED |
| `show(numRows, truncate, vertical)` | Vertical display mode using server-side `ShowString` proto | RESOLVED |
| `colRegex(colName)` | Select columns by regex via `UnresolvedRegex` proto | RESOLVED |
| `metadataColumn(colName)` | Access metadata columns via `UnresolvedAttribute.is_metadata_column` | RESOLVED |
| `to(schema)` | Reconcile DataFrame to a target schema | RESOLVED |
| `dtypes` | Return column names and types as (String, String) pairs | RESOLVED |
| `zipWithIndex()` | Add an index column via internal `distributed_sequence_id` function | RESOLVED |

### LOW PRIORITY / JAVA-INTEROP

| API | Description | Status |
|-----|-------------|--------|
| `as[U: Encoder]` | Typed dataset conversion | RESOLVED |
| `joinWith[U]` | Typed join returning `Dataset[(T, U)]` with `JoinDataType` proto support | RESOLVED |
| `map`, `flatMap`, `mapPartitions` | Typed transformations (with explicit Encoder parameter) | RESOLVED (Scala 3 `derives Encoder` covers Scala use case) |
| `reduce(func)` | Typed aggregation | RESOLVED |
| `groupByKey[K](func)` | Typed grouped dataset | RESOLVED |
| `lateralJoin` | Lateral join support (inner/left/cross) | RESOLVED |
| `select[U1](TypedColumn)` | Typed select with 1-5 arity overloads via `Encoders.tuple` | RESOLVED |
| `scalar()` / `exists()` | Correlated scalar/EXISTS subqueries as Column via `SubqueryExpression` + `WithRelations` | RESOLVED |
| `rdd` / `toJavaRDD` | Convert to RDD | N/A (not supported in Connect) |

### DEFERRED (Not Planned)

| API | Description | Reason |
|-----|-------------|--------|
| `isEmpty` optimization | `IsEmpty` AnalyzePlan RPC for efficient emptiness check | Implemented via `limit(1).collect().isEmpty` -- functionally correct; `IsEmpty` variant does not exist in current proto |
| `map`/`flatMap`/`mapPartitions`/`reduce` with explicit `Encoder` param | Java interop overloads | Scala 3 `derives Encoder` covers the Scala use case |

---

## 3. Column Gaps

### HIGH PRIORITY

| API | Description | Status |
|-----|-------------|--------|
| `when(condition, value)` / `otherwise(value)` | Chained case-when expressions (`when().when().otherwise()`) | RESOLVED |
| `bitwiseOR(other)` / `\|` | Bitwise OR | RESOLVED |
| `bitwiseAND(other)` / `&` | Bitwise AND | RESOLVED |
| `bitwiseXOR(other)` / `^` | Bitwise XOR | RESOLVED |
| `getItem(key)` | Extract element from array/map by key | RESOLVED |
| `getField(fieldName)` | Extract field from struct | RESOLVED |
| `withField(fieldName, col)` | Add/replace a field in a nested struct | RESOLVED |
| `dropFields(fieldNames*)` | Drop fields from a nested struct | RESOLVED |
| `apply(extraction)` | Generic extraction (calls getItem/getField) | RESOLVED |
| `eqNullSafe(other)` / `<=>` | Null-safe equality comparison | RESOLVED |
| `ilike(literal)` | Case-insensitive LIKE | RESOLVED |

### MEDIUM PRIORITY

| API | Description | Status |
|-----|-------------|--------|
| `cast(DataType)` | Cast using DataType object (not just String) | RESOLVED |
| `try_cast(to)` | Permissive cast returning null on failure | RESOLVED |
| `desc_nulls_first` / `desc_nulls_last` / `asc_nulls_first` / `asc_nulls_last` | Fine-grained null ordering | RESOLVED |
| `isInCollection(values)` | `isin` variant for Scala/Java collections | RESOLVED |
| `as(aliases: Seq[String])` | Multi-alias for column | RESOLVED |
| `as(alias, metadata)` | Alias with metadata | RESOLVED |
| `over()` (no-arg) | Window over default empty spec | RESOLVED |
| `outer()` | Mark column as a lateral column alias | RESOLVED |
| `transform(f)` | Apply a transformation function to this column | RESOLVED |
| `explain(extended)` | Debug-print the expression tree | RESOLVED |
| `equalTo`, `notEqual`, `gt`, `lt`, `leq`, `geq` | Java-friendly named comparison methods | RESOLVED |
| `or(other)` / `and(other)` | Java-friendly named logical methods | RESOLVED |

### LOW PRIORITY

| API | Description | Status |
|-----|-------------|--------|
| `as[U: Encoder]` (TypedColumn) | Typed column conversion | RESOLVED |
| `substr(Column, Column)` | Overload taking Column args instead of Int | RESOLVED |
| `isin(Dataset)` | IN subquery: `col IN (SELECT ...)` via `SubqueryExpression` + `WithRelations` | RESOLVED |

---

## 4. DataFrameReader Gaps

| API | Description | Status |
|-----|-------------|--------|
| `format(source)` | Set data source format | RESOLVED (existed from Phase 1) |
| `schema(schemaString)` / `schema(StructType)` | Set schema | RESOLVED |
| `option(key, value)` / `options(map)` | Set options | RESOLVED |
| `load()` / `load(path)` / `load(paths*)` | Load data | RESOLVED |
| `table(tableName)` | Read from table | RESOLVED |
| `json(path)` / `csv(path)` / `parquet(path)` / `orc(path)` / `text(path)` | Format-specific readers | RESOLVED |

No remaining gaps -- DataFrameReader was feature-complete from Phase 1.

---

## 5. DataFrameWriter Gaps

| API | Description | Status |
|-----|-------------|--------|
| `mode(saveMode)` | Set save mode (append/overwrite/errorIfExists/ignore) | RESOLVED (existed from Phase 1) |
| `format(source)` | Set data source format | RESOLVED |
| `option(key, value)` / `options(map)` | Set options | RESOLVED |
| `partitionBy(colNames*)` | Partition output | RESOLVED |
| `bucketBy(numBuckets, colName, colNames*)` | Bucket output | RESOLVED |
| `sortBy(colName, colNames*)` | Sort within buckets | RESOLVED |
| `save()` / `save(path)` | Save data | RESOLVED |
| `saveAsTable(tableName)` | Save to table | RESOLVED |
| `insertInto(tableName)` | Insert into table | RESOLVED |
| `json(path)` / `csv(path)` / `parquet(path)` / `orc(path)` / `text(path)` | Format-specific writers | RESOLVED |
| `DataFrameWriterV2` | Table-aware writes (create, replace, createOrReplace, append, overwrite, overwritePartitions) | RESOLVED |
| `MergeIntoWriter` | MERGE INTO for data lakehouse (whenMatched, whenNotMatched, whenNotMatchedBySource) | RESOLVED |

No remaining gaps.

---

## 6. DataStreamReader Gaps

| API | Description | Status |
|-----|-------------|--------|
| `format(source)` | Set streaming source format | RESOLVED |
| `schema(schemaString)` / `schema(StructType)` | Set schema for streaming source | RESOLVED |
| `option(key, value)` / `options(map)` | Set options | RESOLVED |
| `load()` / `load(path)` | Load streaming data (`Read { is_streaming=true }`) | RESOLVED |
| `table(tableName)` | Read streaming table | RESOLVED |

No remaining gaps -- DataStreamReader was implemented in Phase 4 (Streaming).

---

## 7. DataStreamWriter Gaps

| API | Description | Status |
|-----|-------------|--------|
| `outputMode(mode)` | Set output mode (append/complete/update) | RESOLVED |
| `trigger(trigger)` | Set trigger (ProcessingTime/AvailableNow/Once/Continuous) | RESOLVED |
| `queryName(name)` | Set query name | RESOLVED |
| `format(source)` | Set sink format | RESOLVED |
| `partitionBy(colNames*)` | Partition output | RESOLVED |
| `option(key, value)` / `options(map)` | Set options | RESOLVED |
| `start()` / `start(path)` | Start streaming query | RESOLVED |
| `toTable(tableName)` | Start streaming to table | RESOLVED |
| `foreachBatch(f)` | Custom micro-batch processing via `ForeachWriterPacket` | RESOLVED |
| `foreach(writer)` | Custom row-level streaming sink via `ForeachWriterPacket` | RESOLVED |

No remaining gaps.

---

## 8. KeyValueGroupedDataset Gaps

| API | Description | Status |
|-----|-------------|--------|
| `keys` | DataFrame of group keys | RESOLVED |
| `flatMapGroups(f)` | Apply function to each group | RESOLVED |
| `mapGroups(f)` | Map function over each group | RESOLVED |
| `mapValues(f)` | Map over values | RESOLVED |
| `reduceGroups(f)` | Reduce within each group via `ReduceAggregator` + `agg(TypedColumn)` | RESOLVED |
| `agg[U1](col1: TypedColumn)` through `agg[U1,U2,U3,U4]` | Typed aggregation via `Aggregate` proto (1-4 arity) | RESOLVED |
| `count()` | Count per group | RESOLVED |
| `cogroup(other)(f)` | Co-group two datasets | RESOLVED |
| `mapGroupsWithState(f)` | Stateful streaming processing (2 overloads: with/without initial state) | RESOLVED |
| `flatMapGroupsWithState(f)` | Stateful streaming processing (2 overloads: with/without initial state) | RESOLVED |
| `transformWithState(f)` | New streaming state API (3 overloads: basic, initial state, event time column) | RESOLVED |

No remaining gaps.

---

## Additional Resolved Components

| Component | Description | Status |
|-----------|-------------|--------|
| `Aggregator[IN, BUF, OUT]` + `Encoders` factory + `udaf()` | User-defined aggregate functions | RESOLVED |
| `TypedColumn[-T, U]` + `Aggregator.toColumn` | Typed column with `TypedAggregateExpression` proto | RESOLVED |
| `ReduceAggregator[T]` | Server-side `reduceGroups` via `agg(TypedColumn)` | RESOLVED |
| `typed` object | `typed.avg`, `typed.count`, `typed.sum`, `typed.sumLong` via typed aggregators | RESOLVED |
| `TableValuedFunction` | `SparkSession.tvf` with explode, inline, posexplode, json_tuple, stack, etc. | RESOLVED |
| 542 built-in functions | 100% coverage of the official API | RESOLVED |
| Full Catalog API | All 37 proto RPCs | RESOLVED |
| Error handling | RetryPolicy, GrpcRetryHandler, GrpcExceptionConverter | RESOLVED |
| `ExecutePlanResponseReattachableIterator` | Reattachable execution with automatic reconnect on transient gRPC failures | RESOLVED |
| `ResponseValidator` | Server-side session ID tracking and consistency validation | RESOLVED |
| `SessionCleaner` | GC-based cleanup of `CachedRemoteRelation` + `persist`/`unpersist`/`checkpoint` via AnalyzePlan | RESOLVED |
| `Observation` / `CollectMetrics` | `DataFrame.observe()` + `Observation` class | RESOLVED |
| `StreamingQueryListener` | `StreamingQueryListener` + `StreamingQueryListenerBus` + event dispatch | RESOLVED |
| `SQLImplicits` / `DatasetHolder` | Scala 3 `object implicits` with extension methods (`$"col"`, `Seq[T].toDS/toDF`) | RESOLVED |
| `ConnectRepl` | Ammonite-based Scala 3 REPL with `SparkConnectClientParser` + `AmmoniteClassFinder` | RESOLVED |
| `FetchErrorDetails` RPC | Enriched error details with exception chain, server stack traces, message parameters | RESOLVED |
| Plan Compression (ZSTD) | Server-config-driven ZSTD compression for large plans via `CompressedOperation` proto | RESOLVED |
| Scalar/Exists/IN Subqueries | `Dataset.scalar()`, `Dataset.exists()`, `Column.isin(Dataset)` via `SubqueryExpression` + `WithRelations` | RESOLVED |

---

## Implementation Phases (All Completed)

**Phase 1** (High Priority -- COMPLETED):
1. Parameterized `sql` with args
2. `joinWith` (typed join)
3. `toLocalIterator`
4. `newSession()`
5. `FetchErrorDetails` RPC

**Phase 2** (Medium Priority -- COMPLETED):
6. Operation tags + fine-grained interruption
7. `toJSON` / `show(vertical)`
8. `lateralJoin` / `groupingSets` / `repartitionByRange`
9. `SparkResult` extract / Plan compression (ZSTD)

**Phase 3** (API Completeness -- COMPLETED):
10. `cloneSession` / `range(numPartitions)` / `emptyDataset[T]`
11. Typed `select` with TypedColumn
12. `dropDuplicatesWithinWatermark`
13. `collectAsList` / `takeAsList`
14. `withMetadata` / `colRegex` / `metadataColumn`
15. `transpose` / `zipWithIndex`
16. Static session management
17. `executeCommand` (DeveloperApi)

**Phase 4** (Streaming -- COMPLETED):
- DataStreamReader, DataStreamWriter, StreamingQuery, StreamingQueryManager
- foreachBatch / foreach with ForeachWriterPacket
- mapGroupsWithState / flatMapGroupsWithState / transformWithState
- StreamingQueryListener + StreamingQueryListenerBus

**Phase 5** (Advanced Features -- COMPLETED):
- ExecutePlanResponseReattachableIterator
- ResponseValidator
- SessionCleaner
- TypedColumn / Aggregator / ReduceAggregator
- TableValuedFunction
- ConnectRepl

---

## Remaining Gaps (Deferred)

### `isEmpty` Optimization

**Upstream**: Uses `IsEmpty` AnalyzePlan RPC for efficient emptiness check.

**SC3 status**: Implemented via `limit(1).collect().isEmpty` -- functionally correct. The `IsEmpty` AnalyzePlan variant does not exist in the current proto definitions.

### Java API Overloads

**Upstream**: `map`/`flatMap`/`mapPartitions`/`reduce` with explicit `Encoder` parameter -- Java interop overloads.

**SC3 status**: Not implemented. Scala 3 `derives Encoder` covers the Scala use case; Java API overloads are not needed for a pure Scala 3 project.

### ConnectConversions / ColumnNodeToProtoConverter

**Upstream**: The official client uses a `ColumnNode` tree that is converted to proto via `ColumnNodeToProtoConverter`. SC3 uses a different (direct proto-building) approach for column expressions.

**SC3 status**: Functionally equivalent but architecturally different. No action needed unless upstream introduces ColumnNode-only features.

### `CustomSparkConnectBlockingStub` / `CustomSparkConnectStub`

**Upstream**: Custom gRPC stubs with session ID injection, dynamic retry policy updates, and stub state management via `SparkConnectStubState`.

**SC3 status**: Uses direct gRPC stub wrapping. Functionally equivalent for current feature set.

---

## Known Integration Test Bugs

The following bugs were discovered during integration testing and need to be fixed:

| # | Bug | Root Cause | Affected Tests | Priority |
|---|-----|-----------|----------------|----------|
| 1 | `=!=` sends function name `!=` to server | SC3 uses `fn("!=", other)` but upstream implements as `!(this === other)` (NOT wrapping equals) | `=!=`, `notEqual` in ColumnIntegrationSuite | FIXED |
| 2 | `saveAsTable` / `insertInto` fail with `TABLE_SAVE_METHOD_UNSPECIFIED(0)` | `SaveTable` proto enum value not being set in `DataFrameWriter` | saveAsTable, insertInto in ReadWriteIntegrationSuite | FIXED |
| 3 | `contains`, `startsWith`, `endsWith` fail | Root cause not yet investigated | String methods in ColumnIntegrationSuite | MEDIUM |
| 4 | `groupBy.pivot` with aggregation fails | Root cause not yet investigated | pivot in DataFrameIntegrationSuite | MEDIUM |
| 5 | `catalog.createTable` / `dropTable` incorrect behavior | `tableExists` returns false after `createTable` succeeds | CatalogIntegrationSuite | LOW |

### Bug Details

**Bug 1: `=!=` implementation**
- File: `src/main/scala/org/apache/spark/sql/Column.scala`, line 53
- Current: `def =!=(other: Column): Column = fn("!=", other)`
- Fix: `def =!=(other: Column): Column = !(this === other)`
- Same fix needed for `notEqual` (Java-friendly alias)

**Bug 2: `saveAsTable` / `insertInto` TABLE_SAVE_METHOD_UNSPECIFIED**
- File: `src/main/scala/org/apache/spark/sql/DataFrameWriter.scala`
- The `WriteOperation.SaveTable` enum must be explicitly set to `LOCAL_SAVE_TABLE` or `SAVE_TABLE` (not left as default 0)

**Bug 3-5**: Root cause investigation pending.

---

## Architecture Differences

SC3 intentionally diverges from upstream in several areas:

| Area | Upstream (Scala 2.13) | SC3 (Scala 3) |
|------|----------------------|---------------|
| Encoder derivation | Runtime reflection via `ScalaReflection` | Compile-time `derives Encoder` |
| Column expressions | `ColumnNode` tree -> `ColumnNodeToProtoConverter` | Direct proto construction |
| `AgnosticEncoder` | `EncoderField` with `writeReplace` + `EncoderSerializationProxy` | Same serialization format (cross-Scala compat) |
| gRPC stubs | `CustomSparkConnectBlockingStub` + `SparkConnectStubState` | Direct stub wrapping with `ResponseValidator` |
| Result handling | `SparkResult` (unified batches + metrics) | `ArrowDeserializer` (simpler, batch-focused) |
| Build system | Maven (multi-module) | SBT (single module) |
| Scala version | 2.13 | 3.3.7 LTS |

These differences are by design and do not represent gaps.
