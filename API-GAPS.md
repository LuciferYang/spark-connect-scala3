# API Gap Analysis: SC3 vs Official Spark Connect Client

This document tracks remaining feature gaps between the Spark Connect Scala 3 client (SC3) and the official Spark Connect client (`sql/connect/common/src/main/scala/org/apache/spark/sql/connect/`).

Last updated: 2026-04-06

## Resolved Gaps

These features have been implemented:

| Feature | Commit | Notes |
|---------|--------|-------|
| `foreachBatch` / `foreach` | `fcf47fd` | `ForeachWriterPacket` serialization with correct `@SerialVersionUID` |
| `mapGroupsWithState` | `fcf47fd` | 2 overloads (with/without initial state) |
| `flatMapGroupsWithState` | `fcf47fd` | 2 overloads (with/without initial state) |
| `transformWithState` | `fcf47fd` | 3 overloads (basic, with initial state, with event time column) |
| `DataFrameWriterV2` | `01bf80b` | create, append, overwrite, overwritePartitions, replace, createOrReplace |
| `MergeIntoWriter` | `01bf80b` | whenMatched, whenNotMatched, whenNotMatchedBySource, merge |
| UDAF / Aggregator | `e9b146f` | `Aggregator[IN, BUF, OUT]` + `Encoders` factory + `udaf()` functions |
| 542 built-in functions | `b6776fa` | 100% coverage of the official API |
| Full Catalog API | `67efc18` | All 37 proto RPCs |
| Error handling | `1cf70b5` | RetryPolicy, GrpcRetryHandler, GrpcExceptionConverter |
| ExecutePlanResponseReattachableIterator | — | Reattachable execution with automatic reconnect on transient gRPC failures |
| ResponseValidator | — | Server-side session ID tracking and consistency validation |
| SessionCleaner | — | GC-based cleanup of `CachedRemoteRelation` + `persist`/`unpersist`/`checkpoint` via AnalyzePlan |
| Observation / CollectMetrics | — | `DataFrame.observe()` + `Observation` class |
| StreamingQueryListener | — | `StreamingQueryListener` + `StreamingQueryListenerBus` + event dispatch |
| SQLImplicits / DatasetHolder | — | Scala 3 `object implicits` with extension methods (`$"col"`, `Seq[T].toDS/toDF`) |
| ConnectRepl | — | Ammonite-based Scala 3 REPL with `SparkConnectClientParser` + `AmmoniteClassFinder` |
| Aggregator.toColumn / TypedColumn | — | `TypedColumn[-T, U]` + `Aggregator.toColumn` via `TypedAggregateExpression` proto |
| ReduceAggregator | — | `ReduceAggregator[T]` + server-side `reduceGroups` via `agg(TypedColumn)` |
| TableValuedFunction (TVF) | — | `SparkSession.tvf` with explode, inline, posexplode, json_tuple, stack, etc. |
| `typed` Object | — | `typed.avg`, `typed.count`, `typed.sum`, `typed.sumLong` via typed aggregators |
| `KeyValueGroupedDataset.agg(TypedColumn)` | — | `agg[U1]` through `agg[U1,U2,U3,U4]` via `Aggregate` proto |
| Parameterized SQL | — | `sql(query, args: Map)` + `sql(query, args: Column*)` with named/positional arguments |
| `joinWith` (Typed Join) | — | `Dataset.joinWith[U]` returning `Dataset[(T, U)]` with `JoinDataType` proto support |
| `toLocalIterator` | — | Lazy streaming `java.util.Iterator` on both `DataFrame` and `Dataset` |
| `newSession()` | — | Creates independent session sharing same server endpoint |
| `FetchErrorDetails` RPC | — | Enriched error details with exception chain, server stack traces, and message parameters |

## Remaining Gaps

### Medium Priority

#### 6. Operation Tags (`addTag` / `removeTag` / `getTags` / `clearTags`)

**Upstream**: Tag-based operation management on `SparkSession`. Tags propagate to all subsequent operations and can be used with `interruptTag()` for selective cancellation.

**SC3 status**: Not implemented. Only `interrupt()` (all operations) is available.

#### 7. Fine-grained Interruption (`interruptAll` / `interruptTag` / `interruptOperation`)

**Upstream**: `SparkSession.interruptAll()`, `interruptTag(tag)`, `interruptOperation(operationId)` — granular control over running operations.

**SC3 status**: Only `interruptAll` equivalent via `interrupt()`. No tag-based or operation-based interruption.

#### 8. `toJSON`

**Upstream**: `Dataset.toJSON: Dataset[String]` — converts each row to a JSON string.

**SC3 status**: Not implemented.

#### 9. `lateralJoin`

**Upstream**: `Dataset.lateralJoin(right: Dataset[_], condition: Column, joinType: String)` — LATERAL JOIN support for correlated subqueries.

**SC3 status**: Not implemented.

#### 10. `groupingSets`

**Upstream**: `Dataset.groupingSets(groupingSets: Seq[Seq[Column]], cols: Column*)` — GROUPING SETS aggregation.

**SC3 status**: Not implemented.

#### 11. `repartitionByRange`

**Upstream**: `Dataset.repartitionByRange(numPartitions: Int, partitionExprs: Column*)` — range-based repartitioning.

**SC3 status**: Not implemented. Only hash-based `repartition()` is available.

#### 12. Typed `select` with TypedColumn

**Upstream**: `Dataset.select(c1: TypedColumn[T, U1]): Dataset[U1]` through 5-arity overloads — type-safe column selection.

**SC3 status**: Not implemented. Only untyped `select(cols: Column*)` is available.

#### 13. `show` with Vertical Mode

**Upstream**: `Dataset.show(numRows: Int, truncate: Int, vertical: Boolean)` — vertical display mode for wide tables.

**SC3 status**: Only `show(numRows, truncate)` without vertical mode.

#### 14. `dropDuplicatesWithinWatermark`

**Upstream**: `Dataset.dropDuplicatesWithinWatermark(colNames: Seq[String])` — streaming deduplication within event-time watermark.

**SC3 status**: Not implemented.

#### 15. `SparkResult` Unified Result Handling

**Upstream**: `SparkResult` class manages Arrow batch decoding, metrics collection, and observed metrics in a unified way.

**SC3 status**: Uses simpler `ArrowDeserializer` — functionally equivalent but lacks metrics/observed-metrics extraction from `ExecutePlanResponse`.

#### 16. Plan Compression

**Upstream**: Large protobuf plans are compressed with LZ4 before transmission to reduce network overhead.

**SC3 status**: Not implemented. Plans are sent uncompressed.

#### 17. `cloneSession()`

**Upstream**: `SparkSession.cloneSession(): SparkSession` — clones session with all current configuration.

**SC3 status**: Not implemented.

#### 18. `range` with `numPartitions`

**Upstream**: `SparkSession.range(start, end, step, numPartitions)` — 4-parameter range with partition control.

**SC3 status**: Only `range(start, end)` and `range(start, end, step)` are implemented.

#### 19. `emptyDataset[T]`

**Upstream**: `SparkSession.emptyDataset[T: Encoder]: Dataset[T]` — creates an empty typed Dataset.

**SC3 status**: Only `emptyDataFrame` is implemented.

#### 20. `collectAsList` / `takeAsList`

**Upstream**: `Dataset.collectAsList(): java.util.List[T]` and `takeAsList(n): java.util.List[T]` — Java-friendly collection methods.

**SC3 status**: Not implemented. Only Scala `collect()` and `take()` are available.

#### 21. `withMetadata`

**Upstream**: `Dataset.withMetadata(columnName: String, metadata: Metadata): DataFrame` — attaches metadata to a column.

**SC3 status**: Not implemented.

#### 22. `colRegex`

**Upstream**: `Dataset.colRegex(colName: String): Column` — selects columns by regex pattern matching.

**SC3 status**: Not implemented.

### Low Priority

#### 23. Scalar / Exists Subquery

**Upstream**: `Dataset.scalar()` and `Dataset.exists()` — correlated scalar and EXISTS subqueries as Column expressions.

**SC3 status**: Not implemented.

#### 24. `transpose`

**Upstream**: `Dataset.transpose(indexColumn: Column): DataFrame` — row-to-column transposition.

**SC3 status**: Not implemented.

#### 25. `zipWithIndex`

**Upstream**: `Dataset.zipWithIndex: DataFrame` — adds a monotonically increasing index column.

**SC3 status**: Not implemented.

#### 26. `sameSemantics` / `semanticHash`

**Upstream**: `Dataset.sameSemantics(other: Dataset[_]): Boolean` and `semanticHash(): Int` — semantic equivalence checking.

**SC3 status**: Not implemented.

#### 27. `inputFiles`

**Upstream**: `Dataset.inputFiles: Array[String]` — returns the list of input files for the Dataset.

**SC3 status**: Not implemented.

#### 28. `storageLevel`

**Upstream**: `Dataset.storageLevel: StorageLevel` — queries the current cache storage level.

**SC3 status**: Not implemented.

#### 29. `isLocal` / `isStreaming`

**Upstream**: `Dataset.isLocal: Boolean` and `isStreaming: Boolean` — query execution locality and streaming status.

**SC3 status**: Not implemented.

#### 30. `isEmpty` Optimization

**Upstream**: Uses `IsEmpty` AnalyzePlan RPC for efficient emptiness check.

**SC3 status**: Implemented via `head(1).isEmpty` — functionally correct but less efficient.

#### 31. `metadataColumn`

**Upstream**: `Dataset.metadataColumn(colName: String): Column` — access metadata columns (e.g., `_metadata`).

**SC3 status**: Not implemented.

#### 32. Static Session Management

**Upstream**: `SparkSession.getActiveSession`, `getDefaultSession`, `active` — static/thread-local session management.

**SC3 status**: Not implemented. Sessions are managed explicitly.

#### 33. `executeCommand` (DeveloperApi)

**Upstream**: `SparkSession.executeCommand(runner: String, command: String, options: Map)` — DeveloperApi for direct proto command execution.

**SC3 status**: Not implemented.

#### 34. Java API Overloads

**Upstream**: `map`/`flatMap`/`mapPartitions`/`reduce` with explicit `Encoder` parameter — Java interop overloads.

**SC3 status**: Not implemented. Scala 3 `derives Encoder` covers the Scala use case; Java API overloads are lower priority.

#### 35. ConnectConversions / ColumnNodeToProtoConverter

**Upstream**: The official client uses a `ColumnNode` tree that is converted to proto via `ColumnNodeToProtoConverter`. SC3 uses a different (direct proto-building) approach for column expressions.

**SC3 status**: Functionally equivalent but architecturally different. No action needed unless upstream introduces ColumnNode-only features.

#### 36. `CustomSparkConnectBlockingStub` / `CustomSparkConnectStub`

**Upstream**: Custom gRPC stubs with session ID injection, dynamic retry policy updates, and stub state management via `SparkConnectStubState`.

**SC3 status**: Uses direct gRPC stub wrapping. Functionally equivalent for current feature set.

## Suggested Implementation Phases

**Phase 1** (High Priority — ✅ COMPLETED):
1. ~~Parameterized `sql` with args~~
2. ~~`joinWith` (typed join)~~
3. ~~`toLocalIterator`~~
4. ~~`newSession()`~~
5. ~~`FetchErrorDetails` RPC~~

**Phase 2** (Medium Priority — API completeness):
6. Operation tags + fine-grained interruption
7. `toJSON` / `show(vertical)`
8. `lateralJoin` / `groupingSets` / `repartitionByRange`
9. `SparkResult` / Plan compression
10. `cloneSession` / `range(numPartitions)` / `emptyDataset[T]`

**Phase 3** (Low Priority — can defer):
11. Subquery support (`scalar` / `exists`)
12. Utility APIs (`transpose`, `zipWithIndex`, `sameSemantics`, etc.)
13. Static session management
14. Java API overloads

## Architecture Differences

SC3 intentionally diverges from upstream in several areas:

| Area | Upstream (Scala 2.13) | SC3 (Scala 3) |
|------|----------------------|---------------|
| Encoder derivation | Runtime reflection via `ScalaReflection` | Compile-time `derives Encoder` |
| Column expressions | `ColumnNode` tree → `ColumnNodeToProtoConverter` | Direct proto construction |
| `AgnosticEncoder` | `EncoderField` with `writeReplace` + `EncoderSerializationProxy` | Same serialization format (cross-Scala compat) |
| gRPC stubs | `CustomSparkConnectBlockingStub` + `SparkConnectStubState` | Direct stub wrapping with `ResponseValidator` |
| Result handling | `SparkResult` (unified batches + metrics) | `ArrowDeserializer` (simpler, batch-focused) |
| Build system | Maven (multi-module) | SBT (single module) |
| Scala version | 2.13 | 3.3.7 LTS |

These differences are by design and do not represent gaps.
