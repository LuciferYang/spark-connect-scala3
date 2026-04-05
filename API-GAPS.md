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
| Operation Tags + Interruption | — | `addTag`/`removeTag`/`getTags`/`clearTags` + `interruptAll`/`interruptTag`/`interruptOperation` |
| `toJSON` | — | `DataFrame.toJSON` / `Dataset.toJSON` via `to_json(struct(*))` |
| `show(vertical)` | — | `show(numRows, truncate, vertical)` using server-side `ShowString` proto |
| `lateralJoin` | — | `DataFrame.lateralJoin` supporting inner/left/cross join types |
| `groupingSets` | — | `DataFrame.groupingSets` via `Aggregate.GroupingSets` proto |
| `repartitionByRange` | — | Range-based repartitioning with sort-order expressions |
| `SparkResult` extract | — | Unified `executeAndCollect` helper with observed metrics extraction |
| Plan Compression (ZSTD) | — | Server-config-driven ZSTD compression for large plans via `CompressedOperation` proto |
| `range(numPartitions)` | — | 4-parameter `range(start, end, step, numPartitions)` via `Range.num_partitions` proto |
| `emptyDataset[T]` | — | `SparkSession.emptyDataset[T: Encoder]` via `LocalRelation` with encoder schema |
| `collectAsList` / `takeAsList` | — | Java-friendly `java.util.List` collection methods on both `DataFrame` and `Dataset` |
| `isLocal` | — | `DataFrame.isLocal` via `AnalyzePlan.IsLocal` RPC |
| `dropDuplicatesWithinWatermark` | — | 3 overloads using `Deduplicate.within_watermark` proto field on `DataFrame` and `Dataset` |
| `transpose` | — | `DataFrame.transpose(indexColumn)` and `transpose()` via `Transpose` proto |
| `zipWithIndex` | — | `DataFrame.zipWithIndex` via internal `distributed_sequence_id` function |
| `colRegex` | — | `DataFrame.colRegex` / `Dataset.colRegex` via `UnresolvedRegex` proto |
| `metadataColumn` | — | `DataFrame.metadataColumn` / `Dataset.metadataColumn` via `UnresolvedAttribute.is_metadata_column` |
| `withMetadata` | — | `DataFrame.withMetadata(columnName, metadata)` via `Alias.metadata` JSON string |
| `cloneSession()` | — | `SparkSession.cloneSession()` via `CloneSession` RPC preserving config/views/UDFs |
| Typed `select` with TypedColumn | — | `Dataset.select(TypedColumn)` 1-5 arity overloads with `Encoders.tuple` |
| Static Session Management | — | `getActiveSession`/`getDefaultSession`/`active`/`setActiveSession`/`clearActiveSession`/`setDefaultSession`/`clearDefaultSession` |
| `executeCommand` (DeveloperApi) | — | `SparkSession.executeCommand(runner, command, options)` via `ExecuteExternalCommand` proto |
| `sameSemantics` / `semanticHash` | — | Semantic equivalence checking via `AnalyzePlan` RPC (implemented in earlier phase) |
| `inputFiles` | — | `DataFrame.inputFiles` via `AnalyzePlan` RPC (implemented in earlier phase) |
| `storageLevel` | — | `DataFrame.storageLevel` via `AnalyzePlan` RPC (implemented in earlier phase) |

## Remaining Gaps

### Deferred (Not Planned)

These gaps are intentionally deferred due to low priority or architectural reasons:

#### Scalar / Exists Subquery

**Upstream**: `Dataset.scalar()` and `Dataset.exists()` — correlated scalar and EXISTS subqueries as Column expressions.

**SC3 status**: Not implemented. Requires `WithRelations` architecture restructuring. High complexity, low priority.

#### `isEmpty` Optimization

**Upstream**: Uses `IsEmpty` AnalyzePlan RPC for efficient emptiness check.

**SC3 status**: Implemented via `limit(1).collect().isEmpty` — functionally correct. The `IsEmpty` AnalyzePlan variant does not exist in the current proto definitions.

#### Java API Overloads

**Upstream**: `map`/`flatMap`/`mapPartitions`/`reduce` with explicit `Encoder` parameter — Java interop overloads.

**SC3 status**: Not implemented. Scala 3 `derives Encoder` covers the Scala use case; Java API overloads are not needed for a pure Scala 3 project.

#### ConnectConversions / ColumnNodeToProtoConverter

**Upstream**: The official client uses a `ColumnNode` tree that is converted to proto via `ColumnNodeToProtoConverter`. SC3 uses a different (direct proto-building) approach for column expressions.

**SC3 status**: Functionally equivalent but architecturally different. No action needed unless upstream introduces ColumnNode-only features.

#### `CustomSparkConnectBlockingStub` / `CustomSparkConnectStub`

**Upstream**: Custom gRPC stubs with session ID injection, dynamic retry policy updates, and stub state management via `SparkConnectStubState`.

**SC3 status**: Uses direct gRPC stub wrapping. Functionally equivalent for current feature set.

## Implementation Phases

**Phase 1** (High Priority — ✅ COMPLETED):
1. ~~Parameterized `sql` with args~~
2. ~~`joinWith` (typed join)~~
3. ~~`toLocalIterator`~~
4. ~~`newSession()`~~
5. ~~`FetchErrorDetails` RPC~~

**Phase 2** (Medium Priority — ✅ COMPLETED):
6. ~~Operation tags + fine-grained interruption~~
7. ~~`toJSON` / `show(vertical)`~~
8. ~~`lateralJoin` / `groupingSets` / `repartitionByRange`~~
9. ~~`SparkResult` extract / Plan compression (ZSTD)~~

**Phase 3** (API Completeness — ✅ COMPLETED):
10. ~~`cloneSession` / `range(numPartitions)` / `emptyDataset[T]`~~
11. ~~Typed `select` with TypedColumn~~
12. ~~`dropDuplicatesWithinWatermark`~~
13. ~~`collectAsList` / `takeAsList`~~
14. ~~`withMetadata` / `colRegex` / `metadataColumn`~~
15. ~~`transpose` / `zipWithIndex`~~
16. ~~Static session management~~
17. ~~`executeCommand` (DeveloperApi)~~

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
