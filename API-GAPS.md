# API Gap Analysis: SC3 vs Official Spark Connect Client

This document tracks remaining feature gaps between the Spark Connect Scala 3 client (SC3) and the official Spark Connect client (`sql/connect/common/src/main/scala/org/apache/spark/sql/connect/`).

Last updated: 2026-04-05

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
| Observation / CollectMetrics | — | `DataFrame.observe()` + `Observation` class |
| StreamingQueryListener | — | `StreamingQueryListener` + `StreamingQueryListenerBus` + event dispatch |
| SQLImplicits / DatasetHolder | — | Scala 3 `object implicits` with extension methods (`$"col"`, `Seq[T].toDS/toDF`) |
| ConnectRepl | — | Ammonite-based Scala 3 REPL with `SparkConnectClientParser` + `AmmoniteClassFinder` |
| Aggregator.toColumn / TypedColumn | — | `TypedColumn[-T, U]` + `Aggregator.toColumn` via `TypedAggregateExpression` proto |
| ReduceAggregator | — | `ReduceAggregator[T]` + server-side `reduceGroups` via `agg(TypedColumn)` |
| TableValuedFunction (TVF) | — | `SparkSession.tvf` with explode, inline, posexplode, json_tuple, stack, etc. |
| `typed` Object | — | `typed.avg`, `typed.count`, `typed.sum`, `typed.sumLong` via typed aggregators |
| `KeyValueGroupedDataset.agg(TypedColumn)` | — | `agg[U1]` through `agg[U1,U2,U3,U4]` via `Aggregate` proto |

## Remaining Gaps

### High Priority

(None currently)

### Medium Priority

(None currently)

### Low Priority

#### 9. ExecutePlanResponseReattachableIterator

**Upstream**: Retryable/reattachable execution with automatic reconnect on transient failures. The iterator reattaches to an in-progress query after connection loss.

**SC3 status**: Basic retry via `GrpcRetryHandler` exists, but no reattach support.

#### 10. ResponseValidator

**Upstream**: Validates server session ID tracking, server-side session liveness, and detects session mismatch.

**SC3 status**: Not implemented. Would improve robustness for long-running applications.

#### 11. SessionCleaner

**Upstream**: GC-based cleanup of `CachedRemoteRelation` references on the server when client-side objects are garbage collected.

**SC3 status**: Not implemented. `unpersist()` is supported for explicit cleanup.

#### 12. ConnectConversions / ColumnNodeToProtoConverter

**Upstream**: The official client uses a `ColumnNode` tree that is converted to proto via `ColumnNodeToProtoConverter`. SC3 uses a different (direct proto-building) approach for column expressions.

**SC3 status**: Functionally equivalent but architecturally different. No action needed unless upstream introduces ColumnNode-only features.

## Architecture Differences

SC3 intentionally diverges from upstream in several areas:

| Area | Upstream (Scala 2.13) | SC3 (Scala 3) |
|------|----------------------|---------------|
| Encoder derivation | Runtime reflection via `ScalaReflection` | Compile-time `derives Encoder` |
| Column expressions | `ColumnNode` tree → `ColumnNodeToProtoConverter` | Direct proto construction |
| `AgnosticEncoder` | `EncoderField` with `writeReplace` + `EncoderSerializationProxy` | Same serialization format (cross-Scala compat) |
| Build system | Maven (multi-module) | SBT (single module) |
| Scala version | 2.13 | 3.3.7 LTS |

These differences are by design and do not represent gaps.
