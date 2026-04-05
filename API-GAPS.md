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

## Remaining Gaps

### High Priority

#### 1. ConnectRepl — Interactive Scala 3 REPL

**Upstream**: `org.apache.spark.sql.connect.client.SparkConnectClientParser` + Ammonite-based REPL (`ConnectRepl.scala`)

**Challenge**: Upstream uses Ammonite which is Scala 2 only. SC3 would need to integrate with `scala-cli` or the Scala 3 REPL.

**Scope**: CLI argument parser (`--remote`, `--host`, `--port`, `--jars`, `--packages`), REPL initialization with SparkSession auto-creation, classpath/artifact handling.

#### 2. Observation / CollectMetrics

**Upstream**: `Dataset.observe(name, expr, exprs*)` + `Dataset.observe(observation, expr, exprs*)` + `Observation` class

**What it does**: Allows collecting metrics on a DataFrame during execution without additional passes. The `Observation` class registers a listener and provides `get`/`getOrDefault` to retrieve metric values after a batch query completes.

**Proto support**: `Relation.CollectMetrics` already exists in the proto definitions.

#### 3. StreamingQueryListener

**Upstream**: `StreamingQueryListenerBus` + `StreamingListenerPacket` serialization + server-side listener registration via `StreamingQueryListenerCommand` proto.

**What it does**: Registers listeners for query start/progress/terminated events. Requires Java serialization of the listener object + server-side registration + event streaming back to the client.

#### 4. SQLImplicits / DatasetHolder

**Upstream**: `SQLImplicits` provides `localSeqToDatasetHolder` and implicit conversions enabling `.toDS()` / `.toDF()` on local sequences.

**What it does**: `Seq(1, 2, 3).toDS()`, `Seq("a", "b").toDF("col")`. Requires `DatasetHolder[T]` wrapper class.

**SC3 status**: Partial support exists via `SparkSession.createDataset()`, but the implicit extension syntax is missing.

### Medium Priority

#### 5. Aggregator.toColumn / TypedColumn

**Upstream**: `Aggregator[IN, BUF, OUT].toColumn` returns a `TypedColumn[IN, OUT]` that can be used in typed aggregations on `KeyValueGroupedDataset`.

**What it does**: Enables `groupedDs.agg(myAggregator.toColumn)`. Requires `TypedColumn` class + `InvokeInlineUserDefinedFunction` column node.

**SC3 status**: `Aggregator` class exists; `toColumn` is not yet implemented.

#### 6. ReduceAggregator

**Upstream**: Built-in `ReduceAggregator[T]` for simple reduce operations, used internally by `KeyValueGroupedDataset.reduceGroups`.

**SC3 status**: `reduceGroups` uses a `mapGroups`-based client-side implementation, which works but doesn't delegate to the server's `ReduceAggregator`.

#### 7. TableValuedFunction (TVF)

**Upstream**: `SparkSession.tvf` provides access to table-valued functions: `range`, `explode`, `inline`, `json_tuple`, `posexplode`, `stack`, `collations`, `sql_keywords`, `variant_explode`.

**Proto support**: `Relation.TableValuedFunction` proto exists.

#### 8. `typed` Object

**Upstream**: `org.apache.spark.sql.typed` provides typed aggregation functions: `typed.count`, `typed.sum`, `typed.avg`, `typed.sumLong` etc.

**SC3 status**: Not implemented. Requires `TypedColumn` support.

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
