# API Gaps: SC3 vs Official Spark Connect Client

Feature gaps between the Spark Connect Scala 3 client (SC3) and the official Spark Connect client (`sql/connect/common/src/main/scala/org/apache/spark/sql/connect/`).

## Status

All API gaps have been closed. SC3 covers:

- 542 built-in functions (100%)
- Full Catalog API (all 37 proto RPCs + `catalogExists`)
- Full Row typed getters (`getDecimal`, `getDate`, `getTimestamp`, `getInstant`, `getLocalDate`, `getSeq`, `getList`, `getMap`, `getJavaMap`, `getStruct`, `getAs(fieldName)`, `fieldIndex`, `anyNull`, `json`, `prettyJson`, `copy`)
- `Encoders.row` for Row encoder
- `SparkSession.time`, `SparkSession.Builder.config(Boolean/Long/Double)` overloads
- `UDFRegistration.register` Function0–10 inline overloads
- Full streaming surface (readStream/writeStream, foreachBatch/foreach, mapGroupsWithState, flatMapGroupsWithState, transformWithState, StreamingQueryListener)
- Typed API: `Aggregator`, `TypedColumn`, `ReduceAggregator`, typed `select`, `joinWith`
- Subqueries: `Dataset.scalar()`, `Dataset.exists()`, `Column.isin(Dataset)` via `SubqueryExpression` + `WithRelations`
- Reattachable execution, session cleaner, plan compression (ZSTD), `FetchErrorDetails` RPC
- Ammonite-based Scala 3 REPL with `AmmoniteClassFinder`

---

## Deferred (Not Planned)

### `isEmpty` AnalyzePlan RPC

**Upstream**: Uses an `IsEmpty` AnalyzePlan variant for efficient emptiness check.
**SC3**: Implemented via `limit(1).collect().isEmpty` — functionally correct. The `IsEmpty` AnalyzePlan variant does not exist in the current proto definitions.

### Java API overloads

**Upstream**: `map` / `flatMap` / `mapPartitions` / `reduce` with explicit `Encoder` parameter for Java interop.
**SC3**: Scala 3 `derives Encoder` covers the Scala use case; Java overloads are out of scope for a pure Scala 3 project.

### RDD APIs (`rdd`, `toJavaRDD`)

Not supported in Spark Connect.

---

## Known Limitation: Scala 3 Lambda Serialization

Spark 4.0/4.1 servers are built with Scala 2.13. Any SC3 API that ships a JVM lambda to the server (`Dataset.map/flatMap/filter(typed)/foreach/reduce`, `KeyValueGroupedDataset.mapGroups/flatMapGroups/reduceGroups/mapValues/cogroup`, `foreachBatch`) fails at deserialization because Scala 3 and Scala 2.13 use incompatible lambda serialization — the server cannot find `$deserializeLambda$`.

**Workaround**: prefer Column-expression APIs (`df.filter(col("age") > 28)` instead of `ds.filter(_.age > 28)`).

**Test handling**: lambda-based integration tests are wrapped in `withLambdaCompat` and gracefully `cancel` rather than fail.

See [README.md § Known Limitations](README.md#known-limitations) for the full list of affected/unaffected operations.

---

## Architecture Differences

SC3 intentionally diverges from upstream in several areas. These are by design, not gaps.

| Area | Upstream (Scala 2.13) | SC3 (Scala 3) |
|------|----------------------|---------------|
| Encoder derivation | Runtime reflection via `ScalaReflection` | Compile-time `derives Encoder` |
| Column expressions | `ColumnNode` tree → `ColumnNodeToProtoConverter` | Direct proto construction |
| `AgnosticEncoder` | `EncoderField` with `writeReplace` + `EncoderSerializationProxy` | Same serialization format (cross-Scala compatible) |
| gRPC stubs | `CustomSparkConnectBlockingStub` + `SparkConnectStubState` | Direct stub wrapping with `ResponseValidator` |
| Result handling | `SparkResult` (unified batches + metrics) | `ArrowDeserializer` (batch-focused) |
| REPL class wrapper | `ExtendedCodeClassWrapper` (registers `OuterScopes`) | Ammonite `CodeClassWrapper` |
| Embedded server | Supports `withLocalConnectServer` | Pure client only |
| Build system | Maven (multi-module) | SBT (single module) |
| Scala version | 2.13 | 3.3.7 LTS |
