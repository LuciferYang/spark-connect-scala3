# Spark Connect Client for Scala 3

A lightweight [Apache Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) client written in **Scala 3**. It communicates with a Spark Connect server over gRPC, giving Scala 3 applications full access to Spark SQL without depending on the Spark runtime or its Scala 2.13 internals.

## Motivation

Apache Spark is built on Scala 2.13. A full cross-build to Scala 3 would touch hundreds of files and take 12–18 months of effort (see the [cross-build analysis](https://github.com/LuciferYang/spark-connect-scala3/issues/1) for details). Spark Connect changes the equation: the client communicates with the server purely through protobuf over gRPC, so it can be written in any language — including Scala 3 — with zero dependency on Spark internals.

This project provides that Scala 3 client.

## Features

- **SparkSession** — `builder().remote("sc://host:port").build()`, static session management (`getActiveSession`, `getDefaultSession`, `active`), `cloneSession()`, `executeCommand` (DeveloperApi)
- **DataFrame** — select, filter, groupBy, join, union, distinct, sort, limit, sample, and more
- **Dataset[T]** — typed operations with compile-time `Encoder` derivation via `derives Encoder`, `joinWith` (type-safe join), typed `select(TypedColumn)` (1-5 arity), `toLocalIterator`, `toJSON`
- **Column** — arithmetic, comparison, logical, string, cast, alias, window, sort operators
- **functions** — 542 built-in SQL functions (aggregates, math, string, date/time, window, collection, JSON, XML, URL, variant, datasketch, geospatial, and more) — **100% coverage** of the official API
- **GroupedDataFrame** — groupBy / rollup / cube / pivot / groupingSets with agg, count, sum, avg, min, max
- **DataFrameReader / Writer** — read and write Parquet, JSON, CSV, ORC, text, and tables
- **DataFrameWriterV2 / MergeIntoWriter** — V2 table writes (create, append, overwrite, overwritePartitions) and MERGE INTO support
- **DataStreamReader / Writer** — structured streaming read / write with trigger, `foreachBatch`, and `foreach` support
- **Stateful Streaming** — `mapGroupsWithState`, `flatMapGroupsWithState`, `transformWithState` on `KeyValueGroupedDataset`
- **StreamingQuery / Manager** — streaming query lifecycle management
- **Catalog** — full Catalog API: list/get/create/drop databases, tables, views, functions; cache management; table properties; partitions; analyze/truncate
- **UDF** — register and use JVM lambda UDFs (0–10 arguments)
- **UDAF** — user-defined aggregate functions via `Aggregator[IN, BUF, OUT]` with `Encoders` factory
- **TypedColumn / Aggregator.toColumn** — type-safe aggregation via `TypedColumn[-T, U]` and `Aggregator.toColumn`
- **ReduceAggregator** — server-side reduce aggregator for `reduceGroups`
- **typed object** — typed aggregation functions: `typed.avg`, `typed.count`, `typed.sum`, `typed.sumLong`
- **TableValuedFunction** — `SparkSession.tvf` for explode, inline, posexplode, json_tuple, stack, collations, sql_keywords, variant_explode, and more
- **KeyValueGroupedDataset.agg(TypedColumn)** — typed aggregation with 1–4 TypedColumn arguments via `Aggregate` proto
- **DataFrameNaFunctions** — drop / fill / replace null values
- **DataFrameStatFunctions** — statistical functions (crosstab, freqItems, approxQuantile, etc.)
- **Window** — window specifications with partitionBy, orderBy, rowsBetween, rangeBetween
- **Row / StructType** — typed accessors and schema support
- **Arrow IPC** — createDataFrame with client-side Arrow serialization; server responses deserialized via Arrow
- **RuntimeConfig** — get / set Spark configuration at runtime
- **Operation Tags** — `addTag`/`removeTag`/`getTags`/`clearTags` with fine-grained interruption (`interruptAll`/`interruptTag`/`interruptOperation`)
- **Plan Compression** — ZSTD compression for large plans, with server-config-driven threshold

## Compatibility

Tested against these Spark Connect server versions:

| Server Version | Status |
|---------------|--------|
| Spark 4.0.2 | Supported |
| Spark 4.1.1 | Supported |

## Requirements

| Component | Version |
|-----------|---------|
| JDK | 17+ |
| SBT | 1.10+ |
| Scala | 3.3.7 LTS |
| Spark Connect Server | 4.0.2+ |

## Quick Start

### 1. Start a Spark Connect server

```bash
# Spark 4.0+
$SPARK_HOME/sbin/start-connect-server.sh
```

### 2. Clone and build

```bash
git clone https://github.com/LuciferYang/spark-connect-scala3.git
cd spark-connect-scala3
build/sbt compile
```

### 3. Run the example

```bash
build/sbt "runMain org.apache.spark.sql.examples.quickStart"
```

Override the server URL:

```bash
SPARK_CONNECT_URL=sc://remote-host:15002 \
  build/sbt "runMain org.apache.spark.sql.examples.quickStart"
```

### 4. Launch the Scala 3 REPL

```bash
# Connect to default server (localhost:15002)
build/sbt run

# Connect to a specific server
build/sbt "run --remote sc://myhost:15002"

# Or use the SPARK_REMOTE environment variable
SPARK_REMOTE=sc://myhost:15002 build/sbt run
```

Once the REPL starts, `spark` is available as a pre-bound `SparkSession`:

```scala
scala> spark.sql("SELECT 1 + 1 AS result").show()
+------+
|result|
+------+
|     2|
+------+

scala> import org.apache.spark.sql.functions.*
scala> spark.range(10).select(col("id"), (col("id") * 2).as("doubled")).show()
```

### 5. Run unit tests

```bash
build/sbt test
```

### 6. Run integration tests (requires a running Spark Connect server)

```bash
build/sbt 'testOnly *IntegrationSuite'
```

### 7. Use in your own project

```scala
// build.sbt
libraryDependencies += "io.github.spark-connect" %% "spark-connect-scala3" % "0.1.0-SNAPSHOT"
```

```scala
import org.apache.spark.sql.{SparkSession, functions as F}

val spark = SparkSession.builder()
  .remote("sc://localhost:15002")
  .build()

spark.sql("SELECT * FROM my_table")
  .filter(F.col("age") > F.lit(18))
  .groupBy(F.col("city"))
  .count()
  .show()

spark.stop()
```

## Project Structure

```
src/
├── main/
│   ├── protobuf/spark/connect/         # Proto definitions (from Spark 4.0)
│   │   ├── base.proto
│   │   ├── relations.proto
│   │   ├── expressions.proto
│   │   ├── commands.proto
│   │   ├── types.proto
│   │   └── ...
│   └── scala/org/apache/spark/sql/
│       ├── SparkSession.scala           # Entry point + Builder
│       ├── DataFrame.scala              # Transformations + Actions
│       ├── Dataset.scala                # Typed Dataset[T]
│       ├── Column.scala                 # Expression tree builder
│       ├── TypedColumn.scala            # Column + Encoder for type-safe aggregation
│       ├── TableValuedFunction.scala    # Table-valued functions (explode, inline, etc.)
│       ├── functions.scala              # 542 built-in SQL functions (100% coverage)
│       ├── Row.scala                    # Row with typed accessors
│       ├── Encoder.scala                # Compile-time encoder derivation
│       ├── Encoders.scala               # Encoder factory (for UDAF bufferEncoder/outputEncoder)
│       ├── GroupedDataFrame.scala        # groupBy / rollup / cube / pivot
│       ├── DataFrameReader.scala        # Batch read
│       ├── DataFrameWriter.scala        # Batch write
│       ├── DataFrameWriterV2.scala      # V2 table writes (create/append/overwrite)
│       ├── MergeIntoWriter.scala        # MERGE INTO support
│       ├── DataStreamReader.scala       # Streaming read
│       ├── DataStreamWriter.scala       # Streaming write + Trigger + foreachBatch/foreach
│       ├── StreamingQuery.scala         # Query lifecycle management
│       ├── StreamingQueryManager.scala  # Active query manager
│       ├── ForeachWriter.scala          # Streaming foreach writer abstract class
│       ├── Catalog.scala                # Database/table/function catalog
│       ├── UserDefinedFunction.scala    # UDF + UDAF support
│       ├── UDFRegistration.scala        # UDF registration
│       ├── DataFrameNaFunctions.scala   # Null handling
│       ├── DataFrameStatFunctions.scala # Statistical functions
│       ├── StorageLevel.scala           # Cache storage levels
│       ├── ArrowSerializer.scala        # Row → Arrow IPC encoding
│       ├── KeyValueGroupedDataset.scala # Typed grouped + stateful streaming ops
│       ├── implicits.scala              # Implicit conversions
│       ├── SparkException.scala         # Spark exception hierarchy
│       ├── Artifact.scala               # Artifact management
│       ├── streaming/                   # Stateful streaming types
│       │   ├── OutputMode.scala         # Append / Update / Complete
│       │   ├── GroupStateTimeout.scala  # NoTimeout / ProcessingTime / EventTime
│       │   ├── TimeMode.scala           # None / ProcessingTime / EventTime
│       │   ├── GroupState.scala         # Managed state trait stub
│       │   ├── StatefulProcessor.scala  # StatefulProcessor + WithInitialState
│       │   ├── StatefulProcessorHandle.scala  # State handle trait stub
│       │   ├── StateVariables.scala     # ValueState / ListState / MapState stubs
│       │   ├── TimerValues.scala        # Timer values trait stub
│       │   ├── ExpiredTimerInfo.scala   # Expired timer info trait stub
│       │   ├── TTLConfig.scala          # TTL configuration
│       │   └── QueryInfo.scala          # Query info trait stub
│       ├── expressions/
│       │   ├── Aggregator.scala         # UDAF Aggregator abstract class
│       │   ├── ReduceAggregator.scala   # Server-side reduce aggregator
│       │   └── scalalang/
│       │       └── typed.scala          # Typed aggregation functions (avg, sum, etc.)
│       ├── internal/
│       │   └── TypedAggregators.scala   # TypedAverage, TypedCount, TypedSumDouble, TypedSumLong
│       ├── types/DataType.scala         # Spark SQL type system
│       ├── catalyst/encoders/
│       │   └── AgnosticEncoder.scala    # Agnostic encoder definitions
│       ├── connect/client/
│       │   ├── SparkConnectClient.scala    # gRPC client
│       │   ├── SparkConnectClientParser.scala # CLI argument parser for REPL
│       │   ├── AmmoniteClassFinder.scala    # Ammonite REPL class discovery
│       │   ├── ArrowDeserializer.scala     # Arrow IPC → Row decoding
│       │   ├── DataTypeProtoConverter.scala # Proto ↔ DataType
│       │   ├── ArtifactManager.scala       # Artifact upload/management
│       │   ├── RetryPolicy.scala           # Retry policy definitions
│       │   ├── GrpcRetryHandler.scala      # gRPC retry logic + RetryException
│       │   ├── GrpcExceptionConverter.scala # gRPC → Spark exceptions
│       │   ├── ResponseValidator.scala     # Server-side session ID tracking
│       │   └── ExecutePlanResponseReattachableIterator.scala # Reattachable execution
│       ├── connect/common/
│       │   ├── UdfPacket.scala             # UDF serialization
│       │   └── ForeachWriterPacket.scala   # ForeachWriter serialization
│       ├── connect/
│       │   └── SessionCleaner.scala        # GC-based CachedRemoteRelation cleanup
│       ├── application/
│       │   └── ConnectRepl.scala           # Ammonite-based Scala 3 REPL
│       └── examples/
│           └── QuickStart.scala         # End-to-end example
└── test/
    └── scala/org/apache/spark/sql/
        ├── ColumnSuite.scala
        ├── FunctionsSuite.scala
        ├── WindowSuite.scala
        ├── RowSuite.scala
        ├── EncoderSuite.scala
        ├── StorageLevelSuite.scala
        ├── DataStreamReaderSuite.scala
        ├── DataStreamWriterSuite.scala
        ├── DataStreamWriterForeachSuite.scala
        ├── StreamingQuerySuite.scala
        ├── StreamingQueryManagerSuite.scala
        ├── StreamingTypesSuite.scala
        ├── DataFrameStatFunctionsSuite.scala
        ├── DataFrameSuite.scala
        ├── DataFrameWriterV2Suite.scala
        ├── MergeIntoWriterSuite.scala
        ├── UserDefinedFunctionSuite.scala
        ├── CatalogSuite.scala
        ├── TypedOpsSuite.scala
        ├── SparkSessionSuite.scala
        ├── ExpandedEncoderSuite.scala
        ├── ImplicitsSuite.scala
        ├── KeyValueGroupedDatasetStatefulSuite.scala
        ├── IntegrationSuite.scala       # Requires running server
        ├── expressions/
        │   ├── AggregatorSuite.scala    # UDAF unit tests
        │   └── scalalang/
        │       └── TypedSuite.scala     # typed.avg/count/sum/sumLong tests
        ├── TypedColumnSuite.scala
        ├── TableValuedFunctionSuite.scala
        ├── connect/client/
        │   ├── SparkConnectClientParserSuite.scala
        │   ├── DataTypeProtoConverterSuite.scala
        │   ├── GrpcExceptionConverterSuite.scala
        │   ├── RetryPolicySuite.scala
        │   ├── ResponseValidatorSuite.scala
        │   ├── ReattachableIteratorSuite.scala
        │   └── PlanCompressionSuite.scala
        ├── connect/
        │   └── SessionCleanerSuite.scala
        └── types/
            └── DataTypeSuite.scala
        application/
            └── ConnectReplSuite.scala
```

## How It Works

```
┌──────────────────────┐          gRPC / Protobuf          ┌─────────────────────┐
│  Scala 3 Client      │ ──────────────────────────────▶   │  Spark Connect      │
│                      │                                    │  Server (4.0+)      │
│  SparkSession        │          Arrow IPC                 │                     │
│  DataFrame           │ ◀──────────────────────────────── │  Spark SQL Engine   │
│  Column / functions  │                                    │  Catalyst Optimizer │
└──────────────────────┘                                    └─────────────────────┘
```

1. **Transformations** (select, filter, join, ...) build a protobuf `Relation` tree on the client — no server calls.
2. **Actions** (collect, show, count, ...) serialize the tree into a `Plan` and send it to the server via `ExecutePlan` gRPC.
3. The server optimizes and executes the plan, streaming results back as **Arrow IPC** batches.
4. The client deserializes Arrow batches into `Row` objects.

## Key Dependencies

| Library | Purpose |
|---------|---------|
| [gRPC-Java](https://grpc.io/) | Transport layer for Spark Connect protocol |
| [Protobuf-Java](https://protobuf.dev/) | Java protobuf code generation for proto definitions |
| [Apache Arrow](https://arrow.apache.org/) | Data serialization/deserialization (IPC format) |
| [Ammonite](https://ammonite.io/) | Interactive Scala 3 REPL |
| [ScalaTest](https://www.scalatest.org/) | Unit and integration testing |

## Supported API

### SparkSession
`sql`, `sql(query, args)` (parameterized), `table`, `range` (2/3/4-param), `emptyDataFrame`, `emptyDataset[T]`, `createDataFrame`, `createDataset`, `read`, `readStream`, `streams`, `catalog`, `conf`, `udf`, `tvf`, `newSession`, `cloneSession`, `version`, `addTag`, `removeTag`, `getTags`, `clearTags`, `interruptAll`, `interruptTag`, `interruptOperation`, `executeCommand`, `stop`, `getActiveSession`, `getDefaultSession`, `active`, `setActiveSession`, `clearActiveSession`, `setDefaultSession`, `clearDefaultSession`

### DataFrame Transformations
`select`, `selectExpr`, `filter`, `where`, `limit`, `offset`, `sort`, `orderBy`, `groupBy`, `rollup`, `cube`, `agg`, `join`, `crossJoin`, `lateralJoin`, `groupingSets`, `withColumn`, `withColumnRenamed`, `withMetadata`, `drop`, `distinct`, `dropDuplicates`, `dropDuplicatesWithinWatermark`, `union`, `unionAll`, `unionByName`, `intersect`, `intersectAll`, `except`, `exceptAll`, `repartition`, `repartitionByRange`, `coalesce`, `sample`, `describe`, `summary`, `alias`, `toDF`, `hint`, `broadcast`, `sortWithinPartitions`, `tail`, `transform`, `transpose`, `zipWithIndex`, `colRegex`, `metadataColumn`, `na`, `stat`, `cache`, `persist`, `unpersist`, `checkpoint`, `localCheckpoint`, `withWatermark`, `writeStream`

### DataFrame Actions
`collect`, `collectAsList`, `count`, `first`, `head`, `take`, `takeAsList`, `show`, `show(vertical)`, `toJSON`, `printSchema`, `schema`, `columns`, `explain`, `isEmpty`, `isLocal`, `toLocalIterator`, `createTempView`, `createOrReplaceTempView`, `createGlobalTempView`, `write`

### Structured Streaming
`readStream` (DataStreamReader), `writeStream` (DataStreamWriter), `StreamingQuery` (isActive, stop, awaitTermination, recentProgress, explain, exception), `StreamingQueryManager` (active, get, awaitAnyTermination, resetTerminated), `Trigger` (ProcessingTime, AvailableNow, Once, Continuous), `foreachBatch`, `foreach` (ForeachWriter), `mapGroupsWithState`, `flatMapGroupsWithState`, `transformWithState`

### Column Operators
`===`, `=!=`, `>`, `>=`, `<`, `<=`, `&&`, `||`, `!`, `+`, `-`, `*`, `/`, `%`, `isNull`, `isNotNull`, `isNaN`, `contains`, `startsWith`, `endsWith`, `like`, `rlike`, `isin`, `between`, `substr`, `cast`, `alias`, `as`, `asc`, `desc`, `over`, `when`, `otherwise`, `getItem`, `getField`, `withField`, `dropFields`

### Catalog
`currentDatabase`, `setCurrentDatabase`, `currentCatalog`, `setCurrentCatalog`, `listDatabases`, `listTables`, `listColumns`, `listFunctions`, `listCatalogs`, `listCachedTables`, `listPartitions`, `listViews`, `getDatabase`, `getTable`, `getFunction`, `getTableProperties`, `getCreateTableString`, `databaseExists`, `tableExists`, `functionExists`, `isCached`, `cacheTable`, `uncacheTable`, `clearCache`, `createTable`, `createExternalTable`, `createDatabase`, `dropDatabase`, `dropTable`, `dropView`, `dropTempView`, `dropGlobalTempView`, `truncateTable`, `analyzeTable`, `refreshTable`, `refreshByPath`, `recoverPartitions`

### Functions
542 functions covering 100% of the official API: aggregates, math, string, date/time, null handling, conditional, collection, map, JSON, XML, URL, variant, regex, window, datasketch, geospatial, UDF, and UDAF — see [`functions.scala`](src/main/scala/org/apache/spark/sql/functions.scala) for the full list.

## Roadmap

- [x] SparkSession + gRPC client
- [x] DataFrame / Dataset[T] API
- [x] Column expressions + 542 built-in functions (100% coverage)
- [x] DataFrameReader / Writer
- [x] DataFrameWriterV2 / MergeIntoWriter
- [x] Catalog API (full coverage — all 37 proto RPCs)
- [x] Encoder derivation (Scala 3 `derives`)
- [x] UDF support
- [x] UDAF support (Aggregator + Encoders factory)
- [x] Aggregator.toColumn / TypedColumn (type-safe aggregation)
- [x] ReduceAggregator (server-side reduceGroups)
- [x] TableValuedFunction (SparkSession.tvf)
- [x] typed object (typed.avg, typed.sum, typed.count, typed.sumLong)
- [x] KeyValueGroupedDataset.agg(TypedColumn) (1–4 typed columns)
- [x] Structured Streaming
- [x] `foreachBatch` / `foreach` (ForeachWriter)
- [x] Stateful Streaming (`mapGroupsWithState` / `flatMapGroupsWithState` / `transformWithState`)
- [x] Window functions
- [x] Unit tests (533 tests)
- [x] Integration tests (Spark 4.0.2 / 4.1.1)
- [x] Error handling (retry policies, gRPC exception conversion, reattachable execution, enriched error details via FetchErrorDetails RPC)
- [x] Session management (ResponseValidator, SessionCleaner, checkpoint/localCheckpoint)
- [ ] Publish to Maven Central
- [x] ConnectRepl (Ammonite-based Scala 3 REPL)
- [x] Observation / CollectMetrics (`Dataset.observe()`)
- [x] StreamingQueryListener
- [x] SQLImplicits / DatasetHolder (`.toDS()`, `.toDF()` implicit conversions)
- [x] Parameterized SQL (`sql(query, args: Map)` + `sql(query, args: Column*)`)
- [x] `joinWith` (type-safe join returning `Dataset[(T, U)]`)
- [x] `toLocalIterator` (lazy streaming iteration on DataFrame and Dataset)
- [x] `newSession()` (independent session sharing same server endpoint)
- [x] FetchErrorDetails RPC (enriched error details with exception chain and server stack traces)
- [x] Operation Tags + Fine-grained Interruption (`addTag`/`removeTag`/`getTags`/`clearTags`/`interruptAll`/`interruptTag`/`interruptOperation`)
- [x] `toJSON` / `show(vertical)` (server-side ShowString proto)
- [x] `lateralJoin` / `groupingSets` / `repartitionByRange`
- [x] Plan Compression (ZSTD with server-config-driven threshold)
- [x] Phase 3 API Completeness: `cloneSession`, `range(numPartitions)`, `emptyDataset[T]`, typed `select(TypedColumn)` (1-5 arity), `dropDuplicatesWithinWatermark`, `collectAsList`/`takeAsList`, `withMetadata`, `colRegex`, `metadataColumn`, `transpose`, `zipWithIndex`, `isLocal`, static session management, `executeCommand`

See [API-GAPS.md](API-GAPS.md) for a detailed comparison with the official Spark Connect client.

## License

Apache License 2.0
