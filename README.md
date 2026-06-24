# Spark Connect Client for Scala 3

A lightweight [Apache Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) client written in **Scala 3**. It communicates with a Spark Connect server over gRPC, giving Scala 3 applications full access to Spark SQL without depending on the Spark runtime or its Scala 2.13 internals.

## Motivation

Apache Spark is built on Scala 2.13, and a full cross-build to Scala 3 was estimated at 12–18 months (see the [cross-build analysis](https://github.com/LuciferYang/spark-connect-scala3/issues/1)). Spark Connect changes the equation: the client talks to the server purely through protobuf over gRPC, so it can be written in any language — including Scala 3 — with zero dependency on Spark internals. This project provides that client.

## Features

- **Core** — SparkSession, DataFrame, Dataset[T], Column, Row with compile-time Encoder derivation
- **SQL Functions** — 542 built-in functions (100% of the Spark 4.1 API)
- **I/O** — Reader/Writer for Parquet, JSON, CSV, ORC, JDBC; V2 writes; MergeInto; streaming
- **Typed Operations** — TypedColumn, Aggregator, UDF/UDAF (0–10 args), KeyValueGroupedDataset
- **Streaming** — Structured Streaming with stateful operations (`transformWithState`, `flatMapGroupsWithState`), listener bus
- **Advanced** — Catalog, Window, Subqueries, TableValuedFunction, Plan Compression (ZSTD), Operation Tags
- **Interactive** — Ammonite-based REPL with `spark` session pre-bound

## Compatibility

| Server Version | Status |
|---------------|--------|
| Spark 4.1.x | Supported |
| Spark 4.0.x | **Not supported** — incompatible `AgnosticEncoders` serialVersionUID, missing `SubqueryExpression` / `CloneSession` proto/RPC |

## Requirements

| Component | Version |
|-----------|---------|
| JDK | 17+ |
| SBT | 1.10+ |
| Scala | 3.3.7 LTS |
| Spark Connect Server | 4.1.x |

## Quick Start

Start a Spark Connect server, then build the client:

```bash
$SPARK_HOME/sbin/start-connect-server.sh   # Spark 4.1+

git clone https://github.com/LuciferYang/spark-connect-scala3.git
cd spark-connect-scala3
build/sbt compile
```

```scala
import org.apache.spark.sql.{SparkSession, Row, functions as F}
import org.apache.spark.sql.types.*

val spark = SparkSession.builder()
  .remote("sc://localhost:15002")  // or set SPARK_REMOTE
  .build()

// SQL + transformations
spark.sql("SELECT 1 AS one, 'hello' AS greeting").show()

spark.range(100)
  .withColumn("group", F.col("id") % F.lit(5))
  .groupBy(F.col("group"))
  .agg(F.count(F.col("id")).as("cnt"), F.sum(F.col("id")).as("total"))
  .orderBy(F.col("group"))
  .show()

// createDataFrame with an explicit schema (Arrow serialization)
val schema = StructType(Seq(
  StructField("name", StringType),
  StructField("age", IntegerType)
))
spark.createDataFrame(Seq(Row("Alice", 30), Row("Bob", 25)), schema).show()

spark.stop()
```

### Build, test, and run

```bash
build/sbt run                         # launch the Ammonite REPL (spark pre-bound)
build/sbt "run --remote sc://host:15002"   # or set SPARK_REMOTE=sc://host:15002
build/sbt test                        # unit tests
build/sbt 'set Test / testOptions := Seq()' 'testOnly *IntegrationSuite'   # integration tests (live server)
```

Integration tests that send Scala 3 lambdas to a Scala 2.13 server are **canceled** (not failed) — see [Known Limitations](#known-limitations).

### Use as a dependency

```scala
// build.sbt
libraryDependencies += "io.github.luciferyang" %% "spark-connect-spark41" % "0.7.0"
```

```xml
<!-- Maven -->
<dependency>
  <groupId>io.github.luciferyang</groupId>
  <artifactId>spark-connect-spark41_3</artifactId>
  <version>0.7.0</version>
</dependency>
```

## How It Works

```
┌──────────────────────┐          gRPC / Protobuf          ┌─────────────────────┐
│  Scala 3 Client      │ ──────────────────────────────▶   │  Spark Connect      │
│  SparkSession        │                                    │  Server (4.1+)      │
│  DataFrame           │          Arrow IPC                 │  Spark SQL Engine   │
│  Column / functions  │ ◀──────────────────────────────── │  Catalyst Optimizer │
└──────────────────────┘                                    └─────────────────────┘
```

1. **Transformations** (select, filter, join, …) build a protobuf `Relation` tree on the client — no server calls.
2. **Actions** (collect, show, count, …) serialize the tree into a `Plan` and send it via `ExecutePlan` gRPC.
3. The server optimizes and executes the plan, streaming results back as **Arrow IPC** batches.
4. The client deserializes Arrow batches into `Row` objects.

## Project Structure

```
src/
├── main/
│   ├── protobuf/                   # Spark Connect proto (from the spark-upstream submodule)
│   ├── java/                       # UDF interfaces, Java function shims, serialization proxies
│   └── scala/org/apache/spark/sql/
│       ├── *.scala                 # Public API: SparkSession, DataFrame, Dataset, Column, functions, Row, …
│       ├── connect/client/         # gRPC client, Arrow (de)serialization, retries, reattachable execution
│       ├── connect/common/         # UDF packets, closure cleaning, lambda adaptors
│       ├── catalyst/encoders/      # AgnosticEncoder model
│       ├── streaming/              # Structured Streaming types
│       ├── expressions/            # Aggregator / typed aggregation
│       ├── types/                  # SQL type system
│       └── application/            # Ammonite-based REPL
└── test/                           # Unit suites + @IntegrationTest suites (require a live server)
```

## Key Dependencies

| Library | Purpose |
|---------|---------|
| [gRPC-Java](https://grpc.io/) | Transport for the Spark Connect protocol |
| [Protobuf-Java](https://protobuf.dev/) | Proto code generation |
| [Apache Arrow](https://arrow.apache.org/) | Data serialization (IPC format) |
| [Ammonite](https://ammonite.io/) | Interactive Scala 3 REPL |
| [ScalaTest](https://www.scalatest.org/) | Unit and integration testing |

## Supported API

| Area | Coverage |
|------|----------|
| **SparkSession** | `sql` (incl. parameterized), `table`, `range`, `createDataFrame`/`createDataset`, `read`/`readStream`, `catalog`, `conf`, `udf`, `tvf`, session lifecycle (`newSession`, `cloneSession`, active/default), tags & interruption, `executeCommand` |
| **DataFrame** | Full transformation surface (`select`, `filter`, `join`, `groupBy`/`rollup`/`cube`, `agg`, `window`, `union`/`intersect`/`except`, `repartition`, …) and actions (`collect`, `show`, `count`, `head`/`take`, `toJSON`, `write`, `createTempView`, …) |
| **Dataset[T]** | Typed `map`/`flatMap`/`filter`/`reduce`, `groupByKey`, `joinWith`, `select(TypedColumn)` — subject to the [lambda limitation](#known-limitations) |
| **Column** | Full operator and expression surface (comparison, boolean, arithmetic, `isin`, `between`, `cast`, `when`/`otherwise`, `over`, struct ops, …) |
| **Catalog** | Full coverage — all 37 proto RPCs (databases, tables, functions, caching, partitions) |
| **Functions** | 542 built-ins, 100% of the Spark 4.1 API — see [`functions.scala`](src/main/scala/org/apache/spark/sql/functions.scala) |
| **Streaming** | `readStream`/`writeStream`, triggers, `foreachBatch`/`foreach`, stateful ops (`mapGroupsWithState`, `flatMapGroupsWithState`, `transformWithState`), listeners |

## Known Limitations

### Typed lambdas referencing user case-class fields

Spark 4.0/4.1 servers run Scala 2.13 and cannot link lambda bytecode emitted by Scala 3 when it directly references a user-defined case-class field (e.g. `_.name` on a `Dataset[Person]`). The error surfaces as `INTERNAL_ERROR: Failed to unpack scala udf`.

| | Operations |
|---|---|
| **Affected** (input is a user case class) | `Dataset` typed transforms (`map`, `flatMap`, `filter`, `reduce`, `foreach`, …); `KeyValueGroupedDataset.{reduceGroups, mapValues, agg(TypedColumn)}` with user lambdas; streaming `foreachBatch` |
| **Unaffected** | Primitive-typed `Dataset` transforms; adaptor-wrapped grouping (`groupByKey`, `mapGroups`, `cogroup`, …); all DataFrame/Column APIs; `spark.sql(...)`; UDF/UDAF (serialized via Java `ObjectOutputStream`); streaming lifecycle; all catalog ops |

**Workaround:** prefer Column-expression APIs and column-level UDFs — e.g. replace `ds.filter(_.age > 28)` with `df.filter(col("age") > 28)`. The ~5 affected integration tests are `cancel`ed (not failed) and will pass once a Scala 3-native server exists.

### Server hang on `interruptOperation` with a non-existent operation id

The Spark 4.1.x server hangs indefinitely on an `InterruptRequest` with `INTERRUPT_TYPE_OPERATION_ID` for an unknown id. Only call `spark.interruptOperation(id)` with ids you have actually observed from the server; `interruptAll()` and `interruptTag(tag)` are unaffected.

## Memory & Resource Limits

The Arrow allocators in [`ArrowSerializer`](src/main/scala/org/apache/spark/sql/ArrowSerializer.scala) and [`ArrowDeserializer`](src/main/scala/org/apache/spark/sql/connect/client/ArrowDeserializer.scala) share a reservation cap that defaults to 256 GB. The cap is a ceiling on what the allocator may request, not a commitment — Arrow buffers are off-heap, so `-Xmx` and the container memory limit still gate real usage, and a large or malformed batch can OOM a small container well before the cap.

Override the cap with the `spark.connect.scala3.arrow.maxAllocatorBytes` system property (a positive byte count), e.g. `-Dspark.connect.scala3.arrow.maxAllocatorBytes=2147483648` for a 2 GB ceiling in a constrained container.

## Observability

Attach a gRPC [`ClientInterceptor`](https://grpc.github.io/grpc-java/javadoc/io/grpc/ClientInterceptor.html) to instrument every RPC — for metrics, distributed tracing (OpenTelemetry/Micrometer), or custom headers:

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .remote("sc://localhost:15002")
  .interceptor(myClientInterceptor)  // io.grpc.ClientInterceptor; call multiple times to add more
  .build()
```

Interceptors are applied at channel creation. When a connection token is supplied, the auth-header interceptor is added last so it runs closest to the wire.

The client also emits its own diagnostic warnings (retry/release failures, the streaming listener handler) to `System.err` by default. Route them into your logging system — or silence them — with `ClientLogging`:

```scala
import org.apache.spark.sql.connect.client.ClientLogging

ClientLogging.setHandler(line => logger.warn(line))
```

## Status

The client implements the full Spark Connect 4.1 API surface: SparkSession, DataFrame/Dataset[T], 542 functions (100% coverage), readers/writers (incl. V2 + MergeInto), the full Catalog (all 37 proto RPCs), Structured Streaming with stateful operations, subqueries, plan compression, operation tags, and an Ammonite REPL. It is covered by ~2,374 unit tests and 454 integration tests against Spark 4.1.2, and is published to Maven Central.

## License

Apache License 2.0
