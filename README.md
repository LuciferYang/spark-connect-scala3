# Spark Connect Client for Scala 3

A lightweight [Apache Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) client written in **Scala 3**. It communicates with a Spark Connect server over gRPC, giving Scala 3 applications full access to Spark SQL without depending on the Spark runtime or its Scala 2.13 internals.

## Motivation

Apache Spark is built on Scala 2.13. A full cross-build to Scala 3 would touch hundreds of files and take 12–18 months of effort (see the [cross-build analysis](https://github.com/LuciferYang/spark-connect-scala3/issues/1) for details). Spark Connect changes the equation: the client communicates with the server purely through protobuf over gRPC, so it can be written in any language — including Scala 3 — with zero dependency on Spark internals.

This project provides that Scala 3 client.

## Features

- **SparkSession** — `builder().remote("sc://host:port").build()`
- **DataFrame** — select, filter, groupBy, join, union, distinct, sort, limit, sample, and more
- **Column** — arithmetic, comparison, logical, string, cast, alias, window, sort operators
- **functions** — 60+ built-in SQL functions (aggregates, math, string, date/time, window, collection)
- **GroupedDataFrame** — groupBy / rollup / cube / pivot with agg, count, sum, avg, min, max
- **DataFrameReader / Writer** — read and write Parquet, JSON, CSV, ORC, text, and tables
- **DataFrameNaFunctions** — drop / fill / replace null values
- **Row / StructType** — typed accessors and schema support
- **Arrow IPC** — createDataFrame with client-side Arrow serialization; server responses deserialized via Arrow
- **RuntimeConfig** — get / set Spark configuration at runtime

## Requirements

| Component | Version |
|-----------|---------|
| JDK | 17+ |
| SBT | 1.10+ |
| Scala | 3.3.7 LTS |
| Spark Connect Server | 4.0+ |

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
sbt compile
```

### 3. Run the example

```bash
sbt "runMain org.apache.spark.sql.examples.quickStart"
```

Override the server URL:

```bash
SPARK_CONNECT_URL=sc://remote-host:15002 \
  sbt "runMain org.apache.spark.sql.examples.quickStart"
```

Expected output:

```
[1] Spark version: 4.0.2

[2] SQL: SELECT 1 as one, 'hello' as greeting
+---+--------+
|one|greeting|
+---+--------+
|1  |hello   |
+---+--------+

[3] spark.range(10)
+--+
|id|
+--+
|0 |
|1 |
...

[5] range(100).groupBy(id % 5).agg(count, sum)
+-----+---+-----+
|group|cnt|total|
+-----+---+-----+
|0    |20 |950  |
|1    |20 |970  |
...

[6] createDataFrame with Arrow serialization
+-----+---+-----+
|name |age|score|
+-----+---+-----+
|Alice|30 |95.5 |
|Bob  |25 |88.0 |
|Carol|35 |92.3 |
+-----+---+-----+
Schema:
root
 |-- name: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- score: double (nullable = true)
```

### 4. Use in your own project

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
│   ├── protobuf/spark/connect/    # Proto definitions (from Spark 4.0)
│   │   ├── base.proto
│   │   ├── relations.proto
│   │   ├── expressions.proto
│   │   ├── commands.proto
│   │   ├── types.proto
│   │   └── ...
│   └── scala/org/apache/spark/sql/
│       ├── SparkSession.scala      # Entry point + Builder
│       ├── DataFrame.scala         # Transformations + Actions
│       ├── Column.scala            # Expression tree builder
│       ├── functions.scala         # Built-in SQL functions
│       ├── Row.scala               # Row with typed accessors
│       ├── GroupedDataFrame.scala   # groupBy / rollup / cube / pivot
│       ├── DataFrameReader.scala   # Read from external storage
│       ├── DataFrameWriter.scala   # Write to external storage
│       ├── DataFrameNaFunctions.scala  # Null handling
│       ├── ArrowSerializer.scala   # Row → Arrow IPC encoding
│       ├── types/DataType.scala    # Spark SQL type system
│       ├── connect/client/
│       │   ├── SparkConnectClient.scala    # gRPC client
│       │   ├── ArrowDeserializer.scala     # Arrow IPC → Row decoding
│       │   └── DataTypeProtoConverter.scala # Proto ↔ DataType
│       └── examples/
│           └── QuickStart.scala    # End-to-end example
└── test/                           # (coming soon)
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
| [ScalaPB](https://scalapb.github.io/) | Generates idiomatic Scala 3 case classes from `.proto` files |
| [gRPC-Java](https://grpc.io/) | Transport layer for Spark Connect protocol |
| [Apache Arrow](https://arrow.apache.org/) | Data serialization/deserialization (IPC format) |

## Supported API

### SparkSession
`sql`, `table`, `range`, `emptyDataFrame`, `createDataFrame`, `read`, `conf`, `version`, `stop`

### DataFrame Transformations
`select`, `selectExpr`, `filter`, `where`, `limit`, `offset`, `sort`, `orderBy`, `groupBy`, `rollup`, `cube`, `agg`, `join`, `crossJoin`, `withColumn`, `withColumnRenamed`, `drop`, `distinct`, `dropDuplicates`, `union`, `unionAll`, `unionByName`, `intersect`, `except`, `repartition`, `coalesce`, `sample`, `describe`, `summary`, `alias`, `na`

### DataFrame Actions
`collect`, `count`, `first`, `head`, `take`, `show`, `printSchema`, `schema`, `columns`, `explain`, `isEmpty`

### Column Operators
`===`, `=!=`, `>`, `>=`, `<`, `<=`, `&&`, `||`, `!`, `+`, `-`, `*`, `/`, `%`, `isNull`, `isNotNull`, `isNaN`, `contains`, `startsWith`, `endsWith`, `like`, `rlike`, `isin`, `between`, `substr`, `cast`, `alias`, `as`, `asc`, `desc`, `over`

### Functions
Aggregates, math, string, date/time, null handling, conditional, collection, window — see [`functions.scala`](src/main/scala/org/apache/spark/sql/functions.scala) for the full list.

## Roadmap

- [ ] Unit and integration tests
- [ ] Catalog API (listDatabases, listTables, ...)
- [ ] Streaming (structured streaming read/write)
- [ ] Encoder / Dataset[T] with Scala 3 type class derivation
- [ ] UDF registration
- [ ] Publish to Maven Central

## License

Apache License 2.0
