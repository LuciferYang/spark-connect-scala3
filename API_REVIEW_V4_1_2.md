# Spark Connect Scala 3 API Review Against Upstream Spark Connect v4.1.2

Review date: 2026-06-01
Last updated: 2026-06-04

Reference project:

- `/Users/yangjie01/SourceCode/git/spark-mine-sbt`
- Branch/tag checked by user: `v4.1.2`
- Observed commit: `f0bb2e6a47d`

Current project:

- `/Users/yangjie01/SourceCode/git/spark-connect-scala3`
- Observed branch: `main`
- Observed commit: `965c6ef`

## Scope

This review compares the current project against upstream Spark Connect user API in Spark v4.1.2.
The practical reference is the public API that upstream Spark exposes for Connect clients through
the shared SQL API layer plus Connect-specific entry points. This is not a full Spark classic API
parity review.

The relevant public packages are mainly:

- `org.apache.spark.sql`
- `org.apache.spark.sql.streaming`
- `org.apache.spark.sql.catalog`
- `org.apache.spark.sql.types`
- `org.apache.spark.sql.api.java`
- `org.apache.spark.sql.avro`
- `org.apache.spark.sql.protobuf`

Classic-only internals such as `SparkContext`, RDD execution internals, SQL engine internals, catalyst internals, and server-side implementation APIs are not counted as required gaps for this Scala 3 Spark Connect client.

Shared SQL API classes are counted only when they are visible and useful to upstream Spark Connect
users. APIs that only make sense for classic execution are called out as lower priority or excluded.

## Conclusion

The previously identified high-priority Spark Connect user API gaps against Spark v4.1.2 are now
fixed on `main` through commit `965c6ef`.

Completed high-priority items:

1. `org.apache.spark.sql.streaming.Trigger` compatibility.
2. Connect-compatible `SparkSession` convenience APIs such as `spark.implicits` and Builder config facades.
3. `Metadata`, `MetadataBuilder`, Java `DataTypes`, and `RowFactory`.
4. `Column.as(..., Metadata)` and related metadata-compatible APIs.
5. Java UDF arity `UDF10` through `UDF22` and matching UDF overloads.
6. `RelationalGroupedDataset` public type compatibility.
7. Avro and Protobuf function packages.

The remaining work is lower-priority compatibility shim work. These items are still useful for
source compatibility with upstream examples and libraries, but they are not currently blocking the
core Spark Connect API surface. Work has started on this layer; `org.apache.spark.sql.expressions`
Window package compatibility, Java streaming mode factories, `javalang.typed`, connector catalog
Java value classes, encoder helper shims, `DatasetHolder`, `SQLImplicits`, and `SQLContext` are now
implemented in the current working tree.

## Findings

### 1. Streaming Trigger API Is In The Wrong Public Package

Status: fixed.

Current project defines `Trigger` in root package `org.apache.spark.sql`, inside:

- `src/main/scala/org/apache/spark/sql/DataStreamWriter.scala`

Spark v4.1.2 exposes standard trigger APIs in:

- `org.apache.spark.sql.streaming.Trigger`
- `sql/api/src/main/java/org/apache/spark/sql/streaming/Trigger.java`

Spark v4.1.2 includes overloads for:

- `ProcessingTime(long)`
- `ProcessingTime(long, TimeUnit)`
- `ProcessingTime(Duration)`
- `ProcessingTime(String)`
- `Once()`
- `AvailableNow()`
- `Continuous(...)`
- `RealTime(...)`

Current project only has:

- `ProcessingTime(intervalMs: Long)`
- `AvailableNow`
- `Once`
- `Continuous(intervalMs: Long)`

Impact:

Standard code such as this will not compile cleanly:

```scala
import org.apache.spark.sql.streaming.Trigger

df.writeStream
  .trigger(Trigger.ProcessingTime("10 seconds"))
  .start()
```

Recommendation:

Add `org.apache.spark.sql.streaming.Trigger` as the standard public API and update `DataStreamWriter.trigger` to accept it. Keep root-package `org.apache.spark.sql.Trigger` only as a backward-compatible alias if needed.

### 2. SparkSession Connect Compatibility Entry Points Are Still Missing

Status: fixed.

Current project already has important Connect APIs such as:

- `SparkSession.conf`
- `SparkSession.streams`
- `SparkSession.udf`
- `SparkSession.catalog`
- `SparkSession.builder().remote(...)`
- `SparkSession.builder().config(...)`

Remaining v4.1.2 user-facing gaps include:

- `spark.implicits`
- `SparkSession.builder().appName(...)`
- `SparkSession.builder().master(...)`, mainly as a compatibility/config facade
- `SparkSession.builder().enableHiveSupport()`, mainly as a compatibility/config facade
- potentially `withActive(...)` compatibility

Impact:

Official Spark examples and migration code commonly use:

```scala
val spark = SparkSession.builder()
  .appName("example")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._
```

For this Connect client, `appName`, `master`, and `enableHiveSupport` should be treated as
Connect-compatible source facades, not as a requirement to implement classic local execution or
Hive support.

Recommendation:

Add lightweight compatibility methods. For `spark.implicits`, expose the existing Scala 3 implicits
through a session-bound object where practical. Do not add classic execution behavior behind these
methods.

### 3. Types API Is Missing Metadata, MetadataBuilder, DataTypes, And RowFactory

Status: fixed.

Current project defines `StructField` as:

```scala
final case class StructField(name: String, dataType: DataType, nullable: Boolean = true)
```

Spark v4.1.2 defines:

```scala
case class StructField(
    name: String,
    dataType: DataType,
    nullable: Boolean = true,
    metadata: Metadata = Metadata.empty)
```

Missing or incomplete user-facing APIs include:

- `org.apache.spark.sql.types.Metadata`
- `org.apache.spark.sql.types.MetadataBuilder`
- Java `org.apache.spark.sql.types.DataTypes`
- Java `org.apache.spark.sql.RowFactory`
- richer `DataType` helpers such as JSON / DDL parsing and rendering where feasible

Impact:

Schema metadata is a core Spark SQL API. Without it, users cannot write standard code that attaches or reads column comments and metadata. Java users also lose the standard schema factory APIs.

Recommendation:

Add `Metadata` and `MetadataBuilder` first, extend `StructField`, then add Java facade classes `DataTypes` and `RowFactory`.

### 4. Column Metadata Alias API Is Type-Incompatible

Status: fixed.

Current project has:

```scala
def as(alias: String, metadata: String): Column
```

Spark v4.1.2 has:

```scala
def as(alias: String, metadata: Metadata): Column
```

Spark v4.1.2 also exposes:

```scala
def as(alias: Symbol): Column
```

Impact:

This is source-incompatible with standard Spark. User code that imports or constructs `Metadata` cannot call `Column.as("name", metadata)`.

Recommendation:

After adding `Metadata`, replace or overload the current string-based metadata API with `Metadata`. Keep string metadata only as an internal helper or explicit compatibility method if needed.

### 5. Java UDF Interfaces And UDF Overloads Stop Too Early

Status: fixed.

Current project has Java UDF interfaces only from:

- `UDF0.java` through `UDF9.java`

Spark v4.1.2 has:

- `UDF0.java` through `UDF22.java`

Current `UDFRegistration` Scala closure registration reaches `Function10`. Spark v4.1.2 generated registrations reach `Function22` and Java `UDF22`.

Impact:

Higher-arity Java UDFs and Scala closures that compile under Spark v4.1.2 fail against this client.

Recommendation:

Add `UDF10` through `UDF22` Java interfaces and generated overloads in:

- `functions.udf(...)`
- `spark.udf.register(...)`

This is mostly mechanical, but should include compile tests to avoid arity/order mistakes.

### 6. RelationalGroupedDataset Public Type Is Missing

Status: fixed.

Current project returns a custom type:

```scala
def groupBy(cols: Column*): GroupedDataFrame
```

Spark v4.1.2 returns:

```scala
def groupBy(cols: Column*): RelationalGroupedDataset
```

Current `GroupedDataFrame` already implements many of the expected operations, including `agg`, `count`, `mean`, `avg`, `max`, `min`, `sum`, and `pivot`.

Impact:

Chained calls often work, but explicit user/library type annotations do not:

```scala
val grouped: RelationalGroupedDataset = df.groupBy("k")
```

Recommendation:

Add a public `RelationalGroupedDataset` compatibility type. The lowest-risk route is a facade/alias over the existing `GroupedDataFrame`, or renaming while preserving a compatibility alias.

### 7. Avro And Protobuf Function Packages Are Missing

Status: fixed.

Spark v4.1.2 exposes:

- `org.apache.spark.sql.avro.functions`
- `org.apache.spark.sql.protobuf.functions`

Examples include:

- `from_avro`
- `to_avro`
- `schema_of_avro`
- `from_protobuf`
- `to_protobuf`

Current project does not have these packages.

Impact:

Users migrating Spark code that imports these package-specific functions get compile errors, even though many of these functions can be represented as ordinary unresolved Spark SQL functions in Connect plans.

Recommendation:

Add wrappers that build `Column` expressions via existing function-construction helpers. Protobuf overloads that read descriptor files need a small local file-reading utility.

## Lower-Priority Or Context-Dependent Gaps

These are visible in the v4.1.2 public API, but should not be treated as first-class gaps unless
they are also used by upstream Spark Connect users or needed for source compatibility:

- `org.apache.spark.sql.expressions.Window` and `WindowSpec`. Status: fixed in current working tree.
- Java streaming facade ergonomics for `OutputMode`, `TimeMode`, and `GroupStateTimeout`. Status:
  fixed in current working tree.
- `org.apache.spark.sql.expressions.javalang.typed`. Status: fixed in current working tree.
- connector catalog Java classes such as `Identifier`, `IdentifierImpl`, and `IdentityColumnSpec`.
  Status: fixed in current working tree.
- `Encoders.kryo`, `Encoders.javaSerialization`, and `Encoders.bean`. Status: source-compatible
  shims fixed in current working tree; runtime use fails clearly because these are classic-only in
  this client.
- `DatasetHolder`, except where needed for local `Seq.toDS` / `Seq.toDF` compatibility. Status:
  fixed in current working tree.
- broader `SQLImplicits` parity with RDD conversions, which is classic-only. Status: session-bound
  source compatibility fixed in current working tree; RDD conversions remain out of scope.
- `SQLContext`. Status: Connect-backed facade fixed in current working tree.
- state variable source-file split compatibility: `ValueState.scala`, `ListState.scala`, and
  `MapState.scala` rather than bundled declarations. Status: no code change needed; the public type
  names already exist in `org.apache.spark.sql.streaming`.

Some of these are useful compatibility shims; others depend on broader classic Spark or Java/Scala 2
reflection behavior and should be evaluated case by case. For this project, Connect behavior and
source compatibility should win over classic parity.

## Recommended Implementation Order

Completed order:

1. Add `org.apache.spark.sql.streaming.Trigger` and update `DataStreamWriter`.
2. Add `SparkSession.Builder` compatibility methods and `spark.implicits`.
3. Add `Metadata`, `MetadataBuilder`, extend `StructField`, and update `Column.as(..., Metadata)`.
4. Add Java `DataTypes` and `RowFactory`.
5. Add `UDF10` through `UDF22` and UDF overload tests.
6. Add `RelationalGroupedDataset` compatibility type.
7. Add Avro / Protobuf functions packages.

Next lower-priority order:

1. Add `org.apache.spark.sql.expressions.Window` and `WindowSpec` package-name compatibility. Status: fixed in current working tree.
2. Tighten Java accessors/factory ergonomics for streaming mode classes. Status: fixed in current working tree.
3. Add `org.apache.spark.sql.expressions.javalang.typed` where the existing typed aggregation surface can support it. Status: fixed in current working tree.
4. Add lightweight connector catalog Java value classes where they are pure data/API shims. Status: fixed in current working tree.
5. Evaluate encoder helper shims (`kryo`, `javaSerialization`, `bean`) case by case, because they depend more heavily on classic Spark serialization/reflection behavior. Status: source-compatible shims fixed in current working tree.
6. Add `DatasetHolder` and `SQLImplicits` source compatibility for local `Seq.toDS` / `Seq.toDF`. Status: fixed in current working tree.
7. Add a Connect-backed `SQLContext` facade for common delegated APIs. Status: fixed in current working tree.
8. Confirm state variable public type names. Status: no code change needed.

## Verification Suggestions

Add compile-only tests for common migration snippets:

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

val spark = SparkSession.builder()
  .appName("compat")
  .master("local[*]")
  .remote("sc://localhost:15002")
  .getOrCreate()

import spark.implicits._

val metadata = new MetadataBuilder().putString("comment", "id column").build()
val schema = StructType(Seq(StructField("id", LongType, nullable = false, metadata)))
val grouped: RelationalGroupedDataset = spark.emptyDataFrame.groupBy("id")
val c = functions.col("id").as("id2", metadata)
```

And Java compile checks for:

```java
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.api.java.UDF22;
```
