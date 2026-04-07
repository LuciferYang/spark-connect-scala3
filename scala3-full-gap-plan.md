# SC3 Full API Completion Plan

> Based on a file-by-file comparison of the SC3 source against the official Spark Connect client (sql/api + sql/connect/common)

## Current status summary

| Class | SC3 LOC | Implemented core methods | Missing real Connect methods |
|-------|---------|--------------------------|------------------------------|
| SparkSession | ~600 | range, sql, createDataFrame(Row), read/readStream/writeStream, table, catalog, udf, stop, newSession, cloneSession, executeCommand, static session mgmt | ~22 |
| DataFrame | ~800 | select, filter, where, join (4 variants), groupBy, orderBy, sort, limit, distinct, union/intersect/except, drop, withColumn, agg, show, collect, head, take, write, persist/unpersist/checkpoint, explain, describe, summary, sample, randomSplit, observe, unpivot/melt, withColumnsRenamed, withColumns, lateralJoin, groupingSets, repartitionByRange, toJSON, transpose, zipWithIndex, colRegex, metadataColumn, withMetadata, dropDuplicatesWithinWatermark, collectAsList, takeAsList, isLocal | ~20 |
| Dataset[T] | ~300 | map, flatMap, mapPartitions, filter, reduce, groupByKey, joinWith, as[U], foreach, foreachPartition, toLocalIterator, toJSON, typed select, collectAsList, takeAsList | ~15 |
| Column | 405 | ===, =!=, >, >=, <, <=, &&, ||, !, +, -, *, /, %, isNull, isNotNull, isNaN, contains, startsWith, endsWith, like, rlike, isin, between, substr(Int), when/otherwise, getItem, getField, apply, withField, dropFields, cast(String), as/alias, asc/desc family, over(WindowSpec) | ~25 |
| DataFrameReader | 59 | format, option, options, schema(String), load, load(path), table, json/parquet/orc/csv/text (single path) | ~15 |
| DataFrameWriter | 108 | format, mode(String), option, options, partitionBy, bucketBy, sortBy, save, save(path), saveAsTable, insertInto, json/parquet/orc/csv/text | ~8 |
| DataStreamReader | 66 | format, option, options, schema(String), load, load(path), table | ~12 |
| DataStreamWriter | 145 | format, outputMode(String), trigger, queryName, option, options, partitionBy, foreachBatch, foreach, start, start(path), toTable | ~5 |
| KeyValueGroupedDataset | 607 | keys, mapGroups, flatMapGroups, reduceGroups, count, cogroup, agg (1–4 arity), mapGroupsWithState (2), flatMapGroupsWithState (2), transformWithState (3) | ~8 |

**Total: roughly 130 missing real Connect methods** (excluding ClassicOnly / RDD-only stubs)

---

## Phase 4: Core API completion (high priority)

### 4.1 Column core methods

**File**: `Column.scala` (+60 lines)

| Method | Description | Implementation |
|--------|-------------|----------------|
| `<=>(other: Any): Column` | Null-safe equality | `fn("<=>", Column.lit(other))` |
| `eqNullSafe(other: Any): Column` | Java alias for `<=>` | Delegates to `<=>` |
| `cast(to: DataType): Column` | Cast using a DataType object | `Expression.Cast` + the `type` field from `DataTypeProtoConverter.toProto(to)` |
| `try_cast(to: String): Column` | Safe cast (returns null on failure) | `Expression.Cast` + `eval_mode = TRY` |
| `try_cast(to: DataType): Column` | DataType variant | Same as above |
| `ilike(literal: String): Column` | Case-insensitive LIKE | `fn("ilike", Column.lit(literal))` |
| `as[U: Encoder]: TypedColumn[Any, U]` | Produce a TypedColumn | `TypedColumn(this, summon[Encoder[U]])` |

### 4.2 DataFrame/Dataset core methods

**Files**: `DataFrame.scala` (+40 lines), `Dataset.scala` (+20 lines)

| Method | Description | Implementation |
|--------|-------------|----------------|
| `col(colName: String): Column` | Column reference bound to the current DataFrame | Build an `UnresolvedAttribute` carrying a planId |
| `apply(colName: String): Column` | Alias for `col` | Delegates to `col` |
| `to(schema: StructType): DataFrame` | Schema reconciliation | `Relation.ToSchema` proto |
| `dtypes: Array[(String, String)]` | Array of (column name, type) | `schema.fields.map(f => (f.name, f.dataType.simpleString))` |
| `printSchema(level: Int): Unit` | Print schema with a nesting depth limit | Client-side implementation (walk the schema tree) |
| `as(alias: String): Dataset[T]` | Give a Dataset an alias | Delegates to `DataFrame.alias(alias)` |
| `Dataset.transform[U](t: Dataset[T] => Dataset[U]): Dataset[U]` | Functional transform | `t(this)` |

### 4.3 Join overloads

**File**: `DataFrame.scala` (+30 lines)

| Method | Description |
|--------|-------------|
| `join(right: DataFrame): DataFrame` | Conditionless join (cross/inner) |
| `join(right, usingColumn: String): DataFrame` | Single-column equi-join |
| `join(right, usingColumns: Seq[String], joinType: String): DataFrame` | Multi-column equi-join + join type |
| `join(right, usingColumn: String, joinType: String): DataFrame` | Single-column equi-join + join type |

Proto: `Join.using_columns` (repeated string field 5)

### 4.4 SparkSession lifecycle

**File**: `SparkSession.scala` (+25 lines)

| Method | Description |
|--------|-------------|
| `close(): Unit` | Implements `Closeable`; calls `releaseSession` + `client.close()` and clears the active/default session |
| `Builder.getOrCreate(): SparkSession` | Cache/reuse a session keyed by config (standard API) |
| `Builder.create(): SparkSession` | Equivalent to the current `build()`, renamed for alignment |

---

## Phase 5: Convenience methods and overloads (medium priority)

### 5.1 Column convenience methods

**File**: `Column.scala` (+80 lines)

| Method | Description |
|--------|-------------|
| `equalTo(other: Any)` | Java alias for `===` |
| `notEqual(other: Any)` | Java alias for `=!=` |
| `gt(other: Any)` / `lt(other: Any)` / `leq(other: Any)` / `geq(other: Any)` | Java aliases |
| `and(other: Column)` / `or(other: Column)` | Java aliases for `&&` / `||` |
| `isInCollection(values: Iterable[_])` | Collection-based isin |
| `substr(startPos: Column, len: Column)` | Column-argument substring |
| `over(): Column` | No-arg window (entire window) |
| `bitwiseOR(other: Any)` / `bitwiseAND(other: Any)` / `bitwiseXOR(other: Any)` | Bitwise operations |
| `as(aliases: Seq[String])` | Multi-alias (TVF) |
| `as(alias: String, metadata: Metadata)` | Alias with metadata |

### 5.2 DataFrameReader completion

**File**: `DataFrameReader.scala` (+60 lines)

| Method | Description |
|--------|-------------|
| `schema(schema: StructType): this.type` | Schema as a StructType object |
| `option(key, Boolean/Long/Double)` | Typed option |
| `load(paths: String*)` | Multi-path varargs load |
| `json(paths: String*)` / `csv(paths: String*)` / `parquet(paths: String*)` / `orc(paths: String*)` / `text(paths: String*)` | Multi-path varargs |
| `xml(path: String)` / `xml(paths: String*)` | XML reading |
| `textFile(path: String): Dataset[String]` / `textFile(paths: String*)` | Returns `Dataset[String]` |
| `jdbc(url, table, properties)` | Basic JDBC |
| `jdbc(url, table, columnName, lowerBound, upperBound, numPartitions, properties)` | Partitioned JDBC |
| `json(jsonDataset: Dataset[String])` / `csv(csvDataset: Dataset[String])` | Dataset-based reading |

### 5.3 DataFrameWriter completion

**File**: `DataFrameWriter.scala` (+30 lines)

| Method | Description |
|--------|-------------|
| `mode(saveMode: SaveMode)` | SaveMode enum variant (requires defining SaveMode) |
| `option(key, Boolean/Long/Double)` | Typed option |
| `clusterBy(colName, colNames*)` | Cluster-by |
| `jdbc(url, table, properties)` | JDBC write |
| `xml(path: String)` | XML write |

### 5.4 DataStreamReader completion

**File**: `DataStreamReader.scala` (+40 lines)

| Method | Description |
|--------|-------------|
| `schema(schema: StructType)` | Schema as a StructType object |
| `option(key, Boolean/Long/Double)` | Typed option |
| `json(path)` / `csv(path)` / `parquet(path)` / `orc(path)` / `text(path)` / `xml(path)` | Format shortcuts |
| `textFile(path): Dataset[String]` | Returns `Dataset[String]` |

### 5.5 DataStreamWriter completion

**File**: `DataStreamWriter.scala` (+15 lines)

| Method | Description |
|--------|-------------|
| `outputMode(outputMode: OutputMode)` | OutputMode enum variant |
| `clusterBy(colNames*)` | Cluster-by |
| `option(key, Boolean/Long/Double)` | Typed option |

### 5.6 DataFrame convenience overloads

**File**: `DataFrame.scala` (+40 lines)

| Method | Description |
|--------|-------------|
| `agg(aggExpr: (String, String), aggExprs: (String, String)*)` | String-based aggregation |
| `agg(exprs: Map[String, String])` | Map-based string aggregation |
| `show(): Unit` / `show(numRows: Int)` / `show(truncate: Boolean)` / `show(numRows, truncate: Boolean)` | `show` overloads |
| `sample(fraction)` / `sample(fraction, seed)` / `sample(withReplacement, fraction)` | `sample` overloads |
| `randomSplit(weights: Array[Double])` | Variant without seed |

### 5.7 Dataset convenience methods

**File**: `Dataset.scala` (+20 lines)

| Method | Description |
|--------|-------------|
| `unionByName(other: Dataset[T], allowMissingColumns: Boolean)` | With `allowMissingColumns` |
| `intersectAll(other: Dataset[T])` | Typed intersectAll |
| `exceptAll(other: Dataset[T])` | Typed exceptAll |

### 5.8 KeyValueGroupedDataset completion

**File**: `KeyValueGroupedDataset.scala` (+60 lines)

| Method | Description |
|--------|-------------|
| `keyAs[L: Encoder]: KeyValueGroupedDataset[L, V]` | Re-type the key |
| `mapValues[W: Encoder](func: V => W): KeyValueGroupedDataset[K, W]` | Transform values |
| `flatMapSortedGroups[U: Encoder](sortExprs: Column*)(f: (K, Iterator[V]) => IterableOnce[U])` | flatMapGroups after sorting |
| `cogroupSorted[U, R: Encoder](other)(thisSortExprs*)(otherSortExprs*)(f)` | Sorted cogroup |
| `mapGroupsWithState[S, U](func)` | Variant without timeout (NoTimeout) |
| `agg[U1..U5]` through `agg[U1..U8]` | 5–8 arity aggregations |

### 5.9 SparkSession convenience methods

**File**: `SparkSession.scala` (+40 lines)

| Method | Description |
|--------|-------------|
| `createDataFrame[A <: Product: TypeTag](data: Seq[A])` | Infer schema from a case class (requires TypeTag or a Scala 3 Mirror) |
| `withActive[T](block: => T): T` | Run a block with this session as active |
| `time[T](f: => T): T` | Timing utility |
| `Builder.config(key, Long/Double/Boolean)` | Typed config |
| `Builder.config(map: Map[String, Any])` | Map-based config |
| `Builder.interceptor(ClientInterceptor)` | gRPC interceptor |
| `addArtifact(uri: URI)` / `addArtifact(source, target)` / `addArtifacts(uri*)` | Artifact overloads |
| `range(start, end)` | 2-arg range |
| `emptyDataFrame(schema: StructType)` | Empty DataFrame with a given schema |

---

## Phase 6: Java interoperability (low priority)

A pure Scala 3 project usually doesn't need these, but they can be implemented optionally for completeness.

**Files**: `Dataset.scala` (+40 lines), `KeyValueGroupedDataset.scala` (+30 lines)

| Method | Description |
|--------|-------------|
| `filter(func: FilterFunction[T])` | Java FilterFunction |
| `map[U](func: MapFunction[T, U], encoder: Encoder[U])` | Java MapFunction |
| `flatMap[U](f: FlatMapFunction[T, U], encoder: Encoder[U])` | Java FlatMapFunction |
| `mapPartitions[U](f: MapPartitionsFunction[T, U], encoder: Encoder[U])` | Java MapPartitionsFunction |
| `foreach(func: ForeachFunction[T])` | Java ForeachFunction |
| `foreachPartition(func: ForeachPartitionFunction[T])` | Java ForeachPartitionFunction |
| `reduce(func: ReduceFunction[T])` | Java ReduceFunction |
| `groupByKey[K](func: MapFunction[T, K], encoder: Encoder[K])` | Java groupByKey |
| `sql(sqlText, args: java.util.Map)` / `sql(sqlText, args: Array[_])` | Java sql overloads |
| `createDataFrame(rows: java.util.List[Row], schema)` | Java createDataFrame |
| `createDataset[T](data: java.util.List[T])` | Java createDataset |
| `options(java.util.Map)` (Reader/Writer everywhere) | Java Map options |

---

## Deferred — not doing

| Item | Reason |
|------|--------|
| `scalar()` / `exists()` subqueries | Requires a `WithRelations` architectural rework; high complexity |
| `repartitionById` | Spark 4.1.0+ feature, `DirectShufflePartitionId` proto |
| `newDataFrame(f)` / `newDataset(encoder)(f)` / `execute(command)` | DeveloperApi, internal proto builders |
| `explode` (deprecated) | Deprecated since 3.5.0 |
| `registerTempTable` (deprecated) | Already covered by `createOrReplaceTempView` |
| `localCheckpoint(eager, storageLevel)` | 4.0.0+ feature |

---

## Implementation order and effort estimates

| Phase | Estimated added lines | Files touched | Estimated time |
|-------|----------------------|---------------|----------------|
| Phase 4.1 Column core | +60 | Column.scala | 1h |
| Phase 4.2 DataFrame/Dataset core | +60 | DataFrame.scala, Dataset.scala | 1.5h |
| Phase 4.3 Join overloads | +30 | DataFrame.scala | 0.5h |
| Phase 4.4 SparkSession lifecycle | +25 | SparkSession.scala | 1h |
| Phase 5.1 Column convenience | +80 | Column.scala | 1h |
| Phase 5.2 DataFrameReader | +60 | DataFrameReader.scala | 1h |
| Phase 5.3 DataFrameWriter | +30 | DataFrameWriter.scala | 0.5h |
| Phase 5.4 DataStreamReader | +40 | DataStreamReader.scala | 0.5h |
| Phase 5.5 DataStreamWriter | +15 | DataStreamWriter.scala | 0.25h |
| Phase 5.6 DataFrame overloads | +40 | DataFrame.scala | 0.5h |
| Phase 5.7 Dataset convenience | +20 | Dataset.scala | 0.5h |
| Phase 5.8 KVGD completion | +60 | KeyValueGroupedDataset.scala | 1.5h |
| Phase 5.9 SparkSession convenience | +40 | SparkSession.scala | 1h |
| Phase 6 Java interop | +70 | Dataset.scala, KVGD.scala, SparkSession.scala | 1.5h |
| Tests | +200 | Each suite | 2h |
| Doc updates | ~100 | API-GAPS.md, README.md | 0.5h |
| **Total** | **~930** | | **~13h** |

---

## File change summary

| File | Phase 4 | Phase 5 | Phase 6 | Total |
|------|---------|---------|---------|-------|
| Column.scala | +60 | +80 | — | +140 |
| DataFrame.scala | +70 | +40 | — | +110 |
| Dataset.scala | +20 | +20 | +40 | +80 |
| DataFrameReader.scala | — | +60 | — | +60 |
| DataFrameWriter.scala | — | +30 | — | +30 |
| DataStreamReader.scala | — | +40 | — | +40 |
| DataStreamWriter.scala | — | +15 | — | +15 |
| KeyValueGroupedDataset.scala | — | +60 | +30 | +90 |
| SparkSession.scala | +25 | +40 | — | +65 |
| Test files | +80 | +80 | +40 | +200 |
| Docs | — | — | — | +100 |

---

## Verification

After each step:
1. `build/sbt compile` — compiles cleanly
2. `build/sbt scalafmtCheckAll` — formatting check
3. `build/sbt test` — tests pass

Integration tests can be run after Phase 4 is complete (requires a Spark Connect Server).
