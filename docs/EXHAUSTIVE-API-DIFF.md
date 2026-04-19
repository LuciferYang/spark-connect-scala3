# Exhaustive API Diff: SC3 v0.4.0 vs Official Spark Connect 4.1.1

Generated: 2026-04-19

## Methodology

1. Extracted all `def` signatures from official Spark 4.x `sql/api/` abstract classes (25 user-facing classes)
2. Extracted all `def` signatures from SC3 `src/main/scala/.../sql/` (26 classes)
3. Normalized class names: `Dataset` (official) -> `DataFrame` + `Dataset` (SC3), `RelationalGroupedDataset` -> `GroupedDataFrame`
4. Diffed method names (ignoring overload parameter details)
5. Manually verified each diff entry against SC3 source, including vals/inline defs

## Summary

| Metric | Count |
|--------|-------|
| Official unique method names | 1,212 |
| SC3 unique method names | 1,181 |
| Not applicable (server-only) | 25 |
| Deliberate design difference | 20 |
| Present with different signature | 56 |
| **True gaps remaining** | **31 → 9** |
| **API coverage** | **97.4% → 99.3%** |

---

## True Gaps (31 methods)

### Priority A: Commonly Used (8 → 1 remaining)

| # | Class | Method | Description | Status |
|---|-------|--------|-------------|--------|
| 1 | `Dataset` | `groupingSets(...)` | GROUPING SETS multi-dim aggregation (delegate to DataFrame) | **Done** |
| 2 | `Dataset` | `mergeInto(table, cond)` | MERGE INTO DML (delegate to DataFrame) | **Done** |
| 3 | `Dataset` | `withMetadata(colName, metadata)` | Attach metadata to column (delegate to DataFrame) | **Done** |
| 4 | `StreamingQuery` | `sparkSession` | Back-reference to owning session | **Done** |
| 5 | `GroupedDataFrame` | `as[K]` | Convert to `KeyValueGroupedDataset[K, Row]` | Deferred (needs KVGD refactor) |
| 6 | `Row` | `toJson` / `iteratorToJsonArray` | JSON serialization of Row | **Done** |
| 7 | `Row` | `merge(other: Row)` | Merge two Row instances | **Done** |
| 8 | `Row` | `unapplySeq` | Pattern matching extractor | **Done** |

### Priority B: Niche / Advanced (12 → 2 remaining)

| # | Class | Method | Description | Status |
|---|-------|--------|-------------|--------|
| 9 | `Column` | `explain(extended)` | Print expression tree (debug) | **Done** |
| 10 | `DataFrame`/`Dataset` | `randomSplitAsList(weights)` | Java `List[DataFrame]` return variant | **Done** |
| 11 | `DataFrameStatFunctions` | `bloomFilter(col, ...)` | Build Bloom filter (4 overloads) | **Done** |
| 12 | `DataFrameStatFunctions` | `countMinSketch(col, ...)` | Build Count-Min Sketch (4 overloads) | **Done** |
| 13 | `DataFrameWriter` | `xml(path)` | Write to XML format | **Done** |
| 14 | `Observation` | `getAsJava` | Return `java.util.Map` instead of Scala Map | **Done** |
| 15 | `Encoders` | `CHAR` | CharType encoder constant | **Done** |
| 16 | `Encoders` | `VARCHAR` | VarcharType encoder constant | **Done** |
| 17 | `Encoders` | `DURATION` | DurationType encoder constant | **Done** |
| 18 | `Encoders` | `PERIOD` | PeriodType encoder constant | **Done** |
| 19 | `Encoders` | `LOCALTIME` | LocalTimeType encoder constant | **Done** |
| 20 | `Encoders` | `tuple[T1]` | Single-element tuple encoder | **Done** |

### Priority C: Spatial / Rarely Used (5)

| # | Class | Method | Description | Effort |
|---|-------|--------|-------------|--------|
| 21 | `Encoders` | `GEOGRAPHY` | GeographyType encoder constant | Low |
| 22 | `Encoders` | `GEOMETRY` | GeometryType encoder constant | Low |
| 23 | `Encoders` | `udt[T]` | User-defined type encoder | Medium |
| 24 | `Row` | `getGeography` | Get geography column value | Medium |
| 25 | `Row` | `getGeometry` | Get geometry column value | Medium |

### Priority D: Deprecated but Still Present (6 → 4 remaining)

These methods are `@deprecated` in official Spark but still exist and should be supported (with `@deprecated` annotation):

| # | Class | Method | Description | Status |
|---|-------|--------|-------------|--------|
| 26 | `DataFrame` | `registerTempTable(tableName)` | @deprecated since 2.0, use `createOrReplaceTempView` | **Done** |
| 27 | `Dataset` | `registerTempTable(tableName)` | @deprecated since 2.0, delegate to DataFrame | **Done** |
| 28 | `DataFrame` | `explode[A <: Product](input*)(f)` | @deprecated since 2.0, use `select` + `functions.explode` | Deferred (needs TypeTag) |
| 29 | `DataFrame` | `explode[A, B](inputCol, outputCol)(f)` | @deprecated since 2.0, use `select` + `functions.explode` | Deferred (needs TypeTag) |
| 30 | `Dataset` | `explode[A <: Product](input*)(f)` | @deprecated since 2.0, delegate to DataFrame | Deferred (needs TypeTag) |
| 31 | `Dataset` | `explode[A, B](inputCol, outputCol)(f)` | @deprecated since 2.0, delegate to DataFrame | Deferred (needs TypeTag) |

---

## Not Applicable (25 methods) — Correctly Excluded

These methods exist in official Spark but are intentionally not in SC3:

| Reason | Methods |
|--------|---------|
| RDD-based (not in Connect) | `rdd`, `toJavaRDD`, `javaRDD` (x2 for DataFrame+Dataset) |
| Server/Classic-only | `SparkSession.classic`, `.sqlContext`, `.withActive`, `.stream`, `.addArtifacts`, `.setActiveSession`, `.setDefaultSession`, `.getActiveSession`, `.getDefaultSession` |
| Internal | `repartitionById` (x2) |
| Serialization | `Encoders.bean`, `.kryo` (x2), `.javaSerialization` (x2) |
| Java UDF class | `UDFRegistration.registerJava` |
| Python-only TVF | `TableValuedFunction.python_worker_logs` |
| JVM standard | `Column.equals`, `.hashCode` |

## Deliberate Design Differences (20 methods)

In official Spark, `Dataset` = `DataFrame` (unified). In SC3, they are separate classes:
- Typed operations (`map`, `flatMap`, `mapPartitions`, `foreach`, `foreachPartition`, `groupByKey`, `joinWith`, `reduce`, `select[U1..U5]`, `as[U]`, `transform[U,DSO]`) live on `Dataset[T]`, not on `DataFrame`.
- This is a deliberate architectural choice in SC3.

## Signature Matches (56 methods)

These exist in SC3 with equivalent functionality but different Scala 3 signatures:
- **ColumnName DSL** (16): `boolean`, `int`, `string`, etc. — exist on SC3's `ColumnName` class
- **SparkSession.Builder** (10): `builder`, `getOrCreate`, `config`, `master`, `appName`, `remote`, `connect`, `create`, `enableHiveSupport`, `withExtensions`
- **DataFrameWriterV2 overrides** (6): `clusterBy`, `option`, `options`, `partitionedBy`, `tableProperty`, `using`
- **Dataset typed ops** (7): `map[U]`, `flatMap[U]`, `groupByKey[K]`, etc. — exist with Scala 3 syntax
- **KeyValueGroupedDataset** (7): `cogroup`, `cogroupSorted`, `flatMapGroups`, `flatMapSortedGroups`, `mapGroups` — exist
- **UDFRegistration** (2): `register[...]` — SC3 uses `inline def register`
- **functions** (6): `broadcast`, `typedLit`, `typedlit`, `udf` variants — exist with Scala 3 inline
- **Misc** (2): `Encoders.product`, `DataStreamWriter.option`

## SC3-Only Methods (116)

Methods in SC3 not in official (SC3-specific additions):
- **Catalog extensions** (12): `analyzeTable`, `catalogExists`, `createDatabase`, `dropDatabase`, `dropTable`, `dropView`, `getCreateTableString`, `getTableProperties`, `listCachedTables`, `listPartitions`, `listViews`, `truncateTable`
- **Column DSL** (8): `&`, `^`, `|` operators; `lit` companion; Window methods on Column
- **functions extras** (47): tuple sketch functions, time conversion functions, `kll_merge_agg_*`
- **UDF inline overloads** (11): `register[R, A1..A10]` variants
- **Encoders** (5): `fromRow`, `schema`, `toRow`, `inline product`, `agnosticEncoder`
- **SparkSession/RuntimeConfig** (12): `sessionId`, `cloneSession`, `registerClassFinder`, `addClassDir`, config ops (`get`, `getAll`, `getOption`, `isModifiable`, `set`, `unset`, `build`)
- **Misc** (21): `DataFrame.broadcast`, `.close`, `.transform`; `Dataset.sparkSession`, `.close`; `Row.fromSeqWithSchema`; etc.

---

## Recommended Next Steps

### Remaining 9 gaps (all deferred):

| Category | Items | Reason |
|----------|-------|--------|
| **GroupedDataFrame.as[K]** | #5 | Needs `KeyValueGroupedDatasetImpl` with column-expression support |
| **Deprecated explode** | #28-31 | Needs TypeTag machinery in Scala 3; deprecated since 2.0 |
| **Spatial types** | #21-25 | `@Unstable`; complex type infrastructure (Geometry/Geography) |
