# SC3 全量 API 补全计划

> 基于 SC3 源码 vs 官方 Spark Connect 客户端（sql/api + sql/connect/common）逐文件对比

## 现状总结

| 类 | SC3 行数 | 已实现核心方法 | 缺失真实 Connect 方法数 |
|----|---------|-------------|---------------------|
| SparkSession | ~600 | range, sql, createDataFrame(Row), read/readStream/writeStream, table, catalog, udf, stop, newSession, cloneSession, executeCommand, static session mgmt | ~22 |
| DataFrame | ~800 | select, filter, where, join(4种), groupBy, orderBy, sort, limit, distinct, union/intersect/except, drop, withColumn, agg, show, collect, head, take, write, persist/unpersist/checkpoint, explain, describe, summary, sample, randomSplit, observe, unpivot/melt, withColumnsRenamed, withColumns, lateralJoin, groupingSets, repartitionByRange, toJSON, transpose, zipWithIndex, colRegex, metadataColumn, withMetadata, dropDuplicatesWithinWatermark, collectAsList, takeAsList, isLocal | ~20 |
| Dataset[T] | ~300 | map, flatMap, mapPartitions, filter, reduce, groupByKey, joinWith, as[U], foreach, foreachPartition, toLocalIterator, toJSON, typed select, collectAsList, takeAsList | ~15 |
| Column | 405 | ===, =!=, >, >=, <, <=, &&, ||, !, +, -, *, /, %, isNull, isNotNull, isNaN, contains, startsWith, endsWith, like, rlike, isin, between, substr(Int), when/otherwise, getItem, getField, apply, withField, dropFields, cast(String), as/alias, asc/desc系列, over(WindowSpec) | ~25 |
| DataFrameReader | 59 | format, option, options, schema(String), load, load(path), table, json/parquet/orc/csv/text(单path) | ~15 |
| DataFrameWriter | 108 | format, mode(String), option, options, partitionBy, bucketBy, sortBy, save, save(path), saveAsTable, insertInto, json/parquet/orc/csv/text | ~8 |
| DataStreamReader | 66 | format, option, options, schema(String), load, load(path), table | ~12 |
| DataStreamWriter | 145 | format, outputMode(String), trigger, queryName, option, options, partitionBy, foreachBatch, foreach, start, start(path), toTable | ~5 |
| KeyValueGroupedDataset | 607 | keys, mapGroups, flatMapGroups, reduceGroups, count, cogroup, agg(1-4 arity), mapGroupsWithState(2), flatMapGroupsWithState(2), transformWithState(3) | ~8 |

**合计缺失约 130 个真实 Connect 方法**（不含 ClassicOnly/RDD-only stubs）

---

## Phase 4: 核心 API 补全（高优先级）

### 4.1 Column 核心方法

**文件**: `Column.scala` (+60 行)

| 方法 | 说明 | 实现方式 |
|------|------|---------|
| `<=>(other: Any): Column` | Null-safe 等值比较 | `fn("<=>", Column.lit(other))` |
| `eqNullSafe(other: Any): Column` | Java alias for `<=>` | 委托 `<=>` |
| `cast(to: DataType): Column` | DataType 对象 cast | `Expression.Cast` + `DataTypeProtoConverter.toProto(to)` 的 `type` field |
| `try_cast(to: String): Column` | 安全 cast（失败返回 null） | `Expression.Cast` + `eval_mode = TRY` |
| `try_cast(to: DataType): Column` | DataType 版本 | 同上 |
| `ilike(literal: String): Column` | 大小写不敏感 LIKE | `fn("ilike", Column.lit(literal))` |
| `as[U: Encoder]: TypedColumn[Any, U]` | 生成 TypedColumn | `TypedColumn(this, summon[Encoder[U]])` |

### 4.2 DataFrame/Dataset 核心方法

**文件**: `DataFrame.scala` (+40 行), `Dataset.scala` (+20 行)

| 方法 | 说明 | 实现方式 |
|------|------|---------|
| `col(colName: String): Column` | 绑定到当前 DataFrame 的列引用 | 构建带 planId 的 `UnresolvedAttribute` |
| `apply(colName: String): Column` | `col` 的 alias | 委托 `col` |
| `to(schema: StructType): DataFrame` | Schema 调和 | `Relation.ToSchema` proto |
| `dtypes: Array[(String, String)]` | 列名+类型数组 | `schema.fields.map(f => (f.name, f.dataType.simpleString))` |
| `printSchema(level: Int): Unit` | 嵌套层级限制的 schema 打印 | 客户端实现（遍历 schema tree） |
| `as(alias: String): Dataset[T]` | 给 Dataset 设别名 | 委托 `DataFrame.alias(alias)` |
| `Dataset.transform[U](t: Dataset[T] => Dataset[U]): Dataset[U]` | 函数式转换 | `t(this)` |

### 4.3 Join 重载补全

**文件**: `DataFrame.scala` (+30 行)

| 方法 | 说明 |
|------|------|
| `join(right: DataFrame): DataFrame` | 无条件 join（cross/inner） |
| `join(right, usingColumn: String): DataFrame` | 单列等值 join |
| `join(right, usingColumns: Seq[String], joinType: String): DataFrame` | 多列等值 join + join 类型 |
| `join(right, usingColumn: String, joinType: String): DataFrame` | 单列等值 join + 类型 |

Proto: `Join.using_columns` (repeated string field 5)

### 4.4 SparkSession 生命周期

**文件**: `SparkSession.scala` (+25 行)

| 方法 | 说明 |
|------|------|
| `close(): Unit` | 实现 `Closeable`，调用 `releaseSession` + `client.close()` + 清理 active/default session |
| `Builder.getOrCreate(): SparkSession` | 按 config 缓存/复用 session（标准 API） |
| `Builder.create(): SparkSession` | 等价于当前 `build()`，改名对齐 |

---

## Phase 5: 便捷方法与重载（中优先级）

### 5.1 Column 便捷方法

**文件**: `Column.scala` (+80 行)

| 方法 | 说明 |
|------|------|
| `equalTo(other: Any)` | Java alias for `===` |
| `notEqual(other: Any)` | Java alias for `=!=` |
| `gt(other: Any)` / `lt(other: Any)` / `leq(other: Any)` / `geq(other: Any)` | Java aliases |
| `and(other: Column)` / `or(other: Column)` | Java aliases for `&&` / `||` |
| `isInCollection(values: Iterable[_])` | 集合版 isin |
| `substr(startPos: Column, len: Column)` | Column 参数版 substring |
| `over(): Column` | 无参 window（全量窗口） |
| `bitwiseOR(other: Any)` / `bitwiseAND(other: Any)` / `bitwiseXOR(other: Any)` | 位运算 |
| `as(aliases: Seq[String])` | 多别名（TVF） |
| `as(alias: String, metadata: Metadata)` | 带 metadata 的别名 |

### 5.2 DataFrameReader 补全

**文件**: `DataFrameReader.scala` (+60 行)

| 方法 | 说明 |
|------|------|
| `schema(schema: StructType): this.type` | StructType 对象 schema |
| `option(key, Boolean/Long/Double)` | 类型化 option |
| `load(paths: String*)` | 多路径 varargs load |
| `json(paths: String*)` / `csv(paths: String*)` / `parquet(paths: String*)` / `orc(paths: String*)` / `text(paths: String*)` | 多路径 varargs |
| `xml(path: String)` / `xml(paths: String*)` | XML 读取 |
| `textFile(path: String): Dataset[String]` / `textFile(paths: String*)` | 返回 Dataset[String] |
| `jdbc(url, table, properties)` | 基础 JDBC |
| `jdbc(url, table, columnName, lowerBound, upperBound, numPartitions, properties)` | 分区 JDBC |
| `json(jsonDataset: Dataset[String])` / `csv(csvDataset: Dataset[String])` | Dataset-based 读取 |

### 5.3 DataFrameWriter 补全

**文件**: `DataFrameWriter.scala` (+30 行)

| 方法 | 说明 |
|------|------|
| `mode(saveMode: SaveMode)` | SaveMode enum 版本（需定义 SaveMode） |
| `option(key, Boolean/Long/Double)` | 类型化 option |
| `clusterBy(colName, colNames*)` | Cluster-by |
| `jdbc(url, table, properties)` | JDBC 写入 |
| `xml(path: String)` | XML 写入 |

### 5.4 DataStreamReader 补全

**文件**: `DataStreamReader.scala` (+40 行)

| 方法 | 说明 |
|------|------|
| `schema(schema: StructType)` | StructType 对象 schema |
| `option(key, Boolean/Long/Double)` | 类型化 option |
| `json(path)` / `csv(path)` / `parquet(path)` / `orc(path)` / `text(path)` / `xml(path)` | 格式快捷方法 |
| `textFile(path): Dataset[String]` | 返回 Dataset[String] |

### 5.5 DataStreamWriter 补全

**文件**: `DataStreamWriter.scala` (+15 行)

| 方法 | 说明 |
|------|------|
| `outputMode(outputMode: OutputMode)` | OutputMode enum 版本 |
| `clusterBy(colNames*)` | Cluster-by |
| `option(key, Boolean/Long/Double)` | 类型化 option |

### 5.6 DataFrame 便捷重载

**文件**: `DataFrame.scala` (+40 行)

| 方法 | 说明 |
|------|------|
| `agg(aggExpr: (String, String), aggExprs: (String, String)*)` | 字符串聚合 |
| `agg(exprs: Map[String, String])` | Map 字符串聚合 |
| `show(): Unit` / `show(numRows: Int)` / `show(truncate: Boolean)` / `show(numRows, truncate: Boolean)` | show 重载 |
| `sample(fraction)` / `sample(fraction, seed)` / `sample(withReplacement, fraction)` | sample 重载 |
| `randomSplit(weights: Array[Double])` | 无 seed 版本 |

### 5.7 Dataset 便捷方法

**文件**: `Dataset.scala` (+20 行)

| 方法 | 说明 |
|------|------|
| `unionByName(other: Dataset[T], allowMissingColumns: Boolean)` | 带 allowMissingColumns |
| `intersectAll(other: Dataset[T])` | typed intersectAll |
| `exceptAll(other: Dataset[T])` | typed exceptAll |

### 5.8 KeyValueGroupedDataset 补全

**文件**: `KeyValueGroupedDataset.scala` (+60 行)

| 方法 | 说明 |
|------|------|
| `keyAs[L: Encoder]: KeyValueGroupedDataset[L, V]` | 重新类型化 key |
| `mapValues[W: Encoder](func: V => W): KeyValueGroupedDataset[K, W]` | 转换 values |
| `flatMapSortedGroups[U: Encoder](sortExprs: Column*)(f: (K, Iterator[V]) => IterableOnce[U])` | 排序后 flatMapGroups |
| `cogroupSorted[U, R: Encoder](other)(thisSortExprs*)(otherSortExprs*)(f)` | 排序 cogroup |
| `mapGroupsWithState[S, U](func)` | 无 timeout 版本（NoTimeout） |
| `agg[U1..U5]` 到 `agg[U1..U8]` | 5-8 arity 聚合 |

### 5.9 SparkSession 便捷方法

**文件**: `SparkSession.scala` (+40 行)

| 方法 | 说明 |
|------|------|
| `createDataFrame[A <: Product: TypeTag](data: Seq[A])` | 从 case class 推断 schema（需 TypeTag 或 Scala 3 Mirror） |
| `withActive[T](block: => T): T` | 在此 session 上下文中执行块 |
| `time[T](f: => T): T` | 计时工具 |
| `Builder.config(key, Long/Double/Boolean)` | 类型化 config |
| `Builder.config(map: Map[String, Any])` | Map config |
| `Builder.interceptor(ClientInterceptor)` | gRPC 拦截器 |
| `addArtifact(uri: URI)` / `addArtifact(source, target)` / `addArtifacts(uri*)` | Artifact 重载 |
| `range(start, end)` | 2-arg range |
| `emptyDataFrame(schema: StructType)` | 指定 schema 的空 DataFrame |

---

## Phase 6: Java 互操作（低优先级）

纯 Scala 3 项目通常不需要这些，但为完整性可选实现。

**文件**: `Dataset.scala` (+40 行), `KeyValueGroupedDataset.scala` (+30 行)

| 方法 | 说明 |
|------|------|
| `filter(func: FilterFunction[T])` | Java FilterFunction |
| `map[U](func: MapFunction[T, U], encoder: Encoder[U])` | Java MapFunction |
| `flatMap[U](f: FlatMapFunction[T, U], encoder: Encoder[U])` | Java FlatMapFunction |
| `mapPartitions[U](f: MapPartitionsFunction[T, U], encoder: Encoder[U])` | Java MapPartitionsFunction |
| `foreach(func: ForeachFunction[T])` | Java ForeachFunction |
| `foreachPartition(func: ForeachPartitionFunction[T])` | Java ForeachPartitionFunction |
| `reduce(func: ReduceFunction[T])` | Java ReduceFunction |
| `groupByKey[K](func: MapFunction[T, K], encoder: Encoder[K])` | Java groupByKey |
| `sql(sqlText, args: java.util.Map)` / `sql(sqlText, args: Array[_])` | Java sql 重载 |
| `createDataFrame(rows: java.util.List[Row], schema)` | Java createDataFrame |
| `createDataset[T](data: java.util.List[T])` | Java createDataset |
| `options(java.util.Map)` （Reader/Writer 各处） | Java Map options |

---

## 延迟不做

| 项目 | 原因 |
|------|------|
| `scalar()` / `exists()` 子查询 | 需要 `WithRelations` 架构重构，复杂度高 |
| `repartitionById` | Spark 4.1.0+ 新特性，`DirectShufflePartitionId` proto |
| `newDataFrame(f)` / `newDataset(encoder)(f)` / `execute(command)` | DeveloperApi，内部 proto 构建器 |
| `explode` (deprecated) | 3.5.0 已废弃 |
| `registerTempTable` (deprecated) | 已有 `createOrReplaceTempView` |
| `localCheckpoint(eager, storageLevel)` | 4.0.0+ 新特性 |

---

## 实现顺序与工作量预估

| 阶段 | 预估新增行数 | 涉及文件 | 预计耗时 |
|------|-----------|---------|---------|
| Phase 4.1 Column 核心 | +60 | Column.scala | 1h |
| Phase 4.2 DataFrame/Dataset 核心 | +60 | DataFrame.scala, Dataset.scala | 1.5h |
| Phase 4.3 Join 重载 | +30 | DataFrame.scala | 0.5h |
| Phase 4.4 SparkSession 生命周期 | +25 | SparkSession.scala | 1h |
| Phase 5.1 Column 便捷 | +80 | Column.scala | 1h |
| Phase 5.2 DataFrameReader | +60 | DataFrameReader.scala | 1h |
| Phase 5.3 DataFrameWriter | +30 | DataFrameWriter.scala | 0.5h |
| Phase 5.4 DataStreamReader | +40 | DataStreamReader.scala | 0.5h |
| Phase 5.5 DataStreamWriter | +15 | DataStreamWriter.scala | 0.25h |
| Phase 5.6 DataFrame 重载 | +40 | DataFrame.scala | 0.5h |
| Phase 5.7 Dataset 便捷 | +20 | Dataset.scala | 0.5h |
| Phase 5.8 KVGD 补全 | +60 | KeyValueGroupedDataset.scala | 1.5h |
| Phase 5.9 SparkSession 便捷 | +40 | SparkSession.scala | 1h |
| Phase 6 Java 互操作 | +70 | Dataset.scala, KVGD.scala, SparkSession.scala | 1.5h |
| 测试 | +200 | 各 Suite | 2h |
| 文档更新 | ~100 | API-GAPS.md, README.md | 0.5h |
| **合计** | **~930** | | **~13h** |

---

## 文件改动汇总

| 文件 | Phase 4 | Phase 5 | Phase 6 | 合计 |
|------|---------|---------|---------|------|
| Column.scala | +60 | +80 | — | +140 |
| DataFrame.scala | +70 | +40 | — | +110 |
| Dataset.scala | +20 | +20 | +40 | +80 |
| DataFrameReader.scala | — | +60 | — | +60 |
| DataFrameWriter.scala | — | +30 | — | +30 |
| DataStreamReader.scala | — | +40 | — | +40 |
| DataStreamWriter.scala | — | +15 | — | +15 |
| KeyValueGroupedDataset.scala | — | +60 | +30 | +90 |
| SparkSession.scala | +25 | +40 | — | +65 |
| 测试文件 | +80 | +80 | +40 | +200 |
| 文档 | — | — | — | +100 |

---

## 验证

每步后：
1. `build/sbt compile` — 编译通过
2. `build/sbt scalafmtCheckAll` — 格式检查
3. `build/sbt test` — 测试通过

Phase 4 完成后可做集成测试（需 Spark Connect Server）。
