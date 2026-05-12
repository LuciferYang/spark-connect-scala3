# Spark Connect Scala 3 (SC3) — Complete Public API Extraction

Generated for gap analysis. Covers ALL 69 `.scala` files under
`src/main/scala/org/apache/spark/sql/`.

---

## org.apache.spark.sql

### === SparkSession (extends java.io.Closeable) ===

```
def sessionId: String
def sql(query: String): DataFrame
def sql(query: String, args: Map[String, Any]): DataFrame
def sql(query: String, args: Column*)(using DummyImplicit): DataFrame
def sql(query: String, args: Array[?]): DataFrame
def sql(query: String, args: java.util.Map[String, Any]): DataFrame
def table(tableName: String): DataFrame
def range(end: Long): DataFrame
def range(start: Long, end: Long, step: Long = 1): DataFrame
def range(start: Long, end: Long, step: Long, numPartitions: Int): DataFrame
def emptyDataset[T: Encoder: ClassTag]: Dataset[T]
def emptyDataFrame: DataFrame
def createDataFrame(rows: Seq[Row], schema: types.StructType): DataFrame
def createDataFrame(rows: java.util.List[Row], schema: types.StructType): DataFrame
def createDataset[T: Encoder: ClassTag](data: Seq[T]): Dataset[T]
def createDataset[T: Encoder: ClassTag](data: java.util.List[T]): Dataset[T]
def tvf: TableValuedFunction
def read: DataFrameReader
def readStream: DataStreamReader
lazy val streams: StreamingQueryManager
def catalog: Catalog
def conf: RuntimeConfig
def udf: UDFRegistration
def addArtifact(path: String): Unit
def addArtifact(bytes: Array[Byte], target: String): Unit
def addClassDir(base: Path, exclude: Path => Boolean = _ => false): Unit
def registerClassFinder(finder: ClassFinder): Unit
def version: String
def addTag(tag: String): Unit
def removeTag(tag: String): Unit
def getTags(): Set[String]
def clearTags(): Unit
def interruptAll(): Seq[String]
def interruptTag(tag: String): Seq[String]
def interruptOperation(operationId: String): Seq[String]
def time[T](f: => T): T
def stop(): Unit
def close(): Unit
def newSession(): SparkSession
def cloneSession(): SparkSession
def executeCommand(runner: String, command: String, options: Map[String, String] = Map.empty): DataFrame
def sparkContext: Nothing            // throws UnsupportedOperationException
def sharedState: Nothing             // throws UnsupportedOperationException
def sessionState: Nothing            // throws UnsupportedOperationException
def listenerManager: Nothing         // throws UnsupportedOperationException
def experimental: Nothing            // throws UnsupportedOperationException
def baseRelationToDataFrame: Nothing // throws UnsupportedOperationException
```

### === SparkSession.Builder ===

```
def remote(connectionString: String): Builder
def config(key: String, value: String): Builder
def config(key: String, value: Boolean): Builder
def config(key: String, value: Long): Builder
def config(key: String, value: Double): Builder
def config(map: Map[String, Any]): Builder
def config(map: java.util.Map[String, Any]): Builder
def build(): SparkSession
def create(): SparkSession
def getOrCreate(): SparkSession
```

### === object SparkSession ===

```
def builder(): Builder
def getActiveSession: Option[SparkSession]
def getDefaultSession: Option[SparkSession]
def active: SparkSession
def setActiveSession(session: SparkSession): Unit
def clearActiveSession(): Unit
def setDefaultSession(session: SparkSession): Unit
def clearDefaultSession(): Unit
```

### === RuntimeConfig ===

```
def get(key: String): String
def get(key: String, default: String): String
def getOption(key: String): Option[String]
def getAll: Map[String, String]
def set(key: String, value: String): Unit
def set(key: String, value: Boolean): Unit
def set(key: String, value: Long): Unit
def unset(key: String): Unit
def isModifiable(key: String): Boolean
```

---

### === DataFrame ===

```
// Transformations
def select(cols: Column*): DataFrame
def select(colNames: String*)(using DummyImplicit): DataFrame
def selectExpr(exprs: String*): DataFrame
def filter(condition: Column): DataFrame
def where(condition: Column): DataFrame
def where(conditionExpr: String): DataFrame
def filter(conditionExpr: String): DataFrame
def limit(n: Int): DataFrame
def offset(n: Int): DataFrame
def sort(cols: Column*): DataFrame
def sort(sortCol: String, sortCols: String*): DataFrame
def orderBy(cols: Column*): DataFrame
def orderBy(sortCol: String, sortCols: String*): DataFrame
def groupBy(cols: Column*): GroupedDataFrame
def groupBy(colNames: String*)(using DummyImplicit): GroupedDataFrame
def rollup(cols: Column*): GroupedDataFrame
def rollup(col1: String, cols: String*): GroupedDataFrame
def cube(cols: Column*): GroupedDataFrame
def cube(col1: String, cols: String*): GroupedDataFrame
def agg(aggExpr: Column, aggExprs: Column*): DataFrame
def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame
def agg(exprs: Map[String, String]): DataFrame
def agg(exprs: java.util.Map[String, String]): DataFrame
def join(right: DataFrame, joinExpr: Column, joinType: String = "inner"): DataFrame
def join(right: DataFrame): DataFrame
def join(right: DataFrame, usingColumn: String): DataFrame
def join(right: DataFrame, usingColumns: Seq[String]): DataFrame
def join(right: DataFrame, usingColumns: Seq[String], joinType: String): DataFrame
def crossJoin(right: DataFrame): DataFrame
def withColumn(name: String, col: Column): DataFrame
def withColumnRenamed(existing: String, newName: String): DataFrame
def drop(colNames: String*): DataFrame
def drop(cols: Column*)(using DummyImplicit): DataFrame
def distinct(): DataFrame
def dropDuplicates(): DataFrame
def dropDuplicates(colNames: Seq[String]): DataFrame
def dropDuplicates(col1: String, cols: String*): DataFrame
def dropDuplicates(colNames: Array[String]): DataFrame
def dropDuplicatesWithinWatermark(): DataFrame
def dropDuplicatesWithinWatermark(colNames: Seq[String]): DataFrame
def dropDuplicatesWithinWatermark(col1: String, cols: String*): DataFrame
def union(other: DataFrame): DataFrame
def unionAll(other: DataFrame): DataFrame
def unionByName(other: DataFrame, allowMissingColumns: Boolean = false): DataFrame
def intersect(other: DataFrame): DataFrame
def intersectAll(other: DataFrame): DataFrame
def except(other: DataFrame): DataFrame
def exceptAll(other: DataFrame): DataFrame
def repartition(numPartitions: Int): DataFrame
def repartition(numPartitions: Int, cols: Column*): DataFrame
def repartition(cols: Column*)(using DummyImplicit): DataFrame
def coalesce(numPartitions: Int): DataFrame
def sample(fraction: Double, withReplacement: Boolean = false, seed: Long = 0L): DataFrame
def describe(colNames: String*): DataFrame
def summary(statistics: String*): DataFrame
def alias(name: String): DataFrame
def toDF(colNames: String*): DataFrame
def to(schema: types.StructType): DataFrame
def hint(name: String, parameters: Any*): DataFrame
def broadcast: DataFrame
def sortWithinPartitions(cols: Column*): DataFrame
def sortWithinPartitions(colNames: String*)(using DummyImplicit): DataFrame
def tail(n: Int): Array[Row]
def transform(f: DataFrame => DataFrame): DataFrame
def cache(): DataFrame
def persist(): DataFrame
def persist(storageLevel: StorageLevel): DataFrame
def unpersist(blocking: Boolean = false): DataFrame
def checkpoint(eager: Boolean = true): DataFrame
def localCheckpoint(eager: Boolean = true): DataFrame
def na: DataFrameNaFunctions
def stat: DataFrameStatFunctions
def lateralJoin(right: DataFrame, condition: Column, joinType: String = "inner"): DataFrame
def lateralJoin(right: DataFrame): DataFrame
def lateralJoin(right: DataFrame, joinType: String): DataFrame
def groupingSets(groupingSets: Seq[Seq[Column]], cols: Column*): GroupedDataFrame
def repartitionByRange(numPartitions: Int, partitionExprs: Column*): DataFrame
def repartitionByRange(partitionExprs: Column*)(using DummyImplicit): DataFrame
def unpivot(ids: Array[Column], values: Array[Column], variableColumnName: String, valueColumnName: String): DataFrame
def unpivot(ids: Array[Column], variableColumnName: String, valueColumnName: String): DataFrame
def melt(ids: Array[Column], values: Array[Column], variableColumnName: String, valueColumnName: String): DataFrame
def melt(ids: Array[Column], variableColumnName: String, valueColumnName: String): DataFrame
def withColumnsRenamed(colsMap: Map[String, String]): DataFrame
def withColumnsRenamed(colsMap: java.util.Map[String, String]): DataFrame
def withColumns(colsMap: Map[String, Column]): DataFrame
def withColumns(colsMap: java.util.Map[String, Column]): DataFrame
def observe(name: String, expr: Column, exprs: Column*): DataFrame
def observe(observation: Observation, expr: Column, exprs: Column*): DataFrame
def explain(mode: String): Unit
def randomSplit(weights: Array[Double], seed: Long = 0L): Array[DataFrame]
def toJSON: DataFrame
def withWatermark(eventTime: String, delayThreshold: String): DataFrame
def transpose(indexColumn: Column): DataFrame
def transpose(): DataFrame
def zipWithIndex: DataFrame
def withMetadata(columnName: String, metadata: String): DataFrame

// Actions
def collect(): Array[Row]
def collectAsList(): java.util.List[Row]
def takeAsList(n: Int): java.util.List[Row]
def count(): Long
def first(): Row
def head(n: Int): Array[Row]
def head(): Row
def take(n: Int): Array[Row]
def show(numRows: Int = 20, truncate: Int = 20): Unit
def show(truncate: Boolean): Unit
def show(numRows: Int, truncate: Boolean): Unit
def show(numRows: Int, truncate: Int, vertical: Boolean): Unit
def printSchema(): Unit
def printSchema(level: Int): Unit
def schema: StructType
def columns: Array[String]
def dtypes: Array[(String, String)]
def col(colName: String): Column
def apply(colName: String): Column
def explain(extended: Boolean = false): Unit
def isEmpty: Boolean
def toLocalIterator(): java.util.Iterator[Row] with AutoCloseable
def isStreaming: Boolean
def isLocal: Boolean
def inputFiles: Array[String]
def sameSemantics(other: DataFrame): Boolean
def semanticHash: Int
def storageLevel: StorageLevel

// Subquery expressions
def scalar(): Column
def exists(): Column

// Conversion
def as[T: Encoder: ClassTag]: Dataset[T]
def colRegex(colName: String): Column
def metadataColumn(colName: String): Column

// Views
def createTempView(viewName: String): Unit
def createOrReplaceTempView(viewName: String): Unit
def createGlobalTempView(viewName: String): Unit
def createOrReplaceGlobalTempView(viewName: String): Unit

// Writers
def write: DataFrameWriter
def writeTo(table: String): DataFrameWriterV2
def mergeInto(table: String, condition: Column): MergeIntoWriter
def writeStream: DataStreamWriter
```

---

### === Dataset[T] ===

```
def sparkSession: SparkSession
def schema: types.StructType
def toDF(): DataFrame
def toDF(colNames: String*): DataFrame

// Typed Transformations
def mapPartitions[U: Encoder: ClassTag](func: Iterator[T] => Iterator[U]): Dataset[U]
def filter(func: T => Boolean): Dataset[T]
def map[U: Encoder: ClassTag](func: T => U): Dataset[U]
def flatMap[U: Encoder: ClassTag](func: T => IterableOnce[U]): Dataset[U]
def foreach(func: T => Unit): Unit
def foreachPartition(func: Iterator[T] => Unit): Unit
def distinct(): Dataset[T]
def dropDuplicatesWithinWatermark(): Dataset[T]
def dropDuplicatesWithinWatermark(colNames: Seq[String]): Dataset[T]
def dropDuplicatesWithinWatermark(col1: String, cols: String*): Dataset[T]
def reduce(func: (T, T) => T): T
def reduce(func: ReduceFunction[T])(using DummyImplicit): T
def groupByKey[K: Encoder: ClassTag](func: T => K): KeyValueGroupedDataset[K, T]

// Untyped Transformations (delegate to DataFrame)
def select(cols: Column*): DataFrame
def select(col: String, cols: String*): DataFrame
def selectExpr(exprs: String*): DataFrame

// Typed select (TypedColumn 1-5 arity)
def select[U1: ClassTag](c1: TypedColumn[T, U1]): Dataset[U1]
def select[U1, U2](c1: TypedColumn[T, U1], c2: TypedColumn[T, U2]): Dataset[(U1, U2)]
def select[U1, U2, U3](...): Dataset[(U1, U2, U3)]
def select[U1, U2, U3, U4](...): Dataset[(U1, U2, U3, U4)]
def select[U1, U2, U3, U4, U5](...): Dataset[(U1, U2, U3, U4, U5)]

def filter(condition: Column): Dataset[T]
def filter(conditionExpr: String): Dataset[T]
def where(condition: Column): Dataset[T]
def where(conditionExpr: String): Dataset[T]
def limit(n: Int): Dataset[T]
def orderBy(cols: Column*): Dataset[T]
def sort(cols: Column*): Dataset[T]
def sort(sortCol: String, sortCols: String*): Dataset[T]
def orderBy(sortCol: String, sortCols: String*): Dataset[T]
def sample(withReplacement: Boolean, fraction: Double, seed: Long): Dataset[T]
def sample(withReplacement: Boolean, fraction: Double): Dataset[T]
def sample(fraction: Double, seed: Long): Dataset[T]
def sample(fraction: Double): Dataset[T]
def repartition(numPartitions: Int): Dataset[T]
def repartition(numPartitions: Int, cols: Column*): Dataset[T]
def repartition(cols: Column*)(using DummyImplicit): Dataset[T]
def coalesce(numPartitions: Int): Dataset[T]
def sortWithinPartitions(cols: Column*): Dataset[T]
def sortWithinPartitions(colNames: String*)(using DummyImplicit): Dataset[T]
def groupBy(cols: Column*): GroupedDataFrame
def groupBy(col1: String, cols: String*): GroupedDataFrame
def agg(aggExpr: Column, aggExprs: Column*): DataFrame
def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame
def agg(exprs: Map[String, String]): DataFrame
def agg(exprs: java.util.Map[String, String]): DataFrame
def rollup(cols: Column*): GroupedDataFrame
def rollup(col1: String, cols: String*): GroupedDataFrame
def cube(cols: Column*): GroupedDataFrame
def cube(col1: String, cols: String*): GroupedDataFrame
def offset(n: Int): Dataset[T]
def to(schema: types.StructType): DataFrame
def repartitionByRange(numPartitions: Int, partitionExprs: Column*): Dataset[T]
def repartitionByRange(partitionExprs: Column*)(using DummyImplicit): Dataset[T]
def join(right: DataFrame, joinExpr: Column, joinType: String = "inner"): DataFrame
def joinWith[U: Encoder: ClassTag](other: Dataset[U], condition: Column, joinType: String = "inner"): Dataset[(T, U)]
def observe(name: String, expr: Column, exprs: Column*): Dataset[T]
def observe(observation: Observation, expr: Column, exprs: Column*): Dataset[T]
def withColumn(name: String, col: Column): DataFrame
def withColumnRenamed(existing: String, newName: String): DataFrame
def withColumnsRenamed(colsMap: Map[String, String]): DataFrame
def withColumnsRenamed(colsMap: java.util.Map[String, String]): DataFrame
def withColumns(colsMap: Map[String, Column]): DataFrame
def withColumns(colsMap: java.util.Map[String, Column]): DataFrame
def dropDuplicates(): Dataset[T]
def dropDuplicates(colNames: Seq[String]): Dataset[T]
def dropDuplicates(col1: String, cols: String*): Dataset[T]
def dropDuplicates(colNames: Array[String]): Dataset[T]
def describe(colNames: String*): DataFrame
def summary(statistics: String*): DataFrame
def union(other: Dataset[T]): Dataset[T]
def unionAll(other: Dataset[T]): Dataset[T]
def unionByName(other: Dataset[T], allowMissingColumns: Boolean = false): Dataset[T]
def intersect(other: Dataset[T]): Dataset[T]
def intersectAll(other: Dataset[T]): Dataset[T]
def except(other: Dataset[T]): Dataset[T]
def exceptAll(other: Dataset[T]): Dataset[T]
def as(alias: String): Dataset[T]
def alias(alias: String): Dataset[T]
def transform[U](t: Dataset[T] => Dataset[U]): Dataset[U]
def cache(): Dataset[T]
def persist(): Dataset[T]
def persist(storageLevel: StorageLevel): Dataset[T]
def unpersist(blocking: Boolean = false): Dataset[T]
def colRegex(colName: String): Column
def metadataColumn(colName: String): Column
def unpivot(ids: Array[Column], values: Array[Column], variableColumnName: String, valueColumnName: String): DataFrame
def unpivot(ids: Array[Column], variableColumnName: String, valueColumnName: String): DataFrame
def melt(ids: Array[Column], values: Array[Column], variableColumnName: String, valueColumnName: String): DataFrame
def melt(ids: Array[Column], variableColumnName: String, valueColumnName: String): DataFrame
def transpose(indexColumn: Column): DataFrame
def transpose(): DataFrame
def lateralJoin(right: DataFrame, condition: Column, joinType: String = "inner"): DataFrame
def lateralJoin(right: DataFrame, joinType: String): DataFrame
def lateralJoin(right: DataFrame): DataFrame
def drop(colNames: String*): DataFrame
def drop(cols: Column*)(using DummyImplicit): DataFrame
def randomSplit(weights: Array[Double], seed: Long = 0L): Array[Dataset[T]]
def randomSplit(weights: Array[Double]): Array[Dataset[T]]

// Actions
def collect(): Array[T]
def collectAsList(): java.util.List[T]
def takeAsList(n: Int): java.util.List[T]
def first(): T
def head(n: Int): Array[T]
def head(): T
def take(n: Int): Array[T]
def count(): Long
def show(numRows: Int = 20, truncate: Int = 20): Unit
def show(truncate: Boolean): Unit
def show(numRows: Int, truncate: Boolean): Unit
def show(numRows: Int, truncate: Int, vertical: Boolean): Unit
def toJSON: Dataset[String]
def isEmpty: Boolean
def columns: Array[String]
def dtypes: Array[(String, String)]
def col(colName: String): Column
def apply(colName: String): Column
def explain(extended: Boolean = false): Unit
def explain(mode: String): Unit
def printSchema(): Unit
def printSchema(level: Int): Unit
def isStreaming: Boolean
def isLocal: Boolean
def inputFiles: Array[String]
def sameSemantics(other: Dataset[T]): Boolean
def semanticHash: Int
def storageLevel: StorageLevel
def checkpoint(eager: Boolean = true): Dataset[T]
def localCheckpoint(eager: Boolean = true): Dataset[T]
def tail(n: Int): Array[T]
def na: DataFrameNaFunctions
def stat: DataFrameStatFunctions
def hint(name: String, parameters: Any*): Dataset[T]
def crossJoin(right: DataFrame): DataFrame
def writeTo(table: String): DataFrameWriterV2
def writeStream: DataStreamWriter
def withWatermark(eventTime: String, delayThreshold: String): Dataset[T]

// Subquery expressions
def scalar(): Column
def exists(): Column

// Conversion
def as[U: Encoder: ClassTag]: Dataset[U]

// Views
def createTempView(viewName: String): Unit
def createOrReplaceTempView(viewName: String): Unit
def createGlobalTempView(viewName: String): Unit
def createOrReplaceGlobalTempView(viewName: String): Unit
def toLocalIterator(): java.util.Iterator[T] with AutoCloseable

// Writer
def write: DataFrameWriter

// Unsupported
def rdd: Nothing
def toJavaRDD: Nothing
def javaRDD: Nothing
```

---

### === Column ===

```
// Constructors
def this(name: String)

// Comparison operators
def ===(other: Column): Column
def =!=(other: Column): Column
def !==(other: Column): Column   // @deprecated
def <=>(other: Any): Column
def eqNullSafe(other: Any): Column
def >(other: Column): Column
def >=(other: Column): Column
def <(other: Column): Column
def <=(other: Column): Column
def ===(v: Any): Column
def =!=(v: Any): Column
def >(v: Any): Column
def >=(v: Any): Column
def <(v: Any): Column
def <=(v: Any): Column
def equalTo(other: Any): Column
def notEqual(other: Any): Column
def gt(other: Any): Column
def lt(other: Any): Column
def geq(other: Any): Column
def leq(other: Any): Column

// Logical operators
def &&(other: Column): Column
def ||(other: Column): Column
def unary_! : Column
def and(other: Column): Column
def or(other: Column): Column

// Arithmetic operators
def +(other: Column): Column
def -(other: Column): Column
def *(other: Column): Column
def /(other: Column): Column
def %(other: Column): Column
def unary_- : Column
def plus(v: Any): Column
def minus(v: Any): Column
def multiply(v: Any): Column
def divide(v: Any): Column
def mod(v: Any): Column

// Bitwise operators
def bitwiseOR(other: Any): Column
def bitwiseAND(other: Any): Column
def bitwiseXOR(other: Any): Column
def |(other: Any): Column
def &(other: Any): Column
def ^(other: Any): Column

// Null / NaN checks
def isNull: Column
def isNotNull: Column
def isNaN: Column

// String operators
def contains(other: Column): Column
def contains(literal: String): Column
def startsWith(other: Column): Column
def startsWith(literal: String): Column
def endsWith(other: Column): Column
def endsWith(literal: String): Column
def like(literal: String): Column
def rlike(literal: String): Column
def ilike(literal: String): Column
def isin(values: Any*): Column
def isin(ds: Dataset[?]): Column
def isin(df: DataFrame): Column
def between(lower: Any, upper: Any): Column
def substr(startPos: Int, length: Int): Column
def substr(startPos: Column, len: Column): Column
def isInCollection(values: Iterable[?]): Column

// when / otherwise
def when(condition: Column, value: Any): Column
def otherwise(value: Any): Column

// Nested data access
def getItem(key: Any): Column
def getField(fieldName: String): Column
def apply(key: Any): Column
def withField(fieldName: String, col: Column): Column
def dropFields(fieldNames: String*): Column

// Cast / Alias
def cast(to: String): Column
def cast(to: types.DataType): Column
def try_cast(to: String): Column
def try_cast(to: types.DataType): Column
def alias(name: String): Column
def as(name: String): Column
def name(n: String): Column
def as(aliases: Seq[String]): Column
def as(aliases: Array[String]): Column
def as(alias: String, metadata: String): Column
def as[U: Encoder]: TypedColumn[Any, U]

// Column transforms
def transform(f: Column => Column): Column
def outer(): Column

// Sort
def asc: Column
def asc_nulls_first: Column
def asc_nulls_last: Column
def desc: Column
def desc_nulls_first: Column
def desc_nulls_last: Column

// Window
def over(window: WindowSpec): Column
def over(): Column
```

### === object Column ===

```
def apply(name: String): Column
def lit(value: Any): Column
```

### === TypedColumn[-T, U] (extends Column) ===

```
def name(alias: String): TypedColumn[T, U]
```

---

### === WindowSpec ===

```
def partitionBy(cols: Column*): WindowSpec
def orderBy(cols: Column*): WindowSpec
def rowsBetween(start: Long, end: Long): WindowSpec
def rangeBetween(start: Long, end: Long): WindowSpec
```

### === object Window ===

```
val unboundedPreceding: Long
val unboundedFollowing: Long
val currentRow: Long
def partitionBy(cols: Column*): WindowSpec
def orderBy(cols: Column*): WindowSpec
def rowsBetween(start: Long, end: Long): WindowSpec
def rangeBetween(start: Long, end: Long): WindowSpec
```

---

### === GroupedDataFrame ===

```
def agg(aggExpr: Column, aggExprs: Column*): DataFrame
def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame
def agg(exprs: Map[String, String]): DataFrame
def agg(exprs: java.util.Map[String, String]): DataFrame
def count(): DataFrame
def mean(colNames: String*): DataFrame
def avg(colNames: String*): DataFrame
def max(colNames: String*): DataFrame
def min(colNames: String*): DataFrame
def sum(colNames: String*): DataFrame
def pivot(pivotCol: Column): GroupedDataFrame
def pivot(pivotCol: Column, values: Seq[Any]): GroupedDataFrame
def pivot(pivotColumn: String): GroupedDataFrame
def pivot(pivotColumn: String, values: Seq[Any]): GroupedDataFrame
def pivot(pivotColumn: String, values: java.util.List[Any]): GroupedDataFrame
def pivot(pivotCol: Column, values: java.util.List[Any]): GroupedDataFrame
```

### === object GroupedDataFrame ===

```
enum GroupType: case GroupBy, Rollup, Cube, Pivot, GroupingSets
```

---

### === KeyValueGroupedDataset[K, V] ===

```
def keyAs[L: Encoder: ClassTag]: KeyValueGroupedDataset[L, V]
def mapValues[W: Encoder: ClassTag](func: V => W): KeyValueGroupedDataset[K, W]
def keys: Dataset[K]
def mapGroups[U: Encoder: ClassTag](func: (K, Iterator[V]) => U): Dataset[U]
def flatMapGroups[U: Encoder: ClassTag](func: (K, Iterator[V]) => IterableOnce[U]): Dataset[U]
def reduceGroups(func: (V, V) => V)(using Encoder[(K, V)], ClassTag[(K, V)]): Dataset[(K, V)]
def count()(using Encoder[(K, Long)], ClassTag[(K, Long)]): Dataset[(K, Long)]
def flatMapSortedGroups[U: Encoder: ClassTag](sortExprs: Column*)(func: (K, Iterator[V]) => IterableOnce[U]): Dataset[U]
def cogroup[U: Encoder: ClassTag, R: Encoder: ClassTag](other: KeyValueGroupedDataset[K, U])(func: (K, Iterator[V], Iterator[U]) => IterableOnce[R]): Dataset[R]
def cogroup[U: Encoder: ClassTag, R: Encoder: ClassTag](other: KeyValueGroupedDataset[K, U], func: CoGroupFunction[K, V, U, R]): Dataset[R]
def cogroupSorted[U: Encoder: ClassTag, R: Encoder: ClassTag](other: KeyValueGroupedDataset[K, U])(thisSortExprs: Column*)(otherSortExprs: Column*)(func: (K, Iterator[V], Iterator[U]) => IterableOnce[R]): Dataset[R]
def cogroupSorted[U: Encoder: ClassTag, R: Encoder: ClassTag](other: KeyValueGroupedDataset[K, U], thisSortExprs: Array[Column], otherSortExprs: Array[Column], func: CoGroupFunction[K, V, U, R]): Dataset[R]

// Typed aggregation (agg with TypedColumn) — arities 1-8
def agg[U1](col1: TypedColumn[V, U1])(using ...): Dataset[(K, U1)]
def agg[U1, U2](col1: TypedColumn[V, U1], col2: TypedColumn[V, U2])(using ...): Dataset[(K, U1, U2)]
def agg[U1, U2, U3](...)(using ...): Dataset[(K, U1, U2, U3)]
def agg[U1, U2, U3, U4](...)(using ...): Dataset[(K, U1, U2, U3, U4)]
def agg[U1, U2, U3, U4, U5](...)(using ...): Dataset[(K, U1, U2, U3, U4, U5)]
def agg[U1, U2, U3, U4, U5, U6](...)(using ...): Dataset[(K, U1, U2, U3, U4, U5, U6)]
def agg[U1, U2, U3, U4, U5, U6, U7](...)(using ...): Dataset[(K, U1, U2, U3, U4, U5, U6, U7)]
def agg[U1, U2, U3, U4, U5, U6, U7, U8](...)(using ...): Dataset[(K, U1, U2, U3, U4, U5, U6, U7, U8)]

// Stateful streaming operations
def mapGroupsWithState[S: Encoder: ClassTag, U: Encoder: ClassTag](timeoutConf: GroupStateTimeout)(func: (K, Iterator[V], GroupState[S]) => U): Dataset[U]
def mapGroupsWithState[S: Encoder: ClassTag, U: Encoder: ClassTag](timeoutConf: GroupStateTimeout, initialState: KeyValueGroupedDataset[K, S])(func: (K, Iterator[V], GroupState[S]) => U): Dataset[U]
def flatMapGroupsWithState[S: Encoder: ClassTag, U: Encoder: ClassTag](outputMode: OutputMode, timeoutConf: GroupStateTimeout)(func: (K, Iterator[V], GroupState[S]) => Iterator[U]): Dataset[U]
def flatMapGroupsWithState[S: Encoder: ClassTag, U: Encoder: ClassTag](outputMode: OutputMode, timeoutConf: GroupStateTimeout, initialState: KeyValueGroupedDataset[K, S])(func: (K, Iterator[V], GroupState[S]) => Iterator[U]): Dataset[U]
def transformWithState[U: Encoder: ClassTag](statefulProcessor: StatefulProcessor[K, V, U], timeMode: TimeMode, outputMode: OutputMode): Dataset[U]
def transformWithState[U: Encoder: ClassTag, S: Encoder: ClassTag](statefulProcessor: StatefulProcessorWithInitialState[K, V, U, S], timeMode: TimeMode, outputMode: OutputMode, initialState: KeyValueGroupedDataset[K, S]): Dataset[U]
def transformWithState[U: Encoder: ClassTag](statefulProcessor: StatefulProcessor[K, V, U], eventTimeColumnName: String, outputMode: OutputMode): Dataset[U]
```

---

### === Row ===

```
def size: Int
def length: Int
def get(i: Int): Any
def isNullAt(i: Int): Boolean
def getBoolean(i: Int): Boolean
def getByte(i: Int): Byte
def getShort(i: Int): Short
def getInt(i: Int): Int
def getLong(i: Int): Long
def getFloat(i: Int): Float
def getDouble(i: Int): Double
def getString(i: Int): String
def getAs[T](i: Int): T
def getAs[T](fieldName: String): T
def getDecimal(i: Int): java.math.BigDecimal
def getDate(i: Int): java.sql.Date
def getTimestamp(i: Int): java.sql.Timestamp
def getInstant(i: Int): java.time.Instant
def getLocalDate(i: Int): java.time.LocalDate
def getSeq[T](i: Int): Seq[T]
def getList[T](i: Int): java.util.List[T]
def getMap[K, V](i: Int): Map[K, V]
def getJavaMap[K, V](i: Int): java.util.Map[K, V]
def getStruct(i: Int): Row
def fieldIndex(name: String): Int
def anyNull: Boolean
def json: String
def prettyJson: String
def copy(): Row
def getValuesMap[T](fieldNames: Seq[String]): Map[String, T]
def toSeq: Seq[Any]
def mkString(sep: String): String
val schema: Option[StructType]
```

### === object Row ===

```
def fromSeq(values: Seq[Any]): Row
def fromSeqWithSchema(values: Seq[Any], schema: StructType): Row
def apply(values: Any*): Row
def fromTuple(t: Product): Row
val empty: Row
```

---

### === Encoder[T] (trait) ===

```
def schema: StructType
def fromRow(row: Row): T
def toRow(value: T): Row
def agnosticEncoder: AgnosticEncoder[?]
```

### === object Encoder ===

```
// Given instances for primitive types:
given Encoder[Int], Encoder[Long], Encoder[Double], Encoder[Float],
      Encoder[Short], Encoder[Byte], Encoder[Boolean], Encoder[String],
      Encoder[java.sql.Date], Encoder[java.sql.Timestamp],
      Encoder[java.time.LocalDate], Encoder[java.time.Instant],
      Encoder[BigDecimal], Encoder[Array[Byte]]

// Inline derivation helpers:
inline def sparkTypeOf[T]: DataType
inline def isOption[T]: Boolean
inline def agnosticEncoderOf[T]: AgnosticEncoder[?]
inline def encoderFieldsOf[Types, Labels]: List[EncoderField]
inline def fieldsOf[Types, Labels]: List[StructField]

// Derived encoder:
inline given derived[T](using m: Mirror.ProductOf[T], ct: ClassTag[T]): Encoder[T]
```

---

### === object Encoders ===

```
def scalaBoolean: Encoder[Boolean]
def scalaByte: Encoder[Byte]
def scalaShort: Encoder[Short]
def scalaInt: Encoder[Int]
def scalaLong: Encoder[Long]
def scalaFloat: Encoder[Float]
def scalaDouble: Encoder[Double]
def BOOLEAN: Encoder[java.lang.Boolean]
def BYTE: Encoder[java.lang.Byte]
def SHORT: Encoder[java.lang.Short]
def INT: Encoder[java.lang.Integer]
def LONG: Encoder[java.lang.Long]
def FLOAT: Encoder[java.lang.Float]
def DOUBLE: Encoder[java.lang.Double]
def STRING: Encoder[String]
def BINARY: Encoder[Array[Byte]]
def DATE: Encoder[java.sql.Date]
def LOCALDATE: Encoder[java.time.LocalDate]
def TIMESTAMP: Encoder[java.sql.Timestamp]
def INSTANT: Encoder[java.time.Instant]
def LOCALDATETIME: Encoder[java.time.LocalDateTime]
def DECIMAL: Encoder[java.math.BigDecimal]
def row: Encoder[Row]
def tuple[T1, T2](e1, e2): Encoder[(T1, T2)]
def tuple[T1, T2, T3](e1, e2, e3): Encoder[(T1, T2, T3)]
def tuple[T1, T2, T3, T4](e1, e2, e3, e4): Encoder[(T1, T2, T3, T4)]
def tuple[T1, T2, T3, T4, T5](e1, e2, e3, e4, e5): Encoder[(T1, T2, T3, T4, T5)]
inline def product[T <: Product]: Encoder[T]
```

---

### === DataFrameReader ===

```
def format(fmt: String): DataFrameReader
def option(key: String, value: String): DataFrameReader
def option(key: String, value: Boolean): DataFrameReader
def option(key: String, value: Long): DataFrameReader
def option(key: String, value: Double): DataFrameReader
def options(m: Map[String, String]): DataFrameReader
def schema(schemaString: String): DataFrameReader
def schema(schema: types.StructType): DataFrameReader
def load(path: String): DataFrame
def load(): DataFrame
def load(paths: Seq[String]): DataFrame
def table(tableName: String): DataFrame
def json(paths: String*): DataFrame
def parquet(paths: String*): DataFrame
def orc(paths: String*): DataFrame
def csv(paths: String*): DataFrame
def text(paths: String*): DataFrame
def xml(paths: String*): DataFrame
def textFile(path: String): DataFrame
def textFile(paths: String*)(using DummyImplicit): DataFrame
def jdbc(url: String, table: String, properties: java.util.Properties): DataFrame
def jdbc(url: String, table: String, columnName: String, lowerBound: Long, upperBound: Long, numPartitions: Int, connectionProperties: java.util.Properties): DataFrame
def jdbc(url: String, table: String, predicates: Array[String], connectionProperties: java.util.Properties): DataFrame
```

---

### === DataStreamReader ===

```
def format(fmt: String): DataStreamReader
def option(key: String, value: String): DataStreamReader
def option(key: String, value: Boolean): DataStreamReader
def option(key: String, value: Long): DataStreamReader
def option(key: String, value: Double): DataStreamReader
def options(m: Map[String, String]): DataStreamReader
def schema(schemaString: String): DataStreamReader
def schema(schema: types.StructType): DataStreamReader
def load(): DataFrame
def load(path: String): DataFrame
def load(paths: Seq[String]): DataFrame
def json(path: String): DataFrame
def csv(path: String): DataFrame
def parquet(path: String): DataFrame
def orc(path: String): DataFrame
def text(path: String): DataFrame
def xml(path: String): DataFrame
def textFile(path: String): DataFrame
def table(tableName: String): DataFrame
```

---

### === DataFrameWriter ===

```
def format(fmt: String): DataFrameWriter
def mode(m: String): DataFrameWriter
def mode(m: SaveMode): DataFrameWriter
def option(key: String, value: String): DataFrameWriter
def option(key: String, value: Boolean): DataFrameWriter
def option(key: String, value: Long): DataFrameWriter
def option(key: String, value: Double): DataFrameWriter
def options(m: Map[String, String]): DataFrameWriter
def partitionBy(colNames: String*): DataFrameWriter
def bucketBy(numBuckets: Int, colName: String, colNames: String*): DataFrameWriter
def sortBy(colName: String, colNames: String*): DataFrameWriter
def clusterBy(colName: String, colNames: String*): DataFrameWriter
def save(path: String): Unit
def save(): Unit
def saveAsTable(tableName: String): Unit
def insertInto(tableName: String): Unit
def json(path: String): Unit
def parquet(path: String): Unit
def orc(path: String): Unit
def csv(path: String): Unit
def text(path: String): Unit
def jdbc(url: String, table: String, connectionProperties: java.util.Properties): Unit
```

---

### === DataFrameWriterV2 ===

```
def using(provider: String): DataFrameWriterV2
def option(key: String, value: String): DataFrameWriterV2
def option(key: String, value: Boolean): DataFrameWriterV2
def option(key: String, value: Long): DataFrameWriterV2
def option(key: String, value: Double): DataFrameWriterV2
def options(opts: Map[String, String]): DataFrameWriterV2
def tableProperty(property: String, value: String): DataFrameWriterV2
def partitionedBy(column: Column, columns: Column*): DataFrameWriterV2
def clusterBy(colName: String, colNames: String*): DataFrameWriterV2
def create(): Unit
def replace(): Unit
def createOrReplace(): Unit
def append(): Unit
def overwrite(condition: Column): Unit
def overwritePartitions(): Unit
```

---

### === DataStreamWriter ===

```
def format(fmt: String): DataStreamWriter
def outputMode(m: String): DataStreamWriter
def outputMode(outputMode: streaming.OutputMode): DataStreamWriter
def trigger(t: Trigger): DataStreamWriter
def queryName(qn: String): DataStreamWriter
def option(key: String, value: String): DataStreamWriter
def option(key: String, value: Boolean): DataStreamWriter
def option(key: String, value: Long): DataStreamWriter
def option(key: String, value: Double): DataStreamWriter
def options(m: Map[String, String]): DataStreamWriter
def partitionBy(colNames: String*): DataStreamWriter
def clusterBy(colNames: String*): DataStreamWriter
def foreachBatch(func: (DataFrame, Long) => Unit): DataStreamWriter
def foreach(writer: ForeachWriter[?]): DataStreamWriter
def start(): StreamingQuery
def start(path: String): StreamingQuery
def toTable(tableName: String): StreamingQuery
```

### === sealed trait Trigger ===

```
case class ProcessingTime(intervalMs: Long) extends Trigger
case object AvailableNow extends Trigger
case object Once extends Trigger
case class Continuous(intervalMs: Long) extends Trigger
```

---

### === MergeIntoWriter ===

```
def whenMatched(): WhenMatched
def whenMatched(condition: Column): WhenMatched
def whenNotMatched(): WhenNotMatched
def whenNotMatched(condition: Column): WhenNotMatched
def whenNotMatchedBySource(): WhenNotMatchedBySource
def whenNotMatchedBySource(condition: Column): WhenNotMatchedBySource
def withSchemaEvolution(): MergeIntoWriter
def merge(): Unit
```

### === WhenMatched ===

```
def updateAll(): MergeIntoWriter
def update(assignments: Map[String, Column]): MergeIntoWriter
def delete(): MergeIntoWriter
```

### === WhenNotMatched ===

```
def insertAll(): MergeIntoWriter
def insert(assignments: Map[String, Column]): MergeIntoWriter
```

### === WhenNotMatchedBySource ===

```
def updateAll(): MergeIntoWriter
def update(assignments: Map[String, Column]): MergeIntoWriter
def delete(): MergeIntoWriter
```

---

### === enum SaveMode ===

```
case Overwrite, Append, Ignore, ErrorIfExists
```

---

### === DataFrameNaFunctions ===

```
def drop(): DataFrame
def drop(how: String): DataFrame
def drop(cols: Seq[String]): DataFrame
def drop(cols: Array[String]): DataFrame
def drop(how: String, cols: Array[String]): DataFrame
def drop(how: String, cols: Seq[String]): DataFrame
def drop(minNonNulls: Int): DataFrame
def drop(minNonNulls: Int, cols: Array[String]): DataFrame
def drop(minNonNulls: Int, cols: Seq[String]): DataFrame
def fill(value: Double): DataFrame
def fill(value: String): DataFrame
def fill(value: Long): DataFrame
def fill(value: Boolean): DataFrame
def fill(value: Double, cols: Seq[String]): DataFrame
def fill(value: Double, cols: Array[String]): DataFrame
def fill(value: String, cols: Seq[String]): DataFrame
def fill(value: String, cols: Array[String]): DataFrame
def fill(value: Long, cols: Seq[String]): DataFrame
def fill(value: Long, cols: Array[String]): DataFrame
def fill(value: Boolean, cols: Seq[String]): DataFrame
def fill(value: Boolean, cols: Array[String]): DataFrame
def fill(valueMap: Map[String, Any]): DataFrame
def replace[T](col: String, replacement: Map[T, T]): DataFrame
def replace[T](cols: Seq[String], replacement: Map[T, T]): DataFrame
```

---

### === DataFrameStatFunctions ===

```
def crosstab(col1: String, col2: String): DataFrame
def cov(col1: String, col2: String): Double
def corr(col1: String, col2: String): Double
def corr(col1: String, col2: String, method: String): Double
def approxQuantile(cols: Array[String], probabilities: Array[Double], relativeError: Double): Array[Array[Double]]
def approxQuantile(col: String, probabilities: Array[Double], relativeError: Double): Array[Double]
def freqItems(cols: Seq[String], support: Double): DataFrame
def freqItems(cols: Seq[String]): DataFrame
def sampleBy[T](col: Column, fractions: Map[T, Double], seed: Long): DataFrame
def sampleBy[T](col: String, fractions: Map[T, Double], seed: Long): DataFrame
```

---

### === Catalog ===

```
def currentDatabase: String
def setCurrentDatabase(dbName: String): Unit
def currentCatalog: String
def setCurrentCatalog(catalogName: String): Unit
def listDatabases(): DataFrame
def listDatabases(pattern: String): DataFrame
def listTables(): DataFrame
def listTables(dbName: String): DataFrame
def listTables(dbName: String, pattern: String): DataFrame
def listColumns(tableName: String): DataFrame
def listColumns(tableName: String, dbName: String): DataFrame
def listFunctions(): DataFrame
def listFunctions(dbName: String): DataFrame
def listFunctions(dbName: String, pattern: String): DataFrame
def listCatalogs(): DataFrame
def listCatalogs(pattern: String): DataFrame
def listCachedTables(): DataFrame
def listPartitions(tableName: String): DataFrame
def listViews(): DataFrame
def listViews(dbName: String): DataFrame
def listViews(dbName: String, pattern: String): DataFrame
def getDatabase(dbName: String): DataFrame
def getTable(tableName: String): DataFrame
def getTable(tableName: String, dbName: String): DataFrame
def getFunction(functionName: String): DataFrame
def getFunction(functionName: String, dbName: String): DataFrame
def getTableProperties(tableName: String): DataFrame
def getCreateTableString(tableName: String, asSerde: Boolean): String
def catalogExists(catalogName: String): Boolean
def databaseExists(dbName: String): Boolean
def tableExists(tableName: String): Boolean
def tableExists(tableName: String, dbName: String): Boolean
def functionExists(functionName: String): Boolean
def functionExists(functionName: String, dbName: String): Boolean
def isCached(tableName: String): Boolean
def cacheTable(tableName: String): Unit
def cacheTable(tableName: String, storageLevel: StorageLevel): Unit
def uncacheTable(tableName: String): Unit
def clearCache(): Unit
def createTable(tableName: String, source: String): DataFrame
def createTable(tableName: String, path: String, source: String): DataFrame
def createTable(tableName: String, source: String, options: Map[String, String]): DataFrame
def createTable(tableName: String, source: String, schema: types.StructType, options: Map[String, String]): DataFrame
def createTable(tableName: String, source: String, description: String, schema: types.StructType, options: Map[String, String]): DataFrame
def createExternalTable(tableName: String, path: String): DataFrame                // @deprecated
def createExternalTable(tableName: String, source: String, options: Map[String, String]): DataFrame   // @deprecated
def createExternalTable(tableName: String, path: String, source: String): DataFrame   // @deprecated
def createExternalTable(tableName: String, source: String, schema: types.StructType, options: Map[String, String]): DataFrame   // @deprecated
def createDatabase(dbName: String, ifNotExists: Boolean = true, properties: Map[String, String] = Map.empty): Unit
def dropDatabase(dbName: String, ifExists: Boolean = true, cascade: Boolean = false): Unit
def dropTempView(viewName: String): Boolean
def dropGlobalTempView(viewName: String): Boolean
def dropTable(tableName: String, ifExists: Boolean = false, purge: Boolean = false): Unit
def dropView(viewName: String, ifExists: Boolean = false): Unit
def refreshTable(tableName: String): Unit
def refreshByPath(path: String): Unit
def recoverPartitions(tableName: String): Unit
def truncateTable(tableName: String): Unit
def analyzeTable(tableName: String, noScan: Boolean = false): Unit
```

---

### === StreamingQuery ===

```
val id: String
val runId: String
val name: Option[String]
def isActive: Boolean
def awaitTermination(): Unit
def awaitTermination(timeoutMs: Long): Boolean
def stop(): Unit
def processAllAvailable(): Unit
def recentProgress: Seq[String]
def lastProgress: Option[String]
def status: StreamingQueryCommandResult.StatusResult
def explain(): Unit
def explain(extended: Boolean): Unit
def exception: Option[String]
```

---

### === StreamingQueryManager ===

```
def active: Seq[StreamingQuery]
def get(id: String): StreamingQuery
def awaitAnyTermination(): Unit
def awaitAnyTermination(timeoutMs: Long): Boolean
def resetTerminated(): Unit
def addListener(listener: StreamingQueryListener): Unit
def removeListener(listener: StreamingQueryListener): Unit
def listListeners(): Array[StreamingQueryListener]
def close(): Unit
```

---

### === Observation ===

```
val name: String
def this()                  // generates UUID name
val future: Future[Row]
def get: Map[String, Any]
```

### === object Observation ===

```
def apply(): Observation
def apply(name: String): Observation
```

---

### === StorageLevel (case class) ===

```
val useDisk: Boolean
val useMemory: Boolean
val useOffHeap: Boolean
val deserialized: Boolean
val replication: Int
```

### === object StorageLevel ===

```
val NONE: StorageLevel
val DISK_ONLY: StorageLevel
val DISK_ONLY_2: StorageLevel
val MEMORY_ONLY: StorageLevel
val MEMORY_ONLY_2: StorageLevel
val MEMORY_ONLY_SER: StorageLevel
val MEMORY_AND_DISK: StorageLevel
val MEMORY_AND_DISK_2: StorageLevel
val MEMORY_AND_DISK_SER: StorageLevel
val OFF_HEAP: StorageLevel
```

---

### === UserDefinedFunction ===

```
def withName(name: String): UserDefinedFunction
def asNonNullable(): UserDefinedFunction
def asNondeterministic(): UserDefinedFunction
def name: Option[String]
def apply(cols: Column*): Column
```

### === object UserDefinedFunction ===

```
def apply(func: AnyRef, returnType: DataType, inputTypes: Seq[DataType]): UserDefinedFunction
```

---

### === UDFRegistration ===

```
def register(name: String, udf: UserDefinedFunction): UserDefinedFunction
inline def register[R](name: String, func: () => R): UserDefinedFunction
inline def register[R, A1](name: String, func: A1 => R): UserDefinedFunction
inline def register[R, A1, A2](name: String, func: (A1, A2) => R): UserDefinedFunction
// ... (arities 0 through 10)
inline def register[R, A1, A2, A3, A4, A5, A6, A7, A8, A9, A10](name, func): UserDefinedFunction
```

---

### === TableValuedFunction ===

```
def range(end: Long): DataFrame
def range(start: Long, end: Long): DataFrame
def range(start: Long, end: Long, step: Long): DataFrame
def explode(collection: Column): DataFrame
def explode_outer(collection: Column): DataFrame
def posexplode(collection: Column): DataFrame
def posexplode_outer(collection: Column): DataFrame
def inline(input: Column): DataFrame
def inline_outer(input: Column): DataFrame
def json_tuple(input: Column, fields: Column*): DataFrame
def stack(n: Column, fields: Column*): DataFrame
def collations(): DataFrame
def sql_keywords(): DataFrame
def variant_explode(input: Column): DataFrame
def variant_explode_outer(input: Column): DataFrame
```

---

### === abstract class ForeachWriter[T] (extends Serializable) ===

```
def open(partitionId: Long, epochId: Long): Boolean
def process(value: T): Unit
def close(errorOrNull: Throwable): Unit
```

---

### === object implicits ===

```
class ColumnName(val name: String):
  def column: Column
  def boolean: StructField
  def byte: StructField
  def short: StructField
  def int: StructField
  def long: StructField
  def float: StructField
  def double: StructField
  def string: StructField
  def date: StructField
  def timestamp: StructField
  def binary: StructField
  def decimal: StructField
  def decimal(precision: Int, scale: Int): StructField

given Conversion[ColumnName, Column]
given Conversion[Symbol, Column]
extension (sc: StringContext) def $(args: Any*): ColumnName
extension (colName: String) def col: Column
extension [T: Encoder: ClassTag](seq: Seq[T]):
  def toDS(using spark: SparkSession): Dataset[T]
  def toDF(using spark: SparkSession): DataFrame
  def toDF(colNames: String*)(using spark: SparkSession): DataFrame
```

---

### === SparkException hierarchy ===

```
class SparkException(message, cause, errorClass, sqlState, messageParameters) extends Exception with Serializable
class AnalysisException extends SparkException
class ParseException extends AnalysisException
class StreamingQueryException extends SparkException
class SparkRuntimeException extends SparkException
class SparkUpgradeException extends SparkException
class SparkArithmeticException extends SparkException
class SparkNumberFormatException extends SparkException
class SparkIllegalArgumentException extends SparkException
class SparkUnsupportedOperationException extends SparkException
class SparkArrayIndexOutOfBoundsException extends SparkException
class SparkDateTimeException extends SparkException
class SparkNoSuchElementException extends SparkException
class NamespaceAlreadyExistsException extends AnalysisException
class TableAlreadyExistsException extends AnalysisException
class TempTableAlreadyExistsException extends AnalysisException
class NoSuchDatabaseException extends AnalysisException
class NoSuchNamespaceException extends AnalysisException
class NoSuchTableException extends AnalysisException
```

---

## org.apache.spark.sql.types

### === sealed trait DataType ===

```
def typeName: String
def simpleString: String
def sql: String
```

### Case objects:

```
BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType,
StringType, BinaryType, DateType, TimestampType, TimestampNTZType, NullType,
VariantType, DayTimeIntervalType, YearMonthIntervalType
```

### Case classes:

```
DecimalType(precision: Int, scale: Int) extends DataType
  object DecimalType { val DEFAULT: DecimalType }
ArrayType(elementType: DataType, containsNull: Boolean) extends DataType
MapType(keyType: DataType, valueType: DataType, valueContainsNull: Boolean) extends DataType
StructField(name: String, dataType: DataType, nullable: Boolean = true)
StructType(fields: Seq[StructField]) extends DataType:
  def apply(name: String): StructField
  def fieldNames: Array[String]
  def fieldIndex(name: String): Int
  def toDDL: String
  def treeString: String
  def treeString(maxLevel: Int): String
object StructType { val empty: StructType }
```

---

## org.apache.spark.sql.streaming

### === abstract class StreamingQueryListener (extends Serializable) ===

```
def onQueryStarted(event: QueryStartedEvent): Unit
def onQueryProgress(event: QueryProgressEvent): Unit
def onQueryIdle(event: QueryIdleEvent): Unit
def onQueryTerminated(event: QueryTerminatedEvent): Unit
```

Event types in companion:
```
case class QueryStartedEvent(id: String, runId: String, name: Option[String], timestamp: String)
case class QueryProgressEvent(json: String)
case class QueryIdleEvent(id: String, runId: String, timestamp: String)
case class QueryTerminatedEvent(id: String, runId: String, exception: Option[String], errorClassOnException: Option[String])
```

### === sealed trait OutputMode ===

```
case object Append extends OutputMode
case object Update extends OutputMode
case object Complete extends OutputMode
```

### === trait GroupState[S] (extends Serializable) ===

```
def exists: Boolean
def get: S
def getOption: Option[S]
def update(newState: S): Unit
def remove(): Unit
def hasTimedOut: Boolean
def setTimeoutDuration(durationMs: Long): Unit
def setTimeoutDuration(duration: String): Unit
def setTimeoutTimestamp(timestampMs: Long): Unit
def setTimeoutTimestamp(timestampMs: Long, additionalDuration: String): Unit
def setTimeoutTimestamp(timestamp: java.sql.Date): Unit
def setTimeoutTimestamp(timestamp: java.sql.Date, additionalDuration: String): Unit
def getCurrentWatermarkMs(): Long
def getCurrentProcessingTimeMs(): Long
```

### === sealed trait GroupStateTimeout ===

```
case object NoTimeout extends GroupStateTimeout
case object ProcessingTimeTimeout extends GroupStateTimeout
case object EventTimeTimeout extends GroupStateTimeout
```

### === sealed trait TimeMode ===

```
case object None extends TimeMode
case object ProcessingTime extends TimeMode
case object EventTime extends TimeMode
```

### === abstract class StatefulProcessor[K, I, O] (extends Serializable) ===

```
def init(outputMode: OutputMode, timeMode: TimeMode): Unit
def handleInputRows(key: K, inputRows: Iterator[I], timerValues: TimerValues): Iterator[O]
def handleExpiredTimer(key: K, timerValues: TimerValues, expiredTimerInfo: ExpiredTimerInfo): Iterator[O]
def close(): Unit
```

### === abstract class StatefulProcessorWithInitialState[K, I, O, S] (extends StatefulProcessor[K, I, O]) ===

```
def handleInitialState(key: K, initialState: S, timerValues: TimerValues): Unit
```

### === trait TimerValues ===

```
def getCurrentProcessingTimeInMs(): Long
def getCurrentWatermarkInMs(): Long
```

### === trait ExpiredTimerInfo ===

```
def getExpiryTimeInMs(): Long
def isValid(): Boolean
```

### === case class TTLConfig(ttlDuration: Duration) ===

```
object TTLConfig { val NONE: TTLConfig }
```

### === trait QueryInfo (extends Serializable) ===

```
def getQueryId(): UUID
def getRunId(): UUID
def getBatchId(): Long
```

### === State Variables ===

```
trait ValueState[S]:
  def exists(): Boolean
  def get(): S
  def update(newState: S): Unit
  def clear(): Unit

trait ListState[S]:
  def exists(): Boolean
  def get(): Iterator[S]
  def put(newState: Array[S]): Unit
  def appendValue(value: S): Unit
  def appendList(newState: Array[S]): Unit
  def clear(): Unit

trait MapState[K, V]:
  def exists(): Boolean
  def getValue(key: K): V
  def containsKey(key: K): Boolean
  def updateValue(key: K, value: V): Unit
  def iterator(): Iterator[(K, V)]
  def keys(): Iterator[K]
  def values(): Iterator[V]
  def removeKey(key: K): Unit
  def clear(): Unit
```

### === trait StatefulProcessorHandle ===

```
def getValueState[T](stateName: String)(using Encoder[T]): ValueState[T]
def getValueState[T](stateName: String, ttlConfig: TTLConfig)(using Encoder[T]): ValueState[T]
def getListState[T](stateName: String)(using Encoder[T]): ListState[T]
def getListState[T](stateName: String, ttlConfig: TTLConfig)(using Encoder[T]): ListState[T]
def getMapState[K, V](stateName: String)(using Encoder[K], Encoder[V]): MapState[K, V]
def getMapState[K, V](stateName: String, ttlConfig: TTLConfig)(using Encoder[K], Encoder[V]): MapState[K, V]
def registerTimer(expiryTimestampMs: Long): Unit
def deleteTimer(expiryTimestampMs: Long): Unit
def listTimers(): Iterator[Long]
def getQueryInfo(): QueryInfo
def deleteIfExists(stateName: String): Unit
```

### === StreamingQueryListenerBus ===

```
def close(): Unit
def append(listener: StreamingQueryListener): Unit
def remove(listener: StreamingQueryListener): Unit
def list(): Array[StreamingQueryListener]
```

---

## org.apache.spark.sql.expressions

### === abstract class Aggregator[-IN, BUF, OUT] (extends Serializable) ===

```
def zero: BUF
def reduce(b: BUF, a: IN): BUF
def merge(b1: BUF, b2: BUF): BUF
def finish(reduction: BUF): OUT
def bufferEncoder: Encoder[BUF]
def outputEncoder: Encoder[OUT]
def toColumn: TypedColumn[IN, OUT]
```

### === @deprecated object typed ===

```
def avg[IN](f: IN => Double): TypedColumn[IN, Double]
def count[IN](f: IN => Any): TypedColumn[IN, Long]
def sum[IN](f: IN => Double): TypedColumn[IN, Double]
def sumLong[IN](f: IN => Long): TypedColumn[IN, Long]
```

---

## org.apache.spark.sql.catalyst.encoders

### === trait AgnosticEncoder[T] (extends Serializable) ===

```
def isPrimitive: Boolean
def nullable: Boolean
def dataType: DataType
def lenientSerialization: Boolean
def clsTag: ClassTag[T]
```

### === object AgnosticEncoders ===

(All case objects/classes are private[sql] or internal; key encoder objects:)

```
PrimitiveBooleanEncoder, PrimitiveByteEncoder, PrimitiveShortEncoder,
PrimitiveIntEncoder, PrimitiveLongEncoder, PrimitiveFloatEncoder,
PrimitiveDoubleEncoder, BoxedBooleanEncoder, BoxedByteEncoder,
BoxedShortEncoder, BoxedIntEncoder, BoxedLongEncoder, BoxedFloatEncoder,
BoxedDoubleEncoder, StringEncoder, BinaryEncoder, NullEncoder,
UnboundRowEncoder, SparkDecimalEncoder, ScalaDecimalEncoder,
DateEncoder, LocalDateEncoder, TimestampEncoder, InstantEncoder,
LocalDateTimeEncoder, DayTimeIntervalEncoder, YearMonthIntervalEncoder,
VariantEncoder, JavaEnumEncoder, OptionEncoder, ArrayEncoder,
IterableEncoder, MapEncoder, ProductEncoder, RowEncoder
```

---

## org.apache.spark.sql.connect.client

### === SparkConnectClient ===

```
val sessionId: String
val userId: String
val artifactManager: ArtifactManager
def addTag(tag: String): Unit
def removeTag(tag: String): Unit
def getTags(): Set[String]
def clearTags(): Unit
def serverSideSessionId: Option[String]
def isChannelShutdown: Boolean
def execute(plan: Plan): Iterator[ExecutePlanResponse]
def executeCommand(cmd: Command): Unit
def executeCommandWithResponses(cmd: Command): Seq[ExecutePlanResponse]
def analyzePlan(f: AnalyzePlanRequest.Builder => Unit): AnalyzePlanResponse
def analyzeSchema(plan: Plan): AnalyzePlanResponse
def analyzeExplain(plan: Plan, mode: ExplainMode): String
def getConfig(key: String): String
def getConfigOption(key: String): Option[String]
def getAllConfig(): Map[String, String]
def setConfig(key: String, value: String): Unit
def unsetConfig(key: String): Unit
def isModifiableConfig(key: String): Boolean
def version(): String
def close(): Unit
def newClient(): SparkConnectClient
def cloneSession(): SparkConnectClient
def interruptAll(): Seq[String]
def interruptTag(tag: String): Seq[String]
def interruptOperation(operationId: String): Seq[String]
```

### === object SparkConnectClient ===

```
def create(url: String, configs: Map[String, String] = Map.empty): SparkConnectClient
```

### === RetryPolicy (case class) ===

```
val maxRetries: Int
val initialBackoffMs: Long
val maxBackoffMs: Long
val backoffMultiplier: Double
val jitterMs: Long
val canRetry: Throwable => Boolean
```

### === object RetryPolicy ===

```
def defaultCanRetry(e: Throwable): Boolean
def defaultPolicy(): RetryPolicy
```

---

## org.apache.spark.sql.connect.common

### === UdfPacket (extends Serializable) ===

```
val function: AnyRef
val inputEncoders: Seq[AgnosticEncoder[?]]
val outputEncoder: AgnosticEncoder[?]
```

### === object UdfPacket ===

```
def serialize(packet: UdfPacket): Array[Byte]
```

---

## org.apache.spark.sql.application

### === object ConnectRepl ===

```
def main(args: Array[String]): Unit
```

---

## object functions (~870 public def signatures)

All return `Column` unless otherwise noted. Organized by category:

### Column references
```
col, column, lit, expr
```

### Sort
```
asc, asc_nulls_first, asc_nulls_last, desc, desc_nulls_first, desc_nulls_last
```

### Aggregate functions
```
count, sum, avg, mean, min, max, first, last, countDistinct, count_distinct,
collect_list, collect_set, any_value, count_if, product, every, some, bool_and,
bool_or, bit_and, bit_or, bit_xor, first_value, last_value, max_by, min_by,
median, mode, percentile, any, std, sum_distinct, listagg, listagg_distinct,
string_agg, string_agg_distinct, histogram_numeric, count_min_sketch,
regr_avgx, regr_avgy, regr_count, regr_intercept, regr_r2, regr_slope,
regr_sxx, regr_sxy, regr_syy, approx_percentile, bitmap_construct_agg,
bitmap_or_agg, bitmap_and_agg, hll_sketch_agg, hll_union_agg,
approxCountDistinct(col), approxCountDistinct(col, rsd),
approxCountDistinct(colName), approxCountDistinct(colName, rsd),
approx_count_distinct(col), approx_count_distinct(col, rsd),
approx_count_distinct(colName), approx_count_distinct(colName, rsd),
stddev, stddev_samp, stddev_pop, variance, var_samp, var_pop,
kurtosis, skewness, corr, covar_pop, covar_samp, grouping, grouping_id,
percentile_approx
```

### Math functions
```
abs, sqrt, pow, power, round, floor, ceil, ceiling, log, ln, log10, log2,
exp, greatest, least, rand, randn, log1p, expm1, hypot, pmod, sign, e, pi,
width_bucket, negative, positive, try_mod, random, uniform,
sin, cos, tan, asin, acos, atan, atan2, sinh, cosh, tanh,
cbrt, rint, bround, hex, unhex, conv, signum, factorial,
try_add, try_subtract, try_multiply, try_divide, try_avg, try_sum,
try_to_number, try_aes_decrypt, try_reflect
```

### String functions
```
concat, concat_ws, upper, lower, trim, ltrim, rtrim, substring, length,
replace, lpad, rpad, left, right, char_length, bit_length, octet_length,
contains, startswith, endswith, btrim, position, sentences, char, chr,
character_length, substr, substring_index, split_part, find_in_set, elt,
regexp_count, regexp_extract_all, regexp_instr, regexp_substr, regexp,
regexp_like, rlike, like, ilike, collate, collation, to_binary, try_to_binary,
to_char, to_varchar, to_number, mask, randstr, quote, printf, lcase, ucase,
len, is_valid_utf8, make_valid_utf8, validate_utf8, try_validate_utf8,
regexp_extract, regexp_replace, split, initcap, soundex, levenshtein,
ascii, base64, unbase64, decode, encode, format_number, format_string,
instr, locate, overlay, repeat, translate
```

### Date/Time functions
```
current_date, current_timestamp, year, month, dayofmonth, hour, minute, second,
date_add, date_sub, to_date, to_timestamp, date_format, curdate, current_time,
localtimestamp, date_diff, dateadd, datepart, day, dayname, monthname, weekday,
convert_timezone, dayofweek, dayofyear, weekofyear, quarter, last_day, next_day,
months_between, datediff, add_months, from_unixtime, unix_timestamp,
from_utc_timestamp, to_utc_timestamp, window, date_trunc, trunc,
make_date, make_timestamp, date_part, extract, timestamp_seconds,
timestamp_millis, timestamp_micros, date_from_unix_date, current_timezone, now,
make_dt_interval, make_interval, make_time, make_ym_interval,
make_timestamp_ltz, make_timestamp_ntz, to_timestamp_ltz, to_timestamp_ntz,
to_time, to_unix_timestamp, try_to_timestamp, session_window
```

### Null/Conditional
```
coalesce, isnull, isnan, isnotnull, assert_true, raise_error, when,
nanvl, ifnull, nullif, nvl, nvl2
```

### Collection functions
```
array, struct, explode, explode_outer, posexplode, posexplode_outer, size,
array_contains, array_sort, array_distinct, array_intersect, array_union,
array_except, array_join, array_max, array_min, array_position, array_remove,
array_repeat, arrays_zip, flatten, element_at, slice, reverse, shuffle,
sort_array, array_append, array_prepend, array_compact, array_insert,
arrays_overlap, sequence, array_size, get, map_contains_key, str_to_map,
try_element_at, cardinality, named_struct,
map, map_from_arrays, map_from_entries, map_keys, map_values, map_entries,
map_concat, map_filter, map_zip_with, aggregate, transform, filter (collection),
exists (collection), zip_with, forall, transform_keys, transform_values
```

### JSON functions
```
from_json (12 overloads), to_json, json_tuple, get_json_object,
schema_of_json, json_array_length, json_object_keys
```

### CSV functions
```
from_csv, to_csv, schema_of_csv
```

### XML functions
```
xpath, xpath_string, from_xml, to_xml, schema_of_xml,
xpath_boolean, xpath_double, xpath_number, xpath_float, xpath_int,
xpath_long, xpath_short
```

### Hash/Crypto functions
```
md5, sha1, sha2, crc32, hash, xxhash64, murmur3, aes_encrypt, aes_decrypt,
try_aes_decrypt
```

### Bitwise functions
```
shiftLeft, shiftRight, shiftRightUnsigned, bitwise_not, bit_count,
bitmap_bit_position, bitmap_bucket_number, bitmap_count
```

### Conversion/Casting
```
typedLit, negate, not, bitwiseNOT
```

### Window functions
```
row_number, rank, dense_rank, percent_rank, cume_dist, ntile, lag, lead,
nth_value, monotonically_increasing_id, spark_partition_id
```

### UDF construction
```
udf (inline, arities 0-10): Column => UserDefinedFunction
udaf[IN, BUF, OUT](agg: Aggregator[IN, BUF, OUT]): UserDefinedFunction
call_udf(udfName: String, cols: Column*): Column
call_function(funcName: String, cols: Column*): Column
```

### Variant functions
```
parse_json, variant_get, is_variant_null, schema_of_variant,
schema_of_variant_agg, try_variant_get, variant_explode,
variant_explode_outer, to_variant_object
```

### Misc functions
```
input_file_name, broadcast, typedlit, struct, negate, not, bitwiseNOT,
map_from_entries, create_map, make_interval, make_dt_interval, named_struct,
java_method, reflect, try_reflect, typeof, stack, inline, inline_outer,
posexplode, posexplode_outer, collation, collations, sql_keywords,
version, current_user, user, current_schema, current_database,
current_catalog, assert_true, raise_error, uuid, monotonically_increasing_id,
spark_partition_id, input_file_name, current_timezone
```

### Higher-order functions
```
transform(col, f), transform(col, f_with_idx), filter(col, f),
exists(col, f), forall(col, f), aggregate(col, initialValue, merge),
aggregate(col, initialValue, merge, finish), zip_with(left, right, f),
transform_keys(col, f), transform_values(col, f), map_filter(col, f),
map_zip_with(left, right, f), array_sort(col, comparator)
```

---

## Internal / Private Files (not part of public API)

The following files contain only `private[sql]` or `private[client]` members:

- `ArrowSerializer.scala` — private[sql] object
- `Artifact.scala` — private[sql] class and object
- `connect/client/ArrowDeserializer.scala` — private[sql] object
- `connect/client/ArtifactManager.scala` — private[sql] class
- `connect/client/ClassFinder.scala` — trait ClassFinder (private[sql])
- `connect/client/ClosureCleaner.scala` — private[sql] object
- `connect/client/DataTypeProtoConverter.scala` — private[sql] object
- `connect/client/ExecutePlanResponseReattachableIterator.scala` — private[sql] class
- `connect/client/GrpcRetryHandler.scala` — private[client] class
- `connect/client/ResponseValidator.scala` — private[client] class
- `connect/client/SparkConnectStubState.scala` — private[client] class
- `connect/SessionCleaner.scala` — private[sql] class
- `connect/common/ForeachWriterPacket.scala` — private[sql] class
- `connect/common/LiteralValueProtoConverter.scala` — private[sql] object
- `connect/common/UdfAdaptors.scala` — private[sql] / Serializable adaptor classes
- `connect/common/FunctionAdaptors.scala` — FilterAdaptor, FlatMapAdaptor, MapPartitionsAdaptor, etc.
- `expressions/ReduceAggregator.scala` — private[sql] class

---

## Summary Statistics

| Category | Count |
|----------|-------|
| Total .scala files | 69 |
| Public classes/objects/traits | ~45 |
| Public methods (non-functions.scala) | ~650 |
| functions.scala public defs | ~870 |
| **Total public API surface** | **~1520** |
