package org.apache.spark.sql

import com.google.protobuf.ByteString
import org.apache.spark.connect.proto.{
  CommonInlineUserDefinedFunction,
  Expression,
  Join,
  MapPartitions,
  Relation,
  RelationCommon,
  ScalarScalaUDF,
  SubqueryExpression
}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.ProductEncoder
import org.apache.spark.sql.connect.client.DataTypeProtoConverter
import org.apache.spark.sql.connect.common.UdfPacket

import scala.reflect.ClassTag

/** A strongly-typed collection of domain-specific objects.
  *
  * Dataset[T] wraps a DataFrame and uses an Encoder[T] (derived at compile time via Scala 3
  * Mirrors) to convert between Rows and typed objects.
  *
  * {{{
  *   case class Person(name: String, age: Int) derives Encoder
  *   val ds: Dataset[Person] = df.as[Person]
  *   val names: Dataset[String] = ds.map(_.name)
  * }}}
  */
final class Dataset[T: ClassTag] private[sql] (
    private[sql] val df: DataFrame,
    private[sql] val encoder: Encoder[T]
):

  /** The underlying SparkSession. */
  def sparkSession: SparkSession = df.session

  /** The schema of this Dataset. */
  def schema: types.StructType = df.schema

  /** Return the underlying untyped DataFrame. */
  def toDF(): DataFrame = df

  /** Rename columns. */
  def toDF(colNames: String*): DataFrame = df.toDF(colNames*)

  // ---------------------------------------------------------------------------
  // Typed Transformations
  // ---------------------------------------------------------------------------

  /** Return a new Dataset by applying a function to each partition's iterator.
    *
    * When AgnosticEncoder bridges are available for both input and output types, this builds a
    * server-side `MapPartitions` proto relation. Otherwise it falls back to client-side
    * collect-transform-reupload.
    */
  def mapPartitions[U: Encoder: ClassTag](func: Iterator[T] => Iterator[U]): Dataset[U] =
    val outEnc = summon[Encoder[U]]
    val inAg = encoder.agnosticEncoder
    val outAg = outEnc.agnosticEncoder
    if inAg == null || outAg == null then return mapPartitionsLocal(func, outEnc)
    // Build server-side MapPartitions
    val udfProto = buildMapPartitionsUdf(func, inAg, outAg)
    val relation = Relation
      .newBuilder()
      .setCommon(
        RelationCommon.newBuilder().setPlanId(sparkSession.nextPlanId()).build()
      )
      .setMapPartitions(
        MapPartitions
          .newBuilder()
          .setInput(df.relation)
          .setFunc(udfProto)
          .build()
      )
      .build()
    val newDf = DataFrame(sparkSession, relation)
    Dataset(newDf, outEnc)

  /** Return a new Dataset containing only rows matching the predicate. */
  def filter(func: T => Boolean): Dataset[T] =
    mapPartitions(iter => iter.filter(func))(using encoder, summon[ClassTag[T]])

  /** Return a new Dataset by applying a function to each element. */
  def map[U: Encoder: ClassTag](func: T => U): Dataset[U] =
    mapPartitions(iter => iter.map(func))

  /** Return a new Dataset by applying a function that returns a sequence. */
  def flatMap[U: Encoder: ClassTag](func: T => IterableOnce[U]): Dataset[U] =
    mapPartitions(iter => iter.flatMap(func))

  /** Apply a function to each element. */
  def foreach(func: T => Unit): Unit =
    collect().foreach(func)

  /** Apply a function to each partition. */
  def foreachPartition(func: Iterator[T] => Unit): Unit =
    func(collect().iterator)

  /** Return a new Dataset with distinct elements. */
  def distinct(): Dataset[T] = Dataset(df.distinct(), encoder)

  /** Drop duplicates within watermark for streaming. */
  def dropDuplicatesWithinWatermark(): Dataset[T] =
    Dataset(df.dropDuplicatesWithinWatermark(), encoder)

  def dropDuplicatesWithinWatermark(colNames: Seq[String]): Dataset[T] =
    Dataset(df.dropDuplicatesWithinWatermark(colNames), encoder)

  def dropDuplicatesWithinWatermark(col1: String, cols: String*): Dataset[T] =
    Dataset(df.dropDuplicatesWithinWatermark(col1, cols*), encoder)

  /** Reduce the elements using the given associative binary operator.
    *
    * Falls back to client-side collect when AgnosticEncoder is not available.
    */
  def reduce(func: (T, T) => T): T =
    // reduce is always collected client-side — server-side Aggregate + reduce UDF
    // requires TypedReduceAggregator which is not yet available in proto.
    // This is the same approach as Spark's own Connect client for reduce.
    val data = collect()
    if data.isEmpty then
      throw UnsupportedOperationException("empty collection")
    data.reduce(func)

  /** Group the Dataset by a key-extracting function, returning a KeyValueGroupedDataset. */
  def groupByKey[K: Encoder: ClassTag](func: T => K): KeyValueGroupedDataset[K, T] =
    KeyValueGroupedDataset(this, func)(
      using
      summon[Encoder[K]],
      summon[ClassTag[K]],
      encoder,
      summon[ClassTag[T]]
    )

  // ---------------------------------------------------------------------------
  // Untyped Transformations (delegate to DataFrame)
  // ---------------------------------------------------------------------------

  def select(cols: Column*): DataFrame = df.select(cols*)

  // ---------------------------------------------------------------------------
  // Typed select (TypedColumn 1-5 arity)
  // ---------------------------------------------------------------------------

  /** Type-safe select with a single TypedColumn. */
  def select[U1: ClassTag](c1: TypedColumn[T, U1]): Dataset[U1] =
    Dataset(df.select(c1), c1.encoder)

  /** Type-safe select with two TypedColumns, returning a Dataset of tuples. */
  def select[U1, U2](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2]
  ): Dataset[(U1, U2)] =
    given enc: Encoder[(U1, U2)] = Encoders.tuple(c1.encoder, c2.encoder)
    Dataset(df.select(c1, c2), enc)(using scala.reflect.classTag[(U1, U2)])

  /** Type-safe select with three TypedColumns, returning a Dataset of tuples. */
  def select[U1, U2, U3](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3]
  ): Dataset[(U1, U2, U3)] =
    given enc: Encoder[(U1, U2, U3)] = Encoders.tuple(c1.encoder, c2.encoder, c3.encoder)
    Dataset(df.select(c1, c2, c3), enc)(using scala.reflect.classTag[(U1, U2, U3)])

  /** Type-safe select with four TypedColumns, returning a Dataset of tuples. */
  def select[U1, U2, U3, U4](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3],
      c4: TypedColumn[T, U4]
  ): Dataset[(U1, U2, U3, U4)] =
    given enc: Encoder[(U1, U2, U3, U4)] =
      Encoders.tuple(c1.encoder, c2.encoder, c3.encoder, c4.encoder)
    Dataset(df.select(c1, c2, c3, c4), enc)(using scala.reflect.classTag[(U1, U2, U3, U4)])

  /** Type-safe select with five TypedColumns, returning a Dataset of tuples. */
  def select[U1, U2, U3, U4, U5](
      c1: TypedColumn[T, U1],
      c2: TypedColumn[T, U2],
      c3: TypedColumn[T, U3],
      c4: TypedColumn[T, U4],
      c5: TypedColumn[T, U5]
  ): Dataset[(U1, U2, U3, U4, U5)] =
    given enc: Encoder[(U1, U2, U3, U4, U5)] =
      Encoders.tuple(c1.encoder, c2.encoder, c3.encoder, c4.encoder, c5.encoder)
    Dataset(df.select(c1, c2, c3, c4, c5), enc)(
      using scala.reflect.classTag[(U1, U2, U3, U4, U5)]
    )

  def filter(condition: Column): Dataset[T] = Dataset(df.filter(condition), encoder)

  def where(condition: Column): Dataset[T] = filter(condition)

  def limit(n: Int): Dataset[T] = Dataset(df.limit(n), encoder)

  def orderBy(cols: Column*): Dataset[T] = Dataset(df.orderBy(cols*), encoder)

  def sort(cols: Column*): Dataset[T] = orderBy(cols*)

  def sort(sortCol: String, sortCols: String*): Dataset[T] =
    orderBy((sortCol +: sortCols).map(Column(_))*)

  def orderBy(sortCol: String, sortCols: String*): Dataset[T] =
    orderBy((sortCol +: sortCols).map(Column(_))*)

  /** Sample a fraction of rows. */
  def sample(withReplacement: Boolean, fraction: Double, seed: Long): Dataset[T] =
    Dataset(df.sample(fraction, withReplacement, seed), encoder)

  def sample(withReplacement: Boolean, fraction: Double): Dataset[T] =
    sample(withReplacement, fraction, 0L)

  def sample(fraction: Double, seed: Long): Dataset[T] =
    Dataset(df.sample(fraction, seed = seed), encoder)

  def sample(fraction: Double): Dataset[T] =
    Dataset(df.sample(fraction), encoder)

  /** Repartition by number of partitions. */
  def repartition(numPartitions: Int): Dataset[T] = Dataset(df.repartition(numPartitions), encoder)

  /** Repartition by columns. */
  def repartition(numPartitions: Int, cols: Column*): Dataset[T] =
    Dataset(df.repartition(numPartitions, cols*), encoder)

  /** Repartition by columns with default partition count. */
  def repartition(cols: Column*)(using DummyImplicit): Dataset[T] =
    Dataset(df.repartition(cols*), encoder)

  /** Coalesce to fewer partitions. */
  def coalesce(numPartitions: Int): Dataset[T] = Dataset(df.coalesce(numPartitions), encoder)

  /** Sort within each partition. */
  def sortWithinPartitions(cols: Column*): Dataset[T] =
    Dataset(df.sortWithinPartitions(cols*), encoder)

  def sortWithinPartitions(colNames: String*)(using DummyImplicit): Dataset[T] =
    Dataset(df.sortWithinPartitions(colNames*), encoder)

  def groupBy(cols: Column*): GroupedDataFrame = df.groupBy(cols*)

  def join(right: DataFrame, joinExpr: Column, joinType: String = "inner"): DataFrame =
    df.join(right, joinExpr, joinType)

  /** Type-preserving join that returns a Dataset of pairs.
    *
    * {{{
    *   val ds1: Dataset[Person] = ...
    *   val ds2: Dataset[Department] = ...
    *   val joined: Dataset[(Person, Department)] = ds1.joinWith(ds2, ds1("deptId") === ds2("id"))
    * }}}
    */
  def joinWith[U: Encoder: ClassTag](
      other: Dataset[U],
      condition: Column,
      joinType: String = "inner"
  ): Dataset[(T, U)] =
    val jt = df.toJoinType(joinType)

    // Determine if the left/right schemas are structs for JoinDataType
    val isLeftStruct = encoder.schema.fields.length > 1 ||
      (encoder.agnosticEncoder != null && encoder.agnosticEncoder.isInstanceOf[ProductEncoder[?]])
    val isRightStruct = other.encoder.schema.fields.length > 1 ||
      (other.encoder.agnosticEncoder != null &&
        other.encoder.agnosticEncoder
          .isInstanceOf[ProductEncoder[?]])

    val joinBuilder = Join.newBuilder()
      .setLeft(df.relation)
      .setRight(other.df.relation)
      .setJoinCondition(condition.expr)
      .setJoinType(jt)
      .setJoinDataType(
        Join.JoinDataType.newBuilder()
          .setIsLeftStruct(isLeftStruct)
          .setIsRightStruct(isRightStruct)
          .build()
      )

    val newRelation = Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(df.session.nextPlanId()).build())
      .setJoin(joinBuilder.build())
      .build()

    val tupleEnc = Encoders.tuple(encoder, other.encoder)
    Dataset(DataFrame(df.session, newRelation), tupleEnc)

  def observe(name: String, expr: Column, exprs: Column*): Dataset[T] =
    Dataset(df.observe(name, expr, exprs*), encoder)

  def observe(observation: Observation, expr: Column, exprs: Column*): Dataset[T] =
    Dataset(df.observe(observation, expr, exprs*), encoder)

  def withColumn(name: String, col: Column): DataFrame = df.withColumn(name, col)

  def withColumnRenamed(existing: String, newName: String): DataFrame =
    df.withColumnRenamed(existing, newName)

  def withColumnsRenamed(colsMap: Map[String, String]): DataFrame =
    df.withColumnsRenamed(colsMap)

  def withColumns(colsMap: Map[String, Column]): DataFrame =
    df.withColumns(colsMap)

  /** Select columns matching a regex pattern. */
  def colRegex(colName: String): Column = df.colRegex(colName)

  /** Access a metadata column by name. */
  def metadataColumn(colName: String): Column = df.metadataColumn(colName)

  def drop(colNames: String*): DataFrame = df.drop(colNames*)

  def drop(cols: Column*)(using DummyImplicit): DataFrame = df.drop(cols*)

  def dropDuplicates(): Dataset[T] = Dataset(df.dropDuplicates(), encoder)

  def dropDuplicates(colNames: Seq[String]): Dataset[T] =
    Dataset(df.dropDuplicates(colNames), encoder)

  def describe(colNames: String*): DataFrame = df.describe(colNames*)

  def summary(statistics: String*): DataFrame = df.summary(statistics*)

  def union(other: Dataset[T]): Dataset[T] = Dataset(df.union(other.df), encoder)

  def unionByName(other: Dataset[T], allowMissingColumns: Boolean = false): Dataset[T] =
    Dataset(df.unionByName(other.df, allowMissingColumns), encoder)

  def intersect(other: Dataset[T]): Dataset[T] = Dataset(df.intersect(other.df), encoder)

  def intersectAll(other: Dataset[T]): Dataset[T] = Dataset(df.intersectAll(other.df), encoder)

  def except(other: Dataset[T]): Dataset[T] = Dataset(df.except(other.df), encoder)

  def exceptAll(other: Dataset[T]): Dataset[T] = Dataset(df.exceptAll(other.df), encoder)

  /** Set an alias for this Dataset (useful for self-joins). */
  def as(alias: String): Dataset[T] = Dataset(df.alias(alias), encoder)

  /** Set an alias for this Dataset. */
  def alias(alias: String): Dataset[T] = as(alias)

  /** Pipeline-style transformation. */
  def transform[U](t: Dataset[T] => Dataset[U]): Dataset[U] = t(this)

  def cache(): Dataset[T] =
    df.cache()
    this

  def persist(): Dataset[T] =
    df.persist()
    this

  def unpersist(blocking: Boolean = false): Dataset[T] =
    df.unpersist(blocking)
    this

  // ---------------------------------------------------------------------------
  // Actions
  // ---------------------------------------------------------------------------

  /** Collect all rows and convert to typed array. */
  def collect(): Array[T] =
    df.collect().map(encoder.fromRow)

  /** Collect all rows as a Java List. */
  def collectAsList(): java.util.List[T] = java.util.Arrays.asList(collect()*)

  /** Return the first n elements as a Java List. */
  def takeAsList(n: Int): java.util.List[T] = java.util.Arrays.asList(take(n)*)

  /** Return the first element. */
  def first(): T = encoder.fromRow(df.first())

  /** Return the first n elements. */
  def head(n: Int = 1): Array[T] = df.head(n).map(encoder.fromRow)

  /** Return the first n elements. */
  def take(n: Int): Array[T] = head(n)

  /** Count the number of rows. */
  def count(): Long = df.count()

  /** Show the first numRows. */
  def show(numRows: Int = 20, truncate: Int = 20): Unit = df.show(numRows, truncate)

  /** Show the first numRows with optional vertical format. */
  def show(numRows: Int, truncate: Int, vertical: Boolean): Unit =
    df.show(numRows, truncate, vertical)

  /** Return a Dataset[String] with each row converted to a JSON string. */
  def toJSON: Dataset[String] =
    Dataset(df.toJSON, summon[Encoder[String]])

  def isEmpty: Boolean = df.isEmpty

  /** Column names. */
  def columns: Array[String] = df.columns

  /** Array of (column name, data type string) pairs. */
  def dtypes: Array[(String, String)] = df.dtypes

  /** Select a column by name, bound to this Dataset's plan. */
  def col(colName: String): Column = df.col(colName)

  /** Select a column by name — alias for `col`. */
  def apply(colName: String): Column = col(colName)

  /** Explain the query plan. */
  def explain(extended: Boolean = false): Unit = df.explain(extended)
  def explain(mode: String): Unit = df.explain(mode)

  /** Print the schema tree. */
  def printSchema(): Unit = df.printSchema()
  def printSchema(level: Int): Unit = df.printSchema(level)

  /** Check if this Dataset is a streaming query. */
  def isStreaming: Boolean = df.isStreaming

  /** Check if collect/take can be run locally. */
  def isLocal: Boolean = df.isLocal

  /** Input files used to create this Dataset. */
  def inputFiles: Array[String] = df.inputFiles

  /** Compare semantic equivalence of two DataFrames. */
  def sameSemantics(other: Dataset[T]): Boolean = df.sameSemantics(other.df)

  /** Return a semantic hash of the logical plan. */
  def semanticHash: Int = df.semanticHash

  /** Get the storage level. */
  def storageLevel: StorageLevel = df.storageLevel

  /** Checkpoint this Dataset. */
  def checkpoint(eager: Boolean = true): Dataset[T] = Dataset(df.checkpoint(eager), encoder)

  /** Locally checkpoint this Dataset. */
  def localCheckpoint(eager: Boolean = true): Dataset[T] =
    Dataset(df.localCheckpoint(eager), encoder)

  /** Randomly split into multiple Datasets. */
  def randomSplit(weights: Array[Double], seed: Long = 0L): Array[Dataset[T]] =
    df.randomSplit(weights, seed).map(f => Dataset(f, encoder))

  /** Return the last n elements. */
  def tail(n: Int): Array[T] = df.tail(n).map(encoder.fromRow)

  /** Access DataFrameNaFunctions. */
  def na: DataFrameNaFunctions = df.na

  /** Access DataFrameStatFunctions. */
  def stat: DataFrameStatFunctions = df.stat

  /** Hint the optimizer. */
  def hint(name: String, parameters: Any*): Dataset[T] =
    Dataset(df.hint(name, parameters*), encoder)

  /** Cross join with another DataFrame. */
  def crossJoin(right: DataFrame): DataFrame = df.crossJoin(right)

  /** Access the V2 writer. */
  def writeTo(table: String): DataFrameWriterV2 = df.writeTo(table)

  /** Access the streaming writer. */
  def writeStream: DataStreamWriter = df.writeStream

  /** Define a watermark for streaming aggregations. */
  def withWatermark(eventTime: String, delayThreshold: String): Dataset[T] =
    Dataset(df.withWatermark(eventTime, delayThreshold), encoder)

  /** Persist with a specific storage level. */
  def persist(storageLevel: StorageLevel): Dataset[T] =
    df.persist(storageLevel)
    this

  def createGlobalTempView(viewName: String): Unit = df.createGlobalTempView(viewName)
  def createOrReplaceGlobalTempView(viewName: String): Unit =
    df.createOrReplaceGlobalTempView(viewName)

  /** Return a `java.util.Iterator` that iterates typed elements lazily.
    *
    * The returned iterator implements `AutoCloseable` — callers should close it after use.
    */
  def toLocalIterator(): java.util.Iterator[T] with AutoCloseable =
    val javaIter = df.toLocalIterator()
    new java.util.Iterator[T] with AutoCloseable:
      def hasNext: Boolean = javaIter.hasNext
      def next(): T = encoder.fromRow(javaIter.next())
      def close(): Unit = javaIter.close()

  // ---------------------------------------------------------------------------
  // Subquery expressions
  // ---------------------------------------------------------------------------

  /** Return this Dataset as a scalar subquery Column.
    *
    * The Dataset must return exactly one row and one column.
    * {{{
    *   val maxId = ds.select(functions.max("id")).scalar()
    *   df.filter(Column("id") === maxId)
    * }}}
    */
  def scalar(): Column =
    val rel = df.relation
    val planId = rel.getCommon.getPlanId
    Column(
      Expression.newBuilder()
        .setSubqueryExpression(
          SubqueryExpression.newBuilder()
            .setPlanId(planId)
            .setSubqueryType(SubqueryExpression.SubqueryType.SUBQUERY_TYPE_SCALAR)
            .build()
        )
        .build(),
      Seq(rel)
    )

  /** Return this Dataset as an EXISTS subquery Column.
    *
    * {{{
    *   val hasMatch = matchDs.exists()
    *   df.filter(hasMatch)
    * }}}
    */
  def exists(): Column =
    val rel = df.relation
    val planId = rel.getCommon.getPlanId
    Column(
      Expression.newBuilder()
        .setSubqueryExpression(
          SubqueryExpression.newBuilder()
            .setPlanId(planId)
            .setSubqueryType(SubqueryExpression.SubqueryType.SUBQUERY_TYPE_EXISTS)
            .build()
        )
        .build(),
      Seq(rel)
    )

  // ---------------------------------------------------------------------------
  // Convert to another type
  // ---------------------------------------------------------------------------

  /** Convert this Dataset to a Dataset of another type. */
  def as[U: Encoder: ClassTag]: Dataset[U] = Dataset(df, summon[Encoder[U]])

  // ---------------------------------------------------------------------------
  // Temp Views
  // ---------------------------------------------------------------------------

  def createTempView(viewName: String): Unit = df.createTempView(viewName)
  def createOrReplaceTempView(viewName: String): Unit = df.createOrReplaceTempView(viewName)

  // ---------------------------------------------------------------------------
  // Writer
  // ---------------------------------------------------------------------------

  def write: DataFrameWriter = df.write

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Create a new Dataset[T] from a local Seq[T] using the current session. */
  private def createFromSeq(data: Seq[T]): Dataset[T] =
    val rows = data.map(encoder.toRow)
    val newDf = sparkSession.createDataFrame(rows, encoder.schema)
    Dataset(newDf, encoder)

  /** Create a new Dataset[U] from a local Seq[U]. */
  private def createFromSeqAs[U: ClassTag](data: Seq[U], enc: Encoder[U]): Dataset[U] =
    val rows = data.map(enc.toRow)
    val newDf = sparkSession.createDataFrame(rows, enc.schema)
    Dataset(newDf, enc)

  /** Client-side fallback for mapPartitions when AgnosticEncoder is not available. */
  private def mapPartitionsLocal[U: ClassTag](
      func: Iterator[T] => Iterator[U],
      outEnc: Encoder[U]
  ): Dataset[U] =
    val data = func(collect().iterator).toSeq
    val rows = data.map(outEnc.toRow)
    val newDf = sparkSession.createDataFrame(rows, outEnc.schema)
    Dataset(newDf, outEnc)

  /** Build a CommonInlineUserDefinedFunction proto for MapPartitions. */
  private def buildMapPartitionsUdf(
      func: AnyRef,
      inputEncoder: AgnosticEncoder[?],
      outputEncoder: AgnosticEncoder[?]
  ): CommonInlineUserDefinedFunction =
    val packet = UdfPacket(func, Seq(inputEncoder), outputEncoder)
    val payload = UdfPacket.serialize(packet)
    val scalaUdf = ScalarScalaUDF
      .newBuilder()
      .setPayload(ByteString.copyFrom(payload))
      .setOutputType(
        DataTypeProtoConverter.toProto(outputEncoder.dataType)
      )
      .setNullable(true)
    CommonInlineUserDefinedFunction
      .newBuilder()
      .setFunctionName("mapPartitions")
      .setDeterministic(true)
      .setScalarScalaUdf(scalaUdf.build())
      .build()

  override def toString: String = df.toString

object Dataset:
  private[sql] def apply[T: ClassTag](df: DataFrame, encoder: Encoder[T]): Dataset[T] =
    new Dataset(df, encoder)
