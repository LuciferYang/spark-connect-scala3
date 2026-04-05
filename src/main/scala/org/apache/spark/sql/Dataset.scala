package org.apache.spark.sql

import com.google.protobuf.ByteString
import org.apache.spark.connect.proto.{
  CommonInlineUserDefinedFunction,
  Expression,
  MapPartitions,
  Relation,
  RelationCommon,
  ScalarScalaUDF
}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
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

  def filter(condition: Column): Dataset[T] = Dataset(df.filter(condition), encoder)

  def where(condition: Column): Dataset[T] = filter(condition)

  def limit(n: Int): Dataset[T] = Dataset(df.limit(n), encoder)

  def orderBy(cols: Column*): Dataset[T] = Dataset(df.orderBy(cols*), encoder)

  def sort(cols: Column*): Dataset[T] = orderBy(cols*)

  def groupBy(cols: Column*): GroupedDataFrame = df.groupBy(cols*)

  def join(right: DataFrame, joinExpr: Column, joinType: String = "inner"): DataFrame =
    df.join(right, joinExpr, joinType)

  def withColumn(name: String, col: Column): DataFrame = df.withColumn(name, col)

  def drop(colNames: String*): DataFrame = df.drop(colNames*)

  def union(other: Dataset[T]): Dataset[T] = Dataset(df.union(other.df), encoder)

  def intersect(other: Dataset[T]): Dataset[T] = Dataset(df.intersect(other.df), encoder)

  def except(other: Dataset[T]): Dataset[T] = Dataset(df.except(other.df), encoder)

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

  def isEmpty: Boolean = df.isEmpty

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
