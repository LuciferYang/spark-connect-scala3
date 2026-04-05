package org.apache.spark.sql

import com.google.protobuf.ByteString
import org.apache.spark.connect.proto.{
  CommonInlineUserDefinedFunction,
  Expression,
  GroupMap,
  Relation,
  RelationCommon,
  ScalarScalaUDF
}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.connect.client.DataTypeProtoConverter
import org.apache.spark.sql.connect.common.UdfPacket

import scala.reflect.ClassTag

/** A Dataset that has been grouped by a key-extracting function, enabling typed group operations.
  *
  * Created via `Dataset.groupByKey`. Supports `mapGroups`, `flatMapGroups`, `reduceGroups`,
  * `keys`, and `count`.
  *
  * {{{
  *   case class Person(name: String, age: Int) derives Encoder
  *   val ds: Dataset[Person] = ...
  *   val grouped: KeyValueGroupedDataset[String, Person] = ds.groupByKey(_.name)
  *   val result: Dataset[(String, Int)] = grouped.mapGroups { (key, values) =>
  *     (key, values.size)
  *   }
  * }}}
  */
final class KeyValueGroupedDataset[K: Encoder: ClassTag, V: Encoder: ClassTag] private[sql] (
    private[sql] val ds: Dataset[V],
    private[sql] val groupingFunc: V => K
):

  private val keyEncoder: Encoder[K] = summon[Encoder[K]]
  private val valueEncoder: Encoder[V] = ds.encoder

  /** Return a Dataset of the unique keys. */
  def keys: Dataset[K] =
    ds.map(groupingFunc)
      .toDF()
      .distinct()
      .as[K](using keyEncoder, summon[ClassTag[K]])

  /** Apply a function to each group and return a new Dataset.
    *
    * When AgnosticEncoder bridges are available, builds a server-side `GroupMap` proto.
    * Otherwise falls back to client-side implementation.
    */
  def mapGroups[U: Encoder: ClassTag](func: (K, Iterator[V]) => U): Dataset[U] =
    flatMapGroups((k, iter) => Iterator.single(func(k, iter)))

  /** Apply a function to each group, returning an iterator of results per group. */
  def flatMapGroups[U: Encoder: ClassTag](
      func: (K, Iterator[V]) => IterableOnce[U]
  ): Dataset[U] =
    val outEnc = summon[Encoder[U]]
    val keyAg = keyEncoder.agnosticEncoder
    val valueAg = valueEncoder.agnosticEncoder
    val outAg = outEnc.agnosticEncoder
    if keyAg == null || valueAg == null || outAg == null then
      return flatMapGroupsLocal(func, outEnc)
    // Build server-side GroupMap
    val groupingUdf = buildGroupingUdf(keyAg, valueAg)
    val mapUdf = buildGroupMapUdf(func, keyAg, valueAg, outAg)
    val relation = Relation
      .newBuilder()
      .setCommon(
        RelationCommon.newBuilder().setPlanId(ds.sparkSession.nextPlanId()).build()
      )
      .setGroupMap(
        GroupMap
          .newBuilder()
          .setInput(ds.df.relation)
          .addGroupingExpressions(
            Expression
              .newBuilder()
              .setCommonInlineUserDefinedFunction(groupingUdf)
              .build()
          )
          .setFunc(mapUdf)
          .build()
      )
      .build()
    val newDf = DataFrame(ds.sparkSession, relation)
    Dataset(newDf, outEnc)

  /** Reduce the values in each group using an associative binary operator.
    *
    * Returns a Dataset of (key, reduced_value) pairs. Requires an Encoder for the pair type.
    */
  def reduceGroups(func: (V, V) => V)(using
      pairEnc: Encoder[(K, V)],
      pairCt: ClassTag[(K, V)]
  ): Dataset[(K, V)] =
    mapGroups { (k, iter) =>
      val reduced = iter.reduce(func)
      (k, reduced)
    }

  /** Count the number of elements in each group.
    *
    * Returns a Dataset of (key, count) pairs. Requires an Encoder for the pair type.
    */
  def count()(using
      pairEnc: Encoder[(K, Long)],
      pairCt: ClassTag[(K, Long)]
  ): Dataset[(K, Long)] =
    mapGroups { (k, iter) =>
      (k, iter.size.toLong)
    }

  /** Co-group with another KeyValueGroupedDataset and apply a function.
    *
    * Falls back to client-side implementation since CoGroupMap requires additional proto support.
    */
  def cogroup[U: Encoder: ClassTag, R: Encoder: ClassTag](
      other: KeyValueGroupedDataset[K, U]
  )(func: (K, Iterator[V], Iterator[U]) => IterableOnce[R]): Dataset[R] =
    val outEnc = summon[Encoder[R]]
    cogroupLocal(other, func, outEnc)

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  /** Client-side fallback for flatMapGroups. */
  private def flatMapGroupsLocal[U: ClassTag](
      func: (K, Iterator[V]) => IterableOnce[U],
      outEnc: Encoder[U]
  ): Dataset[U] =
    val data = ds.collect()
    val grouped = data.groupBy(groupingFunc)
    val results = grouped.iterator.flatMap { (key, values) =>
      func(key, values.iterator)
    }.toSeq
    val rows = results.map(outEnc.toRow)
    val newDf = ds.sparkSession.createDataFrame(rows, outEnc.schema)
    Dataset(newDf, outEnc)

  /** Client-side fallback for cogroup. */
  private def cogroupLocal[U: ClassTag, R: ClassTag](
      other: KeyValueGroupedDataset[K, U],
      func: (K, Iterator[V], Iterator[U]) => IterableOnce[R],
      outEnc: Encoder[R]
  ): Dataset[R] =
    val leftData = ds.collect().groupBy(groupingFunc)
    val rightData = other.ds.collect().groupBy(other.groupingFunc)
    val allKeys = (leftData.keySet ++ rightData.keySet)
    val results = allKeys.iterator.flatMap { key =>
      val leftIter = leftData.getOrElse(key, Array.empty[V]).iterator
      val rightIter = rightData.getOrElse(key, Array.empty[U]).iterator
      func(key, leftIter, rightIter)
    }.toSeq
    val rows = results.map(outEnc.toRow)
    val newDf = ds.sparkSession.createDataFrame(rows, outEnc.schema)
    Dataset(newDf, outEnc)

  /** Build the grouping key UDF proto. */
  private def buildGroupingUdf(
      keyAg: AgnosticEncoder[?],
      valueAg: AgnosticEncoder[?]
  ): CommonInlineUserDefinedFunction =
    val packet = UdfPacket(groupingFunc.asInstanceOf[AnyRef], Seq(valueAg), keyAg)
    val payload = UdfPacket.serialize(packet)
    val scalaUdf = ScalarScalaUDF
      .newBuilder()
      .setPayload(ByteString.copyFrom(payload))
      .setOutputType(DataTypeProtoConverter.toProto(keyAg.dataType))
      .setNullable(true)
    CommonInlineUserDefinedFunction
      .newBuilder()
      .setFunctionName("groupByKey")
      .setDeterministic(true)
      .setScalarScalaUdf(scalaUdf.build())
      .build()

  /** Build the group map function UDF proto. */
  private def buildGroupMapUdf(
      func: AnyRef,
      keyAg: AgnosticEncoder[?],
      valueAg: AgnosticEncoder[?],
      outAg: AgnosticEncoder[?]
  ): CommonInlineUserDefinedFunction =
    val packet = UdfPacket(func, Seq(keyAg, valueAg), outAg)
    val payload = UdfPacket.serialize(packet)
    val scalaUdf = ScalarScalaUDF
      .newBuilder()
      .setPayload(ByteString.copyFrom(payload))
      .setOutputType(DataTypeProtoConverter.toProto(outAg.dataType))
      .setNullable(true)
    CommonInlineUserDefinedFunction
      .newBuilder()
      .setFunctionName("flatMapGroups")
      .setDeterministic(true)
      .setScalarScalaUdf(scalaUdf.build())
      .build()

object KeyValueGroupedDataset:
  private[sql] def apply[K: Encoder: ClassTag, V: Encoder: ClassTag](
      ds: Dataset[V],
      func: V => K
  ): KeyValueGroupedDataset[K, V] =
    new KeyValueGroupedDataset(ds, func)
