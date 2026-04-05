package org.apache.spark.sql

import com.google.protobuf.ByteString
import org.apache.spark.connect.proto.{
  CommonInlineUserDefinedFunction,
  Expression,
  GroupMap,
  Relation,
  RelationCommon,
  ScalarScalaUDF,
  TransformWithStateInfo
}
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, AgnosticEncoders}
import org.apache.spark.sql.connect.client.DataTypeProtoConverter
import org.apache.spark.sql.connect.common.UdfPacket
import org.apache.spark.sql.streaming.{
  GroupState,
  GroupStateTimeout,
  OutputMode,
  StatefulProcessor,
  StatefulProcessorWithInitialState,
  EmptyInitialStateStruct,
  TimeMode
}

import scala.reflect.ClassTag

/** A Dataset that has been grouped by a key-extracting function, enabling typed group operations.
  *
  * Created via `Dataset.groupByKey`. Supports `mapGroups`, `flatMapGroups`, `reduceGroups`, `keys`,
  * and `count`.
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
    * When AgnosticEncoder bridges are available, builds a server-side `GroupMap` proto. Otherwise
    * falls back to client-side implementation.
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
  // Stateful streaming operations
  // ---------------------------------------------------------------------------

  /** Apply a function with managed state to each group (mapGroupsWithState).
    *
    * The state function receives the key, an iterator of values, and the group state. It returns a
    * single output value per group invocation. Output mode is implicitly "Update".
    */
  def mapGroupsWithState[S: Encoder: ClassTag, U: Encoder: ClassTag](
      timeoutConf: GroupStateTimeout
  )(func: (K, Iterator[V], GroupState[S]) => U): Dataset[U] =
    val flatMapFunc: (K, Iterator[V], GroupState[S]) => Iterator[U] =
      (k, iter, state) => Iterator.single(func(k, iter, state))
    flatMapGroupsWithStateHelper(
      isMapGroupsWithState = true,
      outputMode = OutputMode.Update,
      timeoutConf = timeoutConf,
      initialState = None
    )(flatMapFunc)

  /** mapGroupsWithState with initial state. */
  def mapGroupsWithState[S: Encoder: ClassTag, U: Encoder: ClassTag](
      timeoutConf: GroupStateTimeout,
      initialState: KeyValueGroupedDataset[K, S]
  )(func: (K, Iterator[V], GroupState[S]) => U): Dataset[U] =
    val flatMapFunc: (K, Iterator[V], GroupState[S]) => Iterator[U] =
      (k, iter, state) => Iterator.single(func(k, iter, state))
    flatMapGroupsWithStateHelper(
      isMapGroupsWithState = true,
      outputMode = OutputMode.Update,
      timeoutConf = timeoutConf,
      initialState = Some(initialState)
    )(flatMapFunc)

  /** Apply a function with managed state to each group (flatMapGroupsWithState).
    *
    * The state function receives the key, an iterator of values, and the group state. It returns an
    * iterator of output values per group invocation.
    */
  def flatMapGroupsWithState[S: Encoder: ClassTag, U: Encoder: ClassTag](
      outputMode: OutputMode,
      timeoutConf: GroupStateTimeout
  )(func: (K, Iterator[V], GroupState[S]) => Iterator[U]): Dataset[U] =
    flatMapGroupsWithStateHelper(
      isMapGroupsWithState = false,
      outputMode = outputMode,
      timeoutConf = timeoutConf,
      initialState = None
    )(func)

  /** flatMapGroupsWithState with initial state. */
  def flatMapGroupsWithState[S: Encoder: ClassTag, U: Encoder: ClassTag](
      outputMode: OutputMode,
      timeoutConf: GroupStateTimeout,
      initialState: KeyValueGroupedDataset[K, S]
  )(func: (K, Iterator[V], GroupState[S]) => Iterator[U]): Dataset[U] =
    flatMapGroupsWithStateHelper(
      isMapGroupsWithState = false,
      outputMode = outputMode,
      timeoutConf = timeoutConf,
      initialState = Some(initialState)
    )(func)

  /** Apply a stateful processor to each group (transformWithState). */
  def transformWithState[U: Encoder: ClassTag](
      statefulProcessor: StatefulProcessor[K, V, U],
      timeMode: TimeMode,
      outputMode: OutputMode
  ): Dataset[U] =
    transformWithStateHelper(statefulProcessor, timeMode, outputMode, None)

  /** transformWithState with initial state. */
  def transformWithState[U: Encoder: ClassTag, S: Encoder: ClassTag](
      statefulProcessor: StatefulProcessorWithInitialState[K, V, U, S],
      timeMode: TimeMode,
      outputMode: OutputMode,
      initialState: KeyValueGroupedDataset[K, S]
  ): Dataset[U] =
    transformWithStateHelper(statefulProcessor, timeMode, outputMode, Some(initialState))

  /** transformWithState with event time column name. */
  def transformWithState[U: Encoder: ClassTag](
      statefulProcessor: StatefulProcessor[K, V, U],
      eventTimeColumnName: String,
      outputMode: OutputMode
  ): Dataset[U] =
    transformWithStateHelper(
      statefulProcessor,
      TimeMode.EventTime,
      outputMode,
      None,
      eventTimeColumnName
    )

  // ---------------------------------------------------------------------------
  // Private helpers — stateful streaming
  // ---------------------------------------------------------------------------

  /** Common helper for mapGroupsWithState / flatMapGroupsWithState.
    *
    * Builds a GroupMap proto with state-related fields.
    */
  private def flatMapGroupsWithStateHelper[S: Encoder: ClassTag, U: Encoder: ClassTag](
      isMapGroupsWithState: Boolean,
      outputMode: OutputMode,
      timeoutConf: GroupStateTimeout,
      initialState: Option[KeyValueGroupedDataset[K, S]]
  )(func: (K, Iterator[V], GroupState[S]) => Iterator[U]): Dataset[U] =
    val outEnc = summon[Encoder[U]]
    val stateEnc = summon[Encoder[S]]
    val keyAg = keyEncoder.agnosticEncoder
    val valueAg = valueEncoder.agnosticEncoder
    val outAg = outEnc.agnosticEncoder
    val stateAg = stateEnc.agnosticEncoder
    require(
      keyAg != null && valueAg != null && outAg != null && stateAg != null,
      "Stateful operations require AgnosticEncoder support for all types"
    )
    val groupingUdf = buildGroupingUdf(keyAg, valueAg)
    val funcUdf = buildStatefulUdf(
      func.asInstanceOf[AnyRef],
      keyAg,
      stateAg,
      valueAg,
      outAg,
      "flatMapGroupsWithState"
    )
    val groupMapBuilder = GroupMap
      .newBuilder()
      .setInput(ds.df.relation)
      .addGroupingExpressions(
        Expression
          .newBuilder()
          .setCommonInlineUserDefinedFunction(groupingUdf)
          .build()
      )
      .setFunc(funcUdf)
      .setIsMapGroupsWithState(isMapGroupsWithState)
      .setOutputMode(outputMode.toString)
      .setTimeoutConf(timeoutConf.toString)
      .setStateSchema(DataTypeProtoConverter.toProto(stateAg.dataType))
    initialState.foreach { is =>
      val isKeyAg = is.keyEncoder.agnosticEncoder
      val isValueAg = is.valueEncoder.agnosticEncoder
      val isGroupingUdf = is.buildGroupingUdf(isKeyAg, isValueAg)
      groupMapBuilder
        .setInitialInput(is.ds.df.relation)
        .addInitialGroupingExpressions(
          Expression
            .newBuilder()
            .setCommonInlineUserDefinedFunction(isGroupingUdf)
            .build()
        )
    }
    val relation = Relation
      .newBuilder()
      .setCommon(
        RelationCommon.newBuilder().setPlanId(ds.sparkSession.nextPlanId()).build()
      )
      .setGroupMap(groupMapBuilder.build())
      .build()
    val newDf = DataFrame(ds.sparkSession, relation)
    Dataset(newDf, outEnc)

  /** Common helper for transformWithState variants.
    *
    * Serializes the statefulProcessor itself as the UDF function and builds a GroupMap proto with
    * TransformWithStateInfo.
    */
  private def transformWithStateHelper[U: Encoder: ClassTag, S: Encoder: ClassTag](
      statefulProcessor: StatefulProcessor[K, V, U],
      timeMode: TimeMode,
      outputMode: OutputMode,
      initialState: Option[KeyValueGroupedDataset[K, S]],
      eventTimeColumnName: String = ""
  ): Dataset[U] =
    val outEnc = summon[Encoder[U]]
    val keyAg = keyEncoder.agnosticEncoder
    val valueAg = valueEncoder.agnosticEncoder
    val outAg = outEnc.agnosticEncoder
    require(
      keyAg != null && valueAg != null && outAg != null,
      "transformWithState requires AgnosticEncoder support for key, value, and output types"
    )
    val initialStateAg: AgnosticEncoder[?] = initialState match
      case Some(is) =>
        val isEnc = is.valueEncoder.agnosticEncoder
        require(isEnc != null, "transformWithState requires AgnosticEncoder for initial state type")
        isEnc
      case None =>
        // Use an empty ProductEncoder as placeholder when no initial state is provided
        AgnosticEncoders.ProductEncoder[EmptyInitialStateStruct](
          summon[ClassTag[EmptyInitialStateStruct]],
          Seq.empty
        )
    val inputEncoders: Seq[AgnosticEncoder[?]] = Seq(keyAg, initialStateAg, valueAg)
    val funcUdf = buildTransformWithStateUdf(
      statefulProcessor.asInstanceOf[AnyRef],
      inputEncoders,
      outAg
    )
    val groupingUdf = buildGroupingUdf(keyAg, valueAg)
    val twsInfoBuilder = TransformWithStateInfo.newBuilder()
      .setTimeMode(timeMode.toString)
    if eventTimeColumnName.nonEmpty then
      twsInfoBuilder.setEventTimeColumnName(eventTimeColumnName)
    val groupMapBuilder = GroupMap
      .newBuilder()
      .setInput(ds.df.relation)
      .addGroupingExpressions(
        Expression
          .newBuilder()
          .setCommonInlineUserDefinedFunction(groupingUdf)
          .build()
      )
      .setFunc(funcUdf)
      .setOutputMode(outputMode.toString)
      .setTransformWithStateInfo(twsInfoBuilder.build())
    initialState.foreach { is =>
      val isKeyAg = is.keyEncoder.agnosticEncoder
      val isValueAg = is.valueEncoder.agnosticEncoder
      val isGroupingUdf = is.buildGroupingUdf(isKeyAg, isValueAg)
      groupMapBuilder
        .setInitialInput(is.ds.df.relation)
        .addInitialGroupingExpressions(
          Expression
            .newBuilder()
            .setCommonInlineUserDefinedFunction(isGroupingUdf)
            .build()
        )
    }
    val relation = Relation
      .newBuilder()
      .setCommon(
        RelationCommon.newBuilder().setPlanId(ds.sparkSession.nextPlanId()).build()
      )
      .setGroupMap(groupMapBuilder.build())
      .build()
    val newDf = DataFrame(ds.sparkSession, relation)
    Dataset(newDf, outEnc)

  /** Build a UDF proto for stateful operations (mapGroupsWithState / flatMapGroupsWithState).
    *
    * inputEncoders order: keyEncoder, stateEncoder, valueEncoder (matches server expectation).
    */
  private def buildStatefulUdf(
      func: AnyRef,
      keyAg: AgnosticEncoder[?],
      stateAg: AgnosticEncoder[?],
      valueAg: AgnosticEncoder[?],
      outAg: AgnosticEncoder[?],
      funcName: String
  ): CommonInlineUserDefinedFunction =
    val packet = UdfPacket(func, Seq(keyAg, stateAg, valueAg), outAg)
    val payload = UdfPacket.serialize(packet)
    val scalaUdf = ScalarScalaUDF
      .newBuilder()
      .setPayload(ByteString.copyFrom(payload))
      .setOutputType(DataTypeProtoConverter.toProto(outAg.dataType))
      .setNullable(true)
    CommonInlineUserDefinedFunction
      .newBuilder()
      .setFunctionName(funcName)
      .setDeterministic(true)
      .setScalarScalaUdf(scalaUdf.build())
      .build()

  /** Build a UDF proto for transformWithState.
    *
    * The statefulProcessor itself is serialized as the function.
    */
  private def buildTransformWithStateUdf(
      statefulProcessor: AnyRef,
      inputEncoders: Seq[AgnosticEncoder[?]],
      outAg: AgnosticEncoder[?]
  ): CommonInlineUserDefinedFunction =
    val packet = UdfPacket(statefulProcessor, inputEncoders, outAg)
    val payload = UdfPacket.serialize(packet)
    val scalaUdf = ScalarScalaUDF
      .newBuilder()
      .setPayload(ByteString.copyFrom(payload))
      .setOutputType(DataTypeProtoConverter.toProto(outAg.dataType))
      .setNullable(true)
    CommonInlineUserDefinedFunction
      .newBuilder()
      .setFunctionName("transformWithState")
      .setDeterministic(true)
      .setScalarScalaUdf(scalaUdf.build())
      .build()

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
    val allKeys = leftData.keySet ++ rightData.keySet
    val results = allKeys.iterator.flatMap { key =>
      val leftIter = leftData.getOrElse(key, Array.empty[V]).iterator
      val rightIter = rightData.getOrElse(key, Array.empty[U]).iterator
      func(key, leftIter, rightIter)
    }.toSeq
    val rows = results.map(outEnc.toRow)
    val newDf = ds.sparkSession.createDataFrame(rows, outEnc.schema)
    Dataset(newDf, outEnc)

  /** Build the grouping key UDF proto. */
  private[sql] def buildGroupingUdf(
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
