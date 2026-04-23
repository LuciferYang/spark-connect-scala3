package org.apache.spark.sql

import com.google.protobuf.ByteString
import org.apache.spark.connect.proto.{
  Aggregate,
  CoGroupMap,
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
import org.apache.spark.sql.connect.common.{
  CoGroupAdaptor,
  CountGroupsAdaptor,
  MapGroupsAdaptor,
  MapValuesFlatMapAdaptor,
  ReduceGroupsAdaptor,
  UdfPacket
}
import org.apache.spark.sql.expressions.ReduceAggregator
import org.apache.spark.sql.streaming.{
  GroupState,
  GroupStateTimeout,
  OutputMode,
  StatefulProcessor,
  StatefulProcessorWithInitialState,
  EmptyInitialStateStruct,
  TimeMode
}

import scala.annotation.nowarn
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
    private[sql] val groupingFunc: V => K,
    // Pre-built grouping UDF proto. Set by mapValues to avoid re-serializing
    // the original groupingFunc against the wrong value type.
    private[sql] val groupingUdfOverride: Option[CommonInlineUserDefinedFunction] = None,
    // Original (pre-mapValues) relation and value encoder for GroupMap input.
    // After mapValues, the input relation in the proto must reference the original dataset,
    // because the server applies the grouping UDF to the input relation.
    private[sql] val originalRelation: Option[Relation] = None,
    private[sql] val originalValueEncoder: Option[Encoder[?]] = None,
    // The mapValues transform function. When set, flatMapGroups/mapGroups compose this
    // with the user function so the server receives an (OriginalV => K) grouping UDF
    // and the map function internally applies the value transform.
    private[sql] val mapValuesFunc: Option[Any => V] = None,
    // Column expressions from groupBy(...).as[K, V]. When set, these are appended
    // after the dummy grouping UDF in all proto builders (matching upstream behavior).
    private[sql] val groupingColumnExprs: Option[Seq[Column]] = None
):

  private val keyEncoder: Encoder[K] = summon[Encoder[K]]
  private val valueEncoder: Encoder[V] = ds.encoder

  /** Cast the key type of this grouped dataset to a new type. */
  def keyAs[L: Encoder: ClassTag]: KeyValueGroupedDataset[L, V] =
    new KeyValueGroupedDataset(
      ds,
      groupingFunc.asInstanceOf[V => L],
      groupingColumnExprs = groupingColumnExprs
    )(using
      summon[Encoder[L]],
      summon[ClassTag[L]],
      ds.encoder,
      summon[ClassTag[V]]
    )

  /** Apply a transformation function to the value of each group. */
  def mapValues[W: Encoder: ClassTag](func: V => W): KeyValueGroupedDataset[K, W] =
    val transformedDs = ds.map(func)(using summon[Encoder[W]], summon[ClassTag[W]])
    // Capture the current grouping UDF proto and original relation before the value type changes.
    // After mapping, the original groupingFunc (V => K) is no longer type-compatible
    // with the new value type W, so we pre-build the UDF and pass it as an override.
    val keyAg = keyEncoder.agnosticEncoder
    val valueAg = valueEncoder.agnosticEncoder
    val cachedGroupingUdf =
      if keyAg != null && valueAg != null then Some(buildGroupingUdf(keyAg, valueAg))
      else groupingUdfOverride
    // Preserve the original (pre-mapValues) relation for GroupMap input.
    // If this KVGD already has an originalRelation (chained mapValues), keep that.
    val origRel = originalRelation.orElse(Some(ds.df.relation))
    val origValEnc = originalValueEncoder.orElse(Some(valueEncoder))
    val newKvgd = new KeyValueGroupedDataset(
      transformedDs,
      groupingFunc.asInstanceOf[W => K], // not actually called — override is used
      cachedGroupingUdf,
      origRel,
      origValEnc,
      // Compose with any existing mapValuesFunc for chained mapValues calls.
      // The composed function maps from the original value type to the final mapped type.
      Some(mapValuesFunc match
        case Some(prev) => ((x: Any) => func(prev(x).asInstanceOf[V])).asInstanceOf[Any => W]
        case None       => ((x: Any) => func(x.asInstanceOf[V])).asInstanceOf[Any => W]),
      groupingColumnExprs
    )(using keyEncoder, summon[ClassTag[K]], summon[Encoder[W]], summon[ClassTag[W]])
    newKvgd

  /** Return a Dataset of the unique keys. */
  def keys: Dataset[K] =
    groupingColumnExprs match
      case Some(exprs) =>
        ds.toDF().select(exprs*).distinct().as[K](using keyEncoder, summon[ClassTag[K]])
      case None =>
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
    flatMapGroups(new MapGroupsAdaptor(func))

  /** Apply a function to each group, returning an iterator of results per group. */
  def flatMapGroups[U: Encoder: ClassTag](
      func: (K, Iterator[V]) => IterableOnce[U]
  ): Dataset[U] =
    val outEnc = summon[Encoder[U]]
    val keyAg = keyEncoder.agnosticEncoder
    val outAg = outEnc.agnosticEncoder
    // For mapValues-derived KVGDs, use the original relation and value encoder
    val inputRelation = originalRelation.getOrElse(ds.df.relation)
    val inputValueAg =
      originalValueEncoder.map(_.agnosticEncoder).getOrElse(valueEncoder.agnosticEncoder)
    val currentValueAg = valueEncoder.agnosticEncoder
    if keyAg == null || inputValueAg == null || currentValueAg == null || outAg == null then
      return flatMapGroupsLocal(func, outEnc)
    // Build server-side GroupMap
    val groupingUdf = buildGroupingUdf(keyAg, inputValueAg)
    // When mapValuesFunc is set, compose it with the user function so the server-side UDF
    // accepts the original value type and internally maps values before calling user func.
    val effectiveFunc: AnyRef = mapValuesFunc match
      case Some(mvf) =>
        new MapValuesFlatMapAdaptor[K, U](
          mvf.asInstanceOf[Any => Any],
          func.asInstanceOf[(K, Iterator[Any]) => IterableOnce[U]]
        )
      case None => func.asInstanceOf[AnyRef]
    val mapUdf = buildGroupMapUdf(effectiveFunc, keyAg, inputValueAg, outAg)
    val groupMapBuilder = GroupMap
      .newBuilder()
      .setInput(inputRelation)
      .addGroupingExpressions(
        Expression
          .newBuilder()
          .setCommonInlineUserDefinedFunction(groupingUdf)
          .build()
      )
      .setFunc(mapUdf)
    groupingColumnExprs.foreach(_.foreach(c => groupMapBuilder.addGroupingExpressions(c.expr)))
    val relation = Relation
      .newBuilder()
      .setCommon(
        RelationCommon.newBuilder().setPlanId(ds.sparkSession.nextPlanId()).build()
      )
      .setGroupMap(groupMapBuilder.build())
      .build()
    val newDf = DataFrame(ds.sparkSession, relation)
    Dataset(newDf, outEnc)

  /** Reduce the values in each group using an associative binary operator.
    *
    * Returns a Dataset of (key, reduced_value) pairs. When AgnosticEncoder is available, uses a
    * server-side `ReduceAggregator`; otherwise falls back to client-side mapGroups.
    */
  def reduceGroups(func: (V, V) => V)(using
      pairEnc: Encoder[(K, V)],
      pairCt: ClassTag[(K, V)]
  ): Dataset[(K, V)] =
    val valueAg = valueEncoder.agnosticEncoder
    if valueAg != null then
      val reducer = ReduceAggregator[V](func)(using valueEncoder)
      agg(reducer.toColumn)
    else mapGroups(new ReduceGroupsAdaptor(func))

  /** Count the number of elements in each group.
    *
    * Returns a Dataset of (key, count) pairs. Requires an Encoder for the pair type.
    */
  def count()(using
      pairEnc: Encoder[(K, Long)],
      pairCt: ClassTag[(K, Long)]
  ): Dataset[(K, Long)] =
    mapGroups(new CountGroupsAdaptor[K])

  /** Apply a function to each group with sorted values, returning an iterator of results per group.
    *
    * The sorting expressions define the order of values within each group before the function is
    * applied.
    */
  def flatMapSortedGroups[U: Encoder: ClassTag](
      sortExprs: Column*
  )(func: (K, Iterator[V]) => IterableOnce[U]): Dataset[U] =
    val outEnc = summon[Encoder[U]]
    val keyAg = keyEncoder.agnosticEncoder
    val valueAg = valueEncoder.agnosticEncoder
    val outAg = outEnc.agnosticEncoder
    if keyAg == null || valueAg == null || outAg == null then
      return flatMapGroupsLocal(func, outEnc)
    val groupingUdf = buildGroupingUdf(keyAg, valueAg)
    val mapUdf = buildGroupMapUdf(func, keyAg, valueAg, outAg)
    val groupMapBuilder = GroupMap
      .newBuilder()
      .setInput(ds.df.relation)
      .addGroupingExpressions(
        Expression
          .newBuilder()
          .setCommonInlineUserDefinedFunction(groupingUdf)
          .build()
      )
      .setFunc(mapUdf)
    groupingColumnExprs.foreach(_.foreach(c => groupMapBuilder.addGroupingExpressions(c.expr)))
    sortExprs.foreach(c => groupMapBuilder.addSortingExpressions(c.expr))
    val relation = Relation
      .newBuilder()
      .setCommon(
        RelationCommon.newBuilder().setPlanId(ds.sparkSession.nextPlanId()).build()
      )
      .setGroupMap(groupMapBuilder.build())
      .build()
    val newDf = DataFrame(ds.sparkSession, relation)
    Dataset(newDf, outEnc)

  /** Co-group with another KeyValueGroupedDataset and apply a function. */
  @nowarn("msg=unused.*parameter")
  def cogroup[U: Encoder: ClassTag, R: Encoder: ClassTag](
      other: KeyValueGroupedDataset[K, U]
  )(func: (K, Iterator[V], Iterator[U]) => IterableOnce[R]): Dataset[R] =
    val outEnc = summon[Encoder[R]]
    val otherEnc = other.valueEncoder
    val keyAg = keyEncoder.agnosticEncoder
    val valueAg = valueEncoder.agnosticEncoder
    val otherValueAg = otherEnc.agnosticEncoder
    val outAg = outEnc.agnosticEncoder
    if keyAg == null || valueAg == null || otherValueAg == null || outAg == null then
      return cogroupLocal(other, func, outEnc)
    val thisGroupingUdf = buildGroupingUdf(keyAg, valueAg)
    val otherGroupingUdf = other.buildGroupingUdf(keyAg, otherValueAg)
    val cogroupUdf = buildCoGroupUdf(
      new CoGroupAdaptor(func).asInstanceOf[AnyRef],
      keyAg,
      valueAg,
      otherValueAg,
      outAg
    )
    val builder = CoGroupMap
      .newBuilder()
      .setInput(ds.df.relation)
      .setOther(other.ds.df.relation)
      .addInputGroupingExpressions(
        Expression.newBuilder().setCommonInlineUserDefinedFunction(thisGroupingUdf).build()
      )
      .addOtherGroupingExpressions(
        Expression.newBuilder().setCommonInlineUserDefinedFunction(otherGroupingUdf).build()
      )
      .setFunc(cogroupUdf)
    groupingColumnExprs.foreach(_.foreach(c => builder.addInputGroupingExpressions(c.expr)))
    other.groupingColumnExprs.foreach(_.foreach(c => builder.addOtherGroupingExpressions(c.expr)))
    val relation = Relation
      .newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(ds.sparkSession.nextPlanId()).build())
      .setCoGroupMap(builder.build())
      .build()
    val newDf = DataFrame(ds.sparkSession, relation)
    Dataset(newDf, outEnc)

  /** Co-group with sorting within each group.
    *
    * Sorts this group's values by `thisSortExprs` and the other group's values by `otherSortExprs`
    * before applying the function.
    */
  @nowarn("msg=unused.*parameter")
  def cogroupSorted[U: Encoder: ClassTag, R: Encoder: ClassTag](
      other: KeyValueGroupedDataset[K, U]
  )(thisSortExprs: Column*)(otherSortExprs: Column*)(
      func: (K, Iterator[V], Iterator[U]) => IterableOnce[R]
  ): Dataset[R] =
    val outEnc = summon[Encoder[R]]
    val otherEnc = other.valueEncoder
    val keyAg = keyEncoder.agnosticEncoder
    val valueAg = valueEncoder.agnosticEncoder
    val otherValueAg = otherEnc.agnosticEncoder
    val outAg = outEnc.agnosticEncoder
    if keyAg == null || valueAg == null || otherValueAg == null || outAg == null then
      return cogroupLocal(other, func, outEnc)
    val thisGroupingUdf = buildGroupingUdf(keyAg, valueAg)
    val otherGroupingUdf = other.buildGroupingUdf(keyAg, otherValueAg)
    val cogroupUdf = buildCoGroupUdf(
      new CoGroupAdaptor(func).asInstanceOf[AnyRef],
      keyAg,
      valueAg,
      otherValueAg,
      outAg
    )
    val builder = CoGroupMap
      .newBuilder()
      .setInput(ds.df.relation)
      .setOther(other.ds.df.relation)
      .addInputGroupingExpressions(
        Expression.newBuilder().setCommonInlineUserDefinedFunction(thisGroupingUdf).build()
      )
      .addOtherGroupingExpressions(
        Expression.newBuilder().setCommonInlineUserDefinedFunction(otherGroupingUdf).build()
      )
      .setFunc(cogroupUdf)
    groupingColumnExprs.foreach(_.foreach(c => builder.addInputGroupingExpressions(c.expr)))
    other.groupingColumnExprs.foreach(_.foreach(c => builder.addOtherGroupingExpressions(c.expr)))
    thisSortExprs.foreach(c => builder.addInputSortingExpressions(c.expr))
    otherSortExprs.foreach(c => builder.addOtherSortingExpressions(c.expr))
    val relation = Relation
      .newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(ds.sparkSession.nextPlanId()).build())
      .setCoGroupMap(builder.build())
      .build()
    val newDf = DataFrame(ds.sparkSession, relation)
    Dataset(newDf, outEnc)

  // ---------------------------------------------------------------------------
  // Java Functional Interface Overloads
  //
  // Only overloads with distinct parameter shapes (not SAM-ambiguous with
  // Scala function types) are provided. For mapGroups, flatMapGroups, and
  // reduceGroups, Java callers should use the Scala function overloads
  // directly (Scala 3 auto-converts Java lambdas).
  // ---------------------------------------------------------------------------

  /** CoGroup using a Java CoGroupFunction. */
  def cogroup[U: Encoder: ClassTag, R: Encoder: ClassTag](
      other: KeyValueGroupedDataset[K, U],
      func: org.apache.spark.api.java.function.CoGroupFunction[K, V, U, R]
  ): Dataset[R] =
    import scala.jdk.CollectionConverters.*
    cogroup(other)((key: K, left: Iterator[V], right: Iterator[U]) =>
      func.call(key, left.asJava, right.asJava).asScala
    )

  /** CogroupSorted using a Java CoGroupFunction. */
  def cogroupSorted[U: Encoder: ClassTag, R: Encoder: ClassTag](
      other: KeyValueGroupedDataset[K, U],
      thisSortExprs: Array[Column],
      otherSortExprs: Array[Column],
      func: org.apache.spark.api.java.function.CoGroupFunction[K, V, U, R]
  ): Dataset[R] =
    import scala.jdk.CollectionConverters.*
    cogroupSorted(other)(thisSortExprs.toSeq*)(otherSortExprs.toSeq*)(
      (key: K, left: Iterator[V], right: Iterator[U]) =>
        func.call(key, left.asJava, right.asJava).asScala
    )

  // ---------------------------------------------------------------------------
  // Typed aggregation (agg with TypedColumn)
  // ---------------------------------------------------------------------------

  /** Compute aggregates by specifying a series of aggregate columns. The aggregate columns must be
    * [[TypedColumn]]s created by typed aggregation functions (e.g. `Aggregator.toColumn` or
    * `typed.sum`). The result is a Dataset containing the key and the aggregation results.
    */
  @nowarn("msg=unused.*parameter")
  def agg[U1](col1: TypedColumn[V, U1])(using
      e1: Encoder[U1],
      pairEnc: Encoder[(K, U1)],
      pairCt: ClassTag[(K, U1)]
  ): Dataset[(K, U1)] =
    aggUntyped(pairEnc, pairCt)(col1)

  @nowarn("msg=unused.*parameter")
  def agg[U1, U2](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2]
  )(using
      e1: Encoder[U1],
      e2: Encoder[U2],
      tupleEnc: Encoder[(K, U1, U2)],
      tupleCt: ClassTag[(K, U1, U2)]
  ): Dataset[(K, U1, U2)] =
    aggUntyped(tupleEnc, tupleCt)(col1, col2)

  @nowarn("msg=unused.*parameter")
  def agg[U1, U2, U3](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3]
  )(using
      e1: Encoder[U1],
      e2: Encoder[U2],
      e3: Encoder[U3],
      tupleEnc: Encoder[(K, U1, U2, U3)],
      tupleCt: ClassTag[(K, U1, U2, U3)]
  ): Dataset[(K, U1, U2, U3)] =
    aggUntyped(tupleEnc, tupleCt)(col1, col2, col3)

  @nowarn("msg=unused.*parameter")
  def agg[U1, U2, U3, U4](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4]
  )(using
      e1: Encoder[U1],
      e2: Encoder[U2],
      e3: Encoder[U3],
      e4: Encoder[U4],
      tupleEnc: Encoder[(K, U1, U2, U3, U4)],
      tupleCt: ClassTag[(K, U1, U2, U3, U4)]
  ): Dataset[(K, U1, U2, U3, U4)] =
    aggUntyped(tupleEnc, tupleCt)(col1, col2, col3, col4)

  @nowarn("msg=unused.*parameter")
  def agg[U1, U2, U3, U4, U5](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4],
      col5: TypedColumn[V, U5]
  )(using
      e1: Encoder[U1],
      e2: Encoder[U2],
      e3: Encoder[U3],
      e4: Encoder[U4],
      e5: Encoder[U5],
      tupleEnc: Encoder[(K, U1, U2, U3, U4, U5)],
      tupleCt: ClassTag[(K, U1, U2, U3, U4, U5)]
  ): Dataset[(K, U1, U2, U3, U4, U5)] =
    aggUntyped(tupleEnc, tupleCt)(col1, col2, col3, col4, col5)

  @nowarn("msg=unused.*parameter")
  def agg[U1, U2, U3, U4, U5, U6](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4],
      col5: TypedColumn[V, U5],
      col6: TypedColumn[V, U6]
  )(using
      e1: Encoder[U1],
      e2: Encoder[U2],
      e3: Encoder[U3],
      e4: Encoder[U4],
      e5: Encoder[U5],
      e6: Encoder[U6],
      tupleEnc: Encoder[(K, U1, U2, U3, U4, U5, U6)],
      tupleCt: ClassTag[(K, U1, U2, U3, U4, U5, U6)]
  ): Dataset[(K, U1, U2, U3, U4, U5, U6)] =
    aggUntyped(tupleEnc, tupleCt)(col1, col2, col3, col4, col5, col6)

  @nowarn("msg=unused.*parameter")
  def agg[U1, U2, U3, U4, U5, U6, U7](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4],
      col5: TypedColumn[V, U5],
      col6: TypedColumn[V, U6],
      col7: TypedColumn[V, U7]
  )(using
      e1: Encoder[U1],
      e2: Encoder[U2],
      e3: Encoder[U3],
      e4: Encoder[U4],
      e5: Encoder[U5],
      e6: Encoder[U6],
      e7: Encoder[U7],
      tupleEnc: Encoder[(K, U1, U2, U3, U4, U5, U6, U7)],
      tupleCt: ClassTag[(K, U1, U2, U3, U4, U5, U6, U7)]
  ): Dataset[(K, U1, U2, U3, U4, U5, U6, U7)] =
    aggUntyped(tupleEnc, tupleCt)(col1, col2, col3, col4, col5, col6, col7)

  @nowarn("msg=unused.*parameter")
  def agg[U1, U2, U3, U4, U5, U6, U7, U8](
      col1: TypedColumn[V, U1],
      col2: TypedColumn[V, U2],
      col3: TypedColumn[V, U3],
      col4: TypedColumn[V, U4],
      col5: TypedColumn[V, U5],
      col6: TypedColumn[V, U6],
      col7: TypedColumn[V, U7],
      col8: TypedColumn[V, U8]
  )(using
      e1: Encoder[U1],
      e2: Encoder[U2],
      e3: Encoder[U3],
      e4: Encoder[U4],
      e5: Encoder[U5],
      e6: Encoder[U6],
      e7: Encoder[U7],
      e8: Encoder[U8],
      tupleEnc: Encoder[(K, U1, U2, U3, U4, U5, U6, U7, U8)],
      tupleCt: ClassTag[(K, U1, U2, U3, U4, U5, U6, U7, U8)]
  ): Dataset[(K, U1, U2, U3, U4, U5, U6, U7, U8)] =
    aggUntyped(tupleEnc, tupleCt)(col1, col2, col3, col4, col5, col6, col7, col8)

  /** Internal implementation for typed aggregation with [[TypedColumn]]s.
    *
    * Builds an `Aggregate` proto (not `GroupMap`) with the grouping key UDF in
    * `grouping_expressions` and each `TypedColumn.expr` (a `TypedAggregateExpression`) in
    * `aggregate_expressions`.
    */
  private def aggUntyped[R](resultEnc: Encoder[R], resultCt: ClassTag[R])(
      columns: TypedColumn[?, ?]*
  ): Dataset[R] =
    val keyAg = keyEncoder.agnosticEncoder
    val valueAg = valueEncoder.agnosticEncoder
    require(
      keyAg != null && valueAg != null,
      "agg(TypedColumn) requires AgnosticEncoder support for key and value types"
    )
    val groupingUdf = buildGroupingUdf(keyAg, valueAg)
    val aggBuilder = Aggregate
      .newBuilder()
      .setInput(ds.df.relation)
      .setGroupType(Aggregate.GroupType.GROUP_TYPE_GROUPBY)
      .addGroupingExpressions(
        Expression
          .newBuilder()
          .setCommonInlineUserDefinedFunction(groupingUdf)
          .build()
      )
    groupingColumnExprs.foreach(_.foreach(c => aggBuilder.addGroupingExpressions(c.expr)))
    columns.foreach(c => aggBuilder.addAggregateExpressions(c.expr))
    val relation = Relation
      .newBuilder()
      .setCommon(
        RelationCommon.newBuilder().setPlanId(ds.sparkSession.nextPlanId()).build()
      )
      .setAggregate(aggBuilder.build())
      .build()
    val newDf = DataFrame(ds.sparkSession, relation)
    Dataset(newDf, resultEnc)(using resultCt)

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
  @nowarn("msg=unused.*parameter")
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
    groupingColumnExprs.foreach(_.foreach(c => groupMapBuilder.addGroupingExpressions(c.expr)))
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
  @nowarn("msg=unused.*parameter")
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
    groupingColumnExprs.foreach(_.foreach(c => groupMapBuilder.addGroupingExpressions(c.expr)))
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

  /** Build the grouping key UDF proto.
    *
    * If a pre-built groupingUdfOverride is available (e.g. from mapValues), returns that directly.
    * Otherwise serializes the groupingFunc.
    */
  private[sql] def buildGroupingUdf(
      keyAg: AgnosticEncoder[?],
      valueAg: AgnosticEncoder[?]
  ): CommonInlineUserDefinedFunction =
    groupingUdfOverride.getOrElse {
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
    }

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

  /** Build the co-group map function UDF proto.
    *
    * inputEncoders order: keyEncoder, valueEncoder, otherValueEncoder (matches server expectation).
    */
  private def buildCoGroupUdf(
      func: AnyRef,
      keyAg: AgnosticEncoder[?],
      valueAg: AgnosticEncoder[?],
      otherValueAg: AgnosticEncoder[?],
      outAg: AgnosticEncoder[?]
  ): CommonInlineUserDefinedFunction =
    val packet = UdfPacket(func, Seq(keyAg, valueAg, otherValueAg), outAg)
    val payload = UdfPacket.serialize(packet)
    val scalaUdf = ScalarScalaUDF
      .newBuilder()
      .setPayload(ByteString.copyFrom(payload))
      .setOutputType(DataTypeProtoConverter.toProto(outAg.dataType))
      .setNullable(true)
    CommonInlineUserDefinedFunction
      .newBuilder()
      .setFunctionName("cogroup")
      .setDeterministic(true)
      .setScalarScalaUdf(scalaUdf.build())
      .build()

object KeyValueGroupedDataset:
  private[sql] def apply[K: Encoder: ClassTag, V: Encoder: ClassTag](
      ds: Dataset[V],
      func: V => K
  ): KeyValueGroupedDataset[K, V] =
    new KeyValueGroupedDataset(ds, func)

  /** Factory for column-based grouping via `GroupedDataFrame.as[K, V]`. */
  private[sql] def fromColumns[K: Encoder: ClassTag, V: Encoder: ClassTag](
      ds: Dataset[V],
      groupingExprs: Seq[Column]
  ): KeyValueGroupedDataset[K, V] =
    val dummyFunc: V => K = _ => null.asInstanceOf[K]
    new KeyValueGroupedDataset(ds, dummyFunc, groupingColumnExprs = Some(groupingExprs))
