package org.apache.spark.sql

import org.apache.spark.connect.proto.{StorageLevel as _, Catalog as _, *}
import org.apache.spark.connect.proto.StorageLevel as ProtoStorageLevel
import org.apache.spark.sql.connect.client.{
  ArrowDeserializer,
  DataTypeProtoConverter,
  SparkConnectClient
}
import org.apache.spark.sql.connect.common.LiteralValueProtoConverter
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable
import scala.jdk.CollectionConverters.*

/** A distributed collection of rows organized into named columns.
  *
  * All transformation methods are lazy — they build a protobuf Relation tree. Action methods
  * (collect, show, count, …) send the plan to the server via gRPC.
  */
final class DataFrame private[sql] (
    private[sql] val session: SparkSession,
    private[sql] val relation: Relation
):
  private def client: SparkConnectClient = session.client

  // ---------------------------------------------------------------------------
  // Transformations
  // ---------------------------------------------------------------------------

  def select(cols: Column*): DataFrame =
    val projBuilder = Project.newBuilder().setInput(relation)
    cols.foreach(c => projBuilder.addExpressions(c.expr))
    withRelation(cols.toSeq)(_.setProject(projBuilder.build()))

  def select(colNames: String*)(using DummyImplicit): DataFrame =
    select(colNames.map(Column(_))*)

  def selectExpr(exprs: String*): DataFrame =
    select(exprs.map(e => functions.expr(e))*)

  def filter(condition: Column): DataFrame =
    withRelation(Seq(condition))(_.setFilter(
      Filter.newBuilder().setInput(relation).setCondition(condition.expr).build()
    ))

  def where(condition: Column): DataFrame = filter(condition)

  def where(conditionExpr: String): DataFrame = filter(functions.expr(conditionExpr))

  def limit(n: Int): DataFrame =
    withRelation(_.setLimit(
      Limit.newBuilder().setInput(relation).setLimit(n).build()
    ))

  def offset(n: Int): DataFrame =
    withRelation(_.setOffset(
      Offset.newBuilder().setInput(relation).setOffset(n).build()
    ))

  def sort(cols: Column*): DataFrame = orderBy(cols*)

  def orderBy(cols: Column*): DataFrame =
    val sortBuilder = Sort.newBuilder().setInput(relation).setIsGlobal(true)
    cols.foreach(c => sortBuilder.addOrder(c.toSortOrder))
    withRelation(cols.toSeq)(_.setSort(sortBuilder.build()))

  def groupBy(cols: Column*): GroupedDataFrame =
    GroupedDataFrame(this, cols.toSeq, GroupedDataFrame.GroupType.GroupBy)

  def groupBy(colNames: String*)(using DummyImplicit): GroupedDataFrame =
    groupBy(colNames.map(Column(_))*)

  def rollup(cols: Column*): GroupedDataFrame =
    GroupedDataFrame(this, cols.toSeq, GroupedDataFrame.GroupType.Rollup)

  def cube(cols: Column*): GroupedDataFrame =
    GroupedDataFrame(this, cols.toSeq, GroupedDataFrame.GroupType.Cube)

  def agg(aggExpr: Column, aggExprs: Column*): DataFrame =
    groupBy(Seq.empty[Column]*).agg(aggExpr, aggExprs*)

  def join(right: DataFrame, joinExpr: Column, joinType: String = "inner"): DataFrame =
    withRelation(Seq(joinExpr))(_.setJoin(
      Join.newBuilder()
        .setLeft(relation)
        .setRight(right.relation)
        .setJoinCondition(joinExpr.expr)
        .setJoinType(toJoinType(joinType))
        .build()
    ))

  /** Join with no condition (implicit cross join). */
  def join(right: DataFrame): DataFrame =
    withRelation(_.setJoin(
      Join.newBuilder()
        .setLeft(relation)
        .setRight(right.relation)
        .setJoinType(Join.JoinType.JOIN_TYPE_INNER)
        .build()
    ))

  /** Equi-join using a single column name. */
  def join(right: DataFrame, usingColumn: String): DataFrame =
    join(right, Seq(usingColumn))

  def join(right: DataFrame, usingColumns: Seq[String]): DataFrame =
    val joinBuilder = Join.newBuilder()
      .setLeft(relation)
      .setRight(right.relation)
      .setJoinType(Join.JoinType.JOIN_TYPE_INNER)
    usingColumns.foreach(joinBuilder.addUsingColumns)
    withRelation(_.setJoin(joinBuilder.build()))

  /** Equi-join using column names with a specified join type. */
  def join(right: DataFrame, usingColumns: Seq[String], joinType: String): DataFrame =
    val joinBuilder = Join.newBuilder()
      .setLeft(relation)
      .setRight(right.relation)
      .setJoinType(toJoinType(joinType))
    usingColumns.foreach(joinBuilder.addUsingColumns)
    withRelation(_.setJoin(joinBuilder.build()))

  def crossJoin(right: DataFrame): DataFrame =
    withRelation(_.setJoin(
      Join.newBuilder()
        .setLeft(relation)
        .setRight(right.relation)
        .setJoinType(Join.JoinType.JOIN_TYPE_CROSS)
        .build()
    ))

  def withColumn(name: String, col: Column): DataFrame =
    withRelation(Seq(col))(_.setWithColumns(
      WithColumns.newBuilder()
        .setInput(relation)
        .addAliases(Expression.Alias.newBuilder()
          .setExpr(col.expr)
          .addName(name)
          .build())
        .build()
    ))

  def withColumnRenamed(existing: String, newName: String): DataFrame =
    withRelation(_.setWithColumnsRenamed(
      WithColumnsRenamed.newBuilder()
        .setInput(relation)
        .addRenames(WithColumnsRenamed.Rename.newBuilder()
          .setColName(existing)
          .setNewColName(newName)
          .build())
        .build()
    ))

  def drop(colNames: String*): DataFrame =
    val dropBuilder = Drop.newBuilder().setInput(relation)
    colNames.foreach { name =>
      dropBuilder.addColumns(
        Expression.newBuilder().setUnresolvedAttribute(
          Expression.UnresolvedAttribute.newBuilder()
            .setUnparsedIdentifier(name).build()
        ).build()
      )
    }
    withRelation(_.setDrop(dropBuilder.build()))

  def distinct(): DataFrame = dropDuplicates()

  def dropDuplicates(): DataFrame =
    withRelation(_.setDeduplicate(
      Deduplicate.newBuilder().setInput(relation).setAllColumnsAsKeys(true).build()
    ))

  def dropDuplicates(colNames: Seq[String]): DataFrame =
    val dedup = Deduplicate.newBuilder().setInput(relation)
    colNames.foreach(dedup.addColumnNames)
    withRelation(_.setDeduplicate(dedup.build()))

  def dropDuplicatesWithinWatermark(): DataFrame =
    withRelation(_.setDeduplicate(
      Deduplicate.newBuilder()
        .setInput(relation).setAllColumnsAsKeys(true).setWithinWatermark(true).build()
    ))

  def dropDuplicatesWithinWatermark(colNames: Seq[String]): DataFrame =
    val dedup = Deduplicate.newBuilder().setInput(relation).setWithinWatermark(true)
    colNames.foreach(dedup.addColumnNames)
    withRelation(_.setDeduplicate(dedup.build()))

  def dropDuplicatesWithinWatermark(col1: String, cols: String*): DataFrame =
    dropDuplicatesWithinWatermark(col1 +: cols)

  def union(other: DataFrame): DataFrame =
    setOp(other, SetOperation.SetOpType.SET_OP_TYPE_UNION, byName = false)

  def unionAll(other: DataFrame): DataFrame = union(other)

  def unionByName(other: DataFrame, allowMissingColumns: Boolean = false): DataFrame =
    setOp(other, SetOperation.SetOpType.SET_OP_TYPE_UNION, byName = true, allowMissingColumns)

  def intersect(other: DataFrame): DataFrame =
    setOp(other, SetOperation.SetOpType.SET_OP_TYPE_INTERSECT, byName = false)

  def except(other: DataFrame): DataFrame =
    setOp(other, SetOperation.SetOpType.SET_OP_TYPE_EXCEPT, byName = false)

  def repartition(numPartitions: Int): DataFrame =
    withRelation(_.setRepartition(
      Repartition.newBuilder()
        .setInput(relation)
        .setNumPartitions(numPartitions)
        .setShuffle(true)
        .build()
    ))

  def coalesce(numPartitions: Int): DataFrame =
    withRelation(_.setRepartition(
      Repartition.newBuilder()
        .setInput(relation)
        .setNumPartitions(numPartitions)
        .setShuffle(false)
        .build()
    ))

  def sample(fraction: Double, withReplacement: Boolean = false, seed: Long = 0L): DataFrame =
    withRelation(_.setSample(
      Sample.newBuilder()
        .setInput(relation)
        .setLowerBound(0.0)
        .setUpperBound(fraction)
        .setWithReplacement(withReplacement)
        .setSeed(seed)
        .build()
    ))

  def describe(colNames: String*): DataFrame =
    val descBuilder = StatDescribe.newBuilder().setInput(relation)
    colNames.foreach(descBuilder.addCols)
    withRelation(_.setDescribe(descBuilder.build()))

  def summary(statistics: String*): DataFrame =
    val summaryBuilder = StatSummary.newBuilder().setInput(relation)
    statistics.foreach(summaryBuilder.addStatistics)
    withRelation(_.setSummary(summaryBuilder.build()))

  def alias(name: String): DataFrame =
    withRelation(_.setSubqueryAlias(
      SubqueryAlias.newBuilder().setInput(relation).setAlias(name).build()
    ))

  /** Rename all columns. Number of names must match number of columns. */
  def toDF(colNames: String*): DataFrame =
    val toDfBuilder = ToDF.newBuilder().setInput(relation)
    colNames.foreach(toDfBuilder.addColumnNames)
    withRelation(_.setToDf(toDfBuilder.build()))

  /** Returns a new DataFrame where each column is reconciled to match the specified schema. */
  def to(schema: types.StructType): DataFrame =
    withRelation(_.setToSchema(
      ToSchema.newBuilder()
        .setInput(relation)
        .setSchema(DataTypeProtoConverter.toProto(schema))
        .build()
    ))

  /** Hint the optimizer (e.g., broadcast). */
  def hint(name: String, parameters: Any*): DataFrame =
    val hintBuilder = Hint.newBuilder().setInput(relation).setName(name)
    parameters.foreach(p => hintBuilder.addParameters(Column.lit(p).expr))
    withRelation(_.setHint(hintBuilder.build()))

  /** Broadcast hint shorthand. */
  def broadcast: DataFrame = hint("broadcast")

  def intersectAll(other: DataFrame): DataFrame =
    setOp(other, SetOperation.SetOpType.SET_OP_TYPE_INTERSECT, byName = false, isAll = true)

  def exceptAll(other: DataFrame): DataFrame =
    setOp(other, SetOperation.SetOpType.SET_OP_TYPE_EXCEPT, byName = false, isAll = true)

  /** Sort within each partition. */
  def sortWithinPartitions(cols: Column*): DataFrame =
    val sortBuilder = Sort.newBuilder().setInput(relation).setIsGlobal(false)
    cols.foreach(c => sortBuilder.addOrder(c.toSortOrder))
    withRelation(cols.toSeq)(_.setSort(sortBuilder.build()))

  def sortWithinPartitions(colNames: String*)(using DummyImplicit): DataFrame =
    sortWithinPartitions(colNames.map(Column(_))*)

  /** Return the last n rows. */
  def tail(n: Int): Array[Row] =
    val tailRel = Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(session.nextPlanId()).build())
      .setTail(Tail.newBuilder().setInput(relation).setLimit(n).build())
      .build()
    val (rows, observed) = executeAndCollect(tailRel)
    if observed.nonEmpty then session.processObservedMetrics(observed)
    rows

  /** Pipeline-style transformation. */
  def transform(f: DataFrame => DataFrame): DataFrame = f(this)

  /** Repartition by columns. */
  def repartition(numPartitions: Int, cols: Column*): DataFrame =
    if cols.isEmpty then
      repartition(numPartitions)
    else
      val builder = RepartitionByExpression.newBuilder()
        .setInput(relation)
        .setNumPartitions(numPartitions)
      cols.foreach(c => builder.addPartitionExprs(c.expr))
      withRelation(cols.toSeq)(_.setRepartitionByExpression(builder.build()))

  /** Repartition by columns with default partition count. */
  def repartition(cols: Column*)(using DummyImplicit): DataFrame =
    val builder = RepartitionByExpression.newBuilder().setInput(relation)
    cols.foreach(c => builder.addPartitionExprs(c.expr))
    withRelation(cols.toSeq)(_.setRepartitionByExpression(builder.build()))

  /** Cache this DataFrame with the default storage level (MEMORY_AND_DISK). */
  def cache(): DataFrame = persist()

  /** Persist this DataFrame with the default storage level (MEMORY_AND_DISK). */
  def persist(): DataFrame = persist(StorageLevel.MEMORY_AND_DISK)

  /** Persist this DataFrame with the given storage level. */
  def persist(storageLevel: StorageLevel): DataFrame =
    val b = AnalyzePlanRequest.Persist.newBuilder().setRelation(relation)
    b.setStorageLevel(storageLevel.toProto)
    client.analyzePlan { rb =>
      rb.setPersist(b.build())
    }
    this

  /** Remove the cached data for this DataFrame. */
  def unpersist(blocking: Boolean = false): DataFrame =
    client.analyzePlan { rb =>
      rb.setUnpersist(
        AnalyzePlanRequest.Unpersist.newBuilder()
          .setRelation(relation)
          .setBlocking(blocking)
          .build()
      )
    }
    this

  /** Returns a checkpointed version of this DataFrame. */
  def checkpoint(eager: Boolean = true): DataFrame = checkpointInternal(eager, local = false)

  /** Returns a locally checkpointed version of this DataFrame. */
  def localCheckpoint(eager: Boolean = true): DataFrame = checkpointInternal(eager, local = true)

  private def checkpointInternal(eager: Boolean, local: Boolean): DataFrame =
    val cmd = Command.newBuilder()
      .setCheckpointCommand(
        CheckpointCommand.newBuilder()
          .setRelation(relation)
          .setLocal(local)
          .setEager(eager)
          .build()
      )
      .build()
    val responses = client.executeCommandWithResponses(cmd)
    val result = responses
      .find(_.hasCheckpointCommandResult)
      .getOrElse(throw RuntimeException("No CheckpointCommandResult in response"))
    val cachedRelation = result.getCheckpointCommandResult.getRelation
    session.cleaner.register(cachedRelation)
    DataFrame(
      session,
      Relation.newBuilder()
        .setCommon(RelationCommon.newBuilder().setPlanId(session.nextPlanId()).build())
        .setCachedRemoteRelation(cachedRelation)
        .build()
    )

  def na: DataFrameNaFunctions = DataFrameNaFunctions(this)

  def stat: DataFrameStatFunctions = DataFrameStatFunctions(this)

  // ---------------------------------------------------------------------------
  // Advanced Transformations
  // ---------------------------------------------------------------------------

  /** Lateral join — only inner, left outer, and cross joins are supported. */
  def lateralJoin(right: DataFrame, condition: Column, joinType: String = "inner"): DataFrame =
    val jt = toJoinType(joinType)
    jt match
      case Join.JoinType.JOIN_TYPE_INNER | Join.JoinType.JOIN_TYPE_LEFT_OUTER |
          Join.JoinType.JOIN_TYPE_CROSS => // ok
      case _ => throw IllegalArgumentException(
          s"Unsupported lateral join type: $joinType. Only inner, left, and cross are supported."
        )
    val builder = LateralJoin.newBuilder()
      .setLeft(relation).setRight(right.relation)
      .setJoinCondition(condition.expr).setJoinType(jt)
    withRelation(Seq(condition))(_.setLateralJoin(builder.build()))

  /** Lateral join without condition (defaults to inner). */
  def lateralJoin(right: DataFrame): DataFrame =
    withRelation(_.setLateralJoin(LateralJoin.newBuilder()
      .setLeft(relation).setRight(right.relation)
      .setJoinType(Join.JoinType.JOIN_TYPE_INNER).build()))

  /** Group by grouping sets. */
  def groupingSets(groupingSets: Seq[Seq[Column]], cols: Column*): GroupedDataFrame =
    val gsProtos = groupingSets.map { gs =>
      val b = Aggregate.GroupingSets.newBuilder()
      gs.foreach(c => b.addGroupingSet(c.expr))
      b.build()
    }
    GroupedDataFrame(this, cols.toSeq, GroupedDataFrame.GroupType.GroupingSets, Some(gsProtos))

  /** Repartition by range using the given partition expressions. */
  def repartitionByRange(numPartitions: Int, partitionExprs: Column*): DataFrame =
    require(partitionExprs.nonEmpty, "At least one partition expression is required.")
    val sortExprs = partitionExprs.map(c => if c.expr.hasSortOrder then c else c.asc)
    val builder =
      RepartitionByExpression.newBuilder().setInput(relation).setNumPartitions(numPartitions)
    sortExprs.foreach(c => builder.addPartitionExprs(c.expr))
    withRelation(_.setRepartitionByExpression(builder.build()))

  /** Repartition by range with default partition count. */
  def repartitionByRange(partitionExprs: Column*)(using DummyImplicit): DataFrame =
    require(partitionExprs.nonEmpty, "At least one partition expression is required.")
    val sortExprs = partitionExprs.map(c => if c.expr.hasSortOrder then c else c.asc)
    val builder = RepartitionByExpression.newBuilder().setInput(relation)
    sortExprs.foreach(c => builder.addPartitionExprs(c.expr))
    withRelation(_.setRepartitionByExpression(builder.build()))

  /** Unpivot a DataFrame from wide to long format. */
  def unpivot(
      ids: Array[Column],
      values: Array[Column],
      variableColumnName: String,
      valueColumnName: String
  ): DataFrame =
    val builder = Unpivot.newBuilder()
      .setInput(relation)
      .setVariableColumnName(variableColumnName)
      .setValueColumnName(valueColumnName)
    ids.foreach(c => builder.addIds(c.expr))
    val valuesMsg = Unpivot.Values.newBuilder()
    values.foreach(c => valuesMsg.addValues(c.expr))
    builder.setValues(valuesMsg.build())
    withRelation(ids.toSeq ++ values.toSeq)(_.setUnpivot(builder.build()))

  /** Unpivot a DataFrame — all non-id columns become values. */
  def unpivot(
      ids: Array[Column],
      variableColumnName: String,
      valueColumnName: String
  ): DataFrame =
    val builder = Unpivot.newBuilder()
      .setInput(relation)
      .setVariableColumnName(variableColumnName)
      .setValueColumnName(valueColumnName)
    ids.foreach(c => builder.addIds(c.expr))
    withRelation(_.setUnpivot(builder.build()))

  /** Alias for unpivot. */
  def melt(
      ids: Array[Column],
      values: Array[Column],
      variableColumnName: String,
      valueColumnName: String
  ): DataFrame = unpivot(ids, values, variableColumnName, valueColumnName)

  /** Alias for unpivot (all non-id columns become values). */
  def melt(
      ids: Array[Column],
      variableColumnName: String,
      valueColumnName: String
  ): DataFrame = unpivot(ids, variableColumnName, valueColumnName)

  /** Rename multiple columns at once. */
  def withColumnsRenamed(colsMap: Map[String, String]): DataFrame =
    val builder = WithColumnsRenamed.newBuilder().setInput(relation)
    colsMap.foreach { (existing, newName) =>
      builder.addRenames(
        WithColumnsRenamed.Rename.newBuilder()
          .setColName(existing)
          .setNewColName(newName)
          .build()
      )
    }
    withRelation(_.setWithColumnsRenamed(builder.build()))

  /** Add or replace multiple columns at once. */
  def withColumns(colsMap: Map[String, Column]): DataFrame =
    val builder = WithColumns.newBuilder().setInput(relation)
    colsMap.foreach { (name, col) =>
      builder.addAliases(
        Expression.Alias.newBuilder()
          .setExpr(col.expr)
          .addName(name)
          .build()
      )
    }
    withRelation(colsMap.values.toSeq)(_.setWithColumns(builder.build()))

  /** Drop columns by Column expression. */
  def drop(cols: Column*)(using DummyImplicit): DataFrame =
    val dropBuilder = Drop.newBuilder().setInput(relation)
    cols.foreach(c => dropBuilder.addColumns(c.expr))
    withRelation(cols.toSeq)(_.setDrop(dropBuilder.build()))

  /** Observe (collect metrics) on this DataFrame. */
  def observe(name: String, expr: Column, exprs: Column*): DataFrame =
    val builder = CollectMetrics.newBuilder()
      .setInput(relation)
      .setName(name)
    builder.addMetrics(expr.expr)
    exprs.foreach(e => builder.addMetrics(e.expr))
    withRelation(expr +: exprs)(_.setCollectMetrics(builder.build()))

  /** Observe (collect metrics) using an [[Observation]] instance.
    *
    * The observation is bound to the returned DataFrame and will be completed when the first action
    * is executed.
    */
  def observe(observation: Observation, expr: Column, exprs: Column*): DataFrame =
    observation.markRegistered()
    val result = observe(observation.name, expr, exprs*)
    val planId = result.relation.getCommon.getPlanId
    observation.planId = planId
    session.registerObservation(planId, observation)
    result

  /** Explain this plan with a mode string. */
  def explain(mode: String): Unit =
    val plan = Plan.newBuilder().setRoot(relation).build()
    val explainMode = mode.toLowerCase match
      case "simple"    => AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_SIMPLE
      case "extended"  => AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_EXTENDED
      case "codegen"   => AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_CODEGEN
      case "cost"      => AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_COST
      case "formatted" => AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_FORMATTED
      case _           => AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_SIMPLE
    val explainStr = client.analyzeExplain(plan, explainMode)
    println(explainStr)

  /** Randomly split this DataFrame into multiple DataFrames by weights. */
  def randomSplit(weights: Array[Double], seed: Long = 0L): Array[DataFrame] =
    val normalizedWeights = {
      val sum = weights.sum
      weights.map(_ / sum)
    }
    var lowerBound = 0.0
    normalizedWeights.map { w =>
      val upper = lowerBound + w
      val df = withRelation(_.setSample(
        Sample.newBuilder()
          .setInput(relation)
          .setLowerBound(lowerBound)
          .setUpperBound(upper)
          .setWithReplacement(false)
          .setSeed(seed)
          .build()
      ))
      lowerBound = upper
      df
    }

  // ---------------------------------------------------------------------------
  // Output Format
  // ---------------------------------------------------------------------------

  /** Return a DataFrame with each row converted to a JSON string. */
  def toJSON: DataFrame =
    select(functions.to_json(functions.struct(Column("*"))).as("value"))

  // ---------------------------------------------------------------------------
  // Actions
  // ---------------------------------------------------------------------------

  def collect(): Array[Row] =
    val (rows, observed) = executeAndCollect(relation)
    if observed.nonEmpty then session.processObservedMetrics(observed)
    rows

  def collectAsList(): java.util.List[Row] = java.util.Arrays.asList(collect()*)

  def takeAsList(n: Int): java.util.List[Row] = java.util.Arrays.asList(take(n)*)

  def count(): Long =
    import functions.{count as countFn, lit}
    groupBy(Seq.empty[Column]*).agg(countFn(lit(1)).as("count")).collect().head.getLong(0)

  def first(): Row = limit(1).collect().head

  def head(n: Int = 1): Array[Row] = limit(n).collect()

  def take(n: Int): Array[Row] = head(n)

  def show(numRows: Int = 20, truncate: Int = 20): Unit =
    show(numRows, truncate, vertical = false)

  /** Display the first numRows rows of this DataFrame.
    *
    * @param numRows
    *   number of rows to show
    * @param truncate
    *   max width of each column (0 = no truncation)
    * @param vertical
    *   if true, display in vertical format (one line per column value)
    */
  def show(numRows: Int, truncate: Int, vertical: Boolean): Unit =
    val showRel = Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(session.nextPlanId()).build())
      .setShowString(ShowString.newBuilder()
        .setInput(relation)
        .setNumRows(numRows)
        .setTruncate(truncate)
        .setVertical(vertical)
        .build())
      .build()
    val plan = Plan.newBuilder().setRoot(showRel).build()
    val responses = client.execute(plan)
    try
      responses.foreach { resp =>
        if resp.hasArrowBatch then
          ArrowDeserializer
            .fromArrowBatch(resp.getArrowBatch.getData.toByteArray)
            .foreach(row => print(row.get(0)))
      }
    finally
      (responses: Any) match
        case c: AutoCloseable => c.close()
        case _                => ()

  def printSchema(): Unit =
    val s = schema
    println(s.treeString)

  /** Print the schema up to the given nesting depth. */
  def printSchema(level: Int): Unit =
    val s = schema
    println(s.treeString(level))

  def schema: StructType =
    schemaInternal().getOrElse(StructType.empty)

  def columns: Array[String] = schema.fieldNames

  /** Return an array of (column name, data type string) pairs. */
  def dtypes: Array[(String, String)] =
    schema.fields.map(f => (f.name, f.dataType.simpleString)).toArray

  /** Select a column by name, bound to this DataFrame. */
  def col(colName: String): Column =
    Column(Expression.newBuilder()
      .setUnresolvedAttribute(Expression.UnresolvedAttribute.newBuilder()
        .setUnparsedIdentifier(colName)
        .setPlanId(relation.getCommon.getPlanId)
        .build())
      .build())

  /** Select a column by name — alias for `col`. */
  def apply(colName: String): Column = col(colName)

  def explain(extended: Boolean = false): Unit =
    val plan = Plan.newBuilder().setRoot(relation).build()
    val mode =
      if extended then AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_EXTENDED
      else AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_SIMPLE
    val explainStr = client.analyzeExplain(plan, mode)
    println(explainStr)

  def isEmpty: Boolean = limit(1).collect().isEmpty

  /** Return a `java.util.Iterator` that iterates rows lazily, one Arrow batch at a time.
    *
    * The returned iterator implements `AutoCloseable` — callers should close it after use to
    * release server-side resources. The iterator auto-closes when exhausted.
    */
  def toLocalIterator(): java.util.Iterator[Row] with AutoCloseable =
    val plan = Plan.newBuilder().setRoot(relation).build()
    val responses = client.execute(plan)
    val rowIter = responses.flatMap { resp =>
      if resp.hasArrowBatch then
        ArrowDeserializer.fromArrowBatch(resp.getArrowBatch.getData.toByteArray)
      else
        Iterator.empty
    }
    new java.util.Iterator[Row] with AutoCloseable:
      private var closed = false
      def hasNext: Boolean =
        val hn = rowIter.hasNext
        if !hn && !closed then close()
        hn
      def next(): Row = rowIter.next()
      def close(): Unit =
        if !closed then
          closed = true
          (responses: Any) match
            case c: AutoCloseable => c.close()
            case _                => ()

  /** Check if this DataFrame represents a streaming query. */
  def isStreaming: Boolean =
    val resp = client.analyzePlan { b =>
      b.setIsStreaming(
        AnalyzePlanRequest.IsStreaming.newBuilder()
          .setPlan(Plan.newBuilder().setRoot(relation).build())
          .build()
      )
    }
    resp.getIsStreaming.getIsStreaming

  /** Returns true if the `collect` and `take` methods can be run locally. */
  def isLocal: Boolean =
    val resp = client.analyzePlan { b =>
      b.setIsLocal(
        AnalyzePlanRequest.IsLocal.newBuilder()
          .setPlan(Plan.newBuilder().setRoot(relation).build())
          .build()
      )
    }
    resp.getIsLocal.getIsLocal

  /** Return the input files that were used to create this DataFrame. */
  def inputFiles: Array[String] =
    val resp = client.analyzePlan { b =>
      b.setInputFiles(
        AnalyzePlanRequest.InputFiles.newBuilder()
          .setPlan(Plan.newBuilder().setRoot(relation).build())
          .build()
      )
    }
    resp.getInputFiles.getFilesList.asScala.toArray

  /** Returns true if the other DataFrame has the same semantic plan. */
  def sameSemantics(other: DataFrame): Boolean =
    val resp = client.analyzePlan { b =>
      b.setSameSemantics(
        AnalyzePlanRequest.SameSemantics.newBuilder()
          .setTargetPlan(Plan.newBuilder().setRoot(relation).build())
          .setOtherPlan(Plan.newBuilder().setRoot(other.relation).build())
          .build()
      )
    }
    resp.getSameSemantics.getResult

  /** Returns a semantic hash of the logical plan. */
  def semanticHash: Int =
    val resp = client.analyzePlan { b =>
      b.setSemanticHash(
        AnalyzePlanRequest.SemanticHash.newBuilder()
          .setPlan(Plan.newBuilder().setRoot(relation).build())
          .build()
      )
    }
    resp.getSemanticHash.getResult

  /** Get the storage level of this DataFrame. */
  def storageLevel: StorageLevel =
    val resp = client.analyzePlan { b =>
      b.setGetStorageLevel(
        AnalyzePlanRequest.GetStorageLevel.newBuilder()
          .setRelation(relation)
          .build()
      )
    }
    val sl = resp.getGetStorageLevel.getStorageLevel
    StorageLevel(
      useDisk = sl.getUseDisk,
      useMemory = sl.getUseMemory,
      useOffHeap = sl.getUseOffHeap,
      deserialized = sl.getDeserialized,
      replication = sl.getReplication
    )

  // ---------------------------------------------------------------------------
  // Temp Views
  // ---------------------------------------------------------------------------

  def createTempView(viewName: String): Unit =
    createViewCommand(viewName, isGlobal = false, replace = false)

  def createOrReplaceTempView(viewName: String): Unit =
    createViewCommand(viewName, isGlobal = false, replace = true)

  def createGlobalTempView(viewName: String): Unit =
    createViewCommand(viewName, isGlobal = true, replace = false)

  def createOrReplaceGlobalTempView(viewName: String): Unit =
    createViewCommand(viewName, isGlobal = true, replace = true)

  private def createViewCommand(viewName: String, isGlobal: Boolean, replace: Boolean): Unit =
    val cmd = Command.newBuilder()
      .setCreateDataframeView(CreateDataFrameViewCommand.newBuilder()
        .setInput(relation)
        .setName(viewName)
        .setIsGlobal(isGlobal)
        .setReplace(replace)
        .build())
      .build()
    client.executeCommand(cmd)

  // ---------------------------------------------------------------------------
  // Writer
  // ---------------------------------------------------------------------------

  def write: DataFrameWriter = DataFrameWriter(this)

  def writeTo(table: String): DataFrameWriterV2 = DataFrameWriterV2(table, this)

  def mergeInto(table: String, condition: Column): MergeIntoWriter =
    MergeIntoWriter(table, this, condition)

  def writeStream: DataStreamWriter = DataStreamWriter(this)

  /** Define a watermark on an event-time column for streaming aggregations. */
  def withWatermark(eventTime: String, delayThreshold: String): DataFrame =
    withRelation(_.setWithWatermark(
      WithWatermark.newBuilder()
        .setInput(relation)
        .setEventTime(eventTime)
        .setDelayThreshold(delayThreshold)
        .build()
    ))

  /** Transpose this DataFrame from wide to tall format. */
  def transpose(indexColumn: Column): DataFrame =
    withRelation(Seq(indexColumn))(_.setTranspose(
      Transpose.newBuilder()
        .setInput(relation).addIndexColumns(indexColumn.expr).build()
    ))

  /** Transpose this DataFrame from wide to tall format (no index column). */
  def transpose(): DataFrame =
    withRelation(_.setTranspose(
      Transpose.newBuilder().setInput(relation).build()
    ))

  /** Zip this DataFrame with a unique Long index for each row. */
  def zipWithIndex: DataFrame =
    val seqId = Expression.newBuilder()
      .setUnresolvedFunction(Expression.UnresolvedFunction.newBuilder()
        .setFunctionName("distributed_sequence_id")
        .setIsInternal(true)
        .addArguments(Column.lit(0L).expr)
        .build())
      .build()
    select(Column("*"), Column(seqId).as("index"))

  /** Convert this DataFrame to a strongly-typed Dataset[T]. */
  def as[T: Encoder: scala.reflect.ClassTag]: Dataset[T] = Dataset(this, summon[Encoder[T]])

  /** Select columns matching a regex pattern. */
  def colRegex(colName: String): Column =
    Column(Expression.newBuilder()
      .setUnresolvedRegex(Expression.UnresolvedRegex.newBuilder()
        .setColName(colName).build())
      .build())

  /** Access a metadata column by name (e.g., `_metadata`). */
  def metadataColumn(colName: String): Column =
    Column(Expression.newBuilder()
      .setUnresolvedAttribute(Expression.UnresolvedAttribute.newBuilder()
        .setUnparsedIdentifier(colName).setIsMetadataColumn(true).build())
      .build())

  /** Attach metadata (as JSON string) to a column. */
  def withMetadata(columnName: String, metadata: String): DataFrame =
    withRelation(_.setWithColumns(
      WithColumns.newBuilder()
        .setInput(relation)
        .addAliases(Expression.Alias.newBuilder()
          .setExpr(Column(columnName).expr)
          .addName(columnName)
          .setMetadata(metadata)
          .build())
        .build()
    ))

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Execute a relation and collect rows + observed metrics. */
  private[sql] def executeAndCollect(rel: Relation): (Array[Row], Seq[(Long, Row)]) =
    val plan = Plan.newBuilder().setRoot(rel).build()
    val responses = client.execute(plan)
    try
      val rows = mutable.ArrayBuffer.empty[Row]
      val observedMetrics = mutable.ArrayBuffer.empty[(Long, Row)]
      responses.foreach { resp =>
        if resp.hasArrowBatch then
          rows ++= ArrowDeserializer.fromArrowBatch(resp.getArrowBatch.getData.toByteArray)
        resp.getObservedMetricsList.asScala.foreach { om =>
          val keys = om.getKeysList.asScala.toSeq
          val values =
            om.getValuesList.asScala.map(LiteralValueProtoConverter.toScalaValue).toSeq
          val schema = StructType(
            keys.zip(om.getValuesList.asScala).map { (key, lit) =>
              StructField(key, LiteralValueProtoConverter.toDataType(lit))
            }.toSeq
          )
          val row = Row.fromSeqWithSchema(values, schema)
          observedMetrics += ((om.getPlanId, row))
        }
      }
      (rows.toArray, observedMetrics.toSeq)
    finally
      (responses: Any) match
        case c: AutoCloseable => c.close()
        case _                => ()

  /** Resolve schema from the server. */
  private def schemaInternal(): Option[StructType] =
    val plan = Plan.newBuilder().setRoot(relation).build()
    val resp = client.analyzeSchema(plan)
    if resp.hasSchema && resp.getSchema.hasSchema then
      DataTypeProtoConverter.fromProto(resp.getSchema.getSchema) match
        case st: StructType => Some(st)
        case _              => None
    else None

  private[sql] def withRelation(f: Relation.Builder => Relation.Builder): DataFrame =
    val builder = Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(session.nextPlanId()).build())
    DataFrame(session, f(builder).build())

  /** Build a Relation; if cols contain subquery references, wrap with WithRelations. */
  private[sql] def withRelation(cols: Seq[Column])(
      f: Relation.Builder => Relation.Builder
  ): DataFrame =
    val allRefs = cols.flatMap(_.subqueryRelations)
    if allRefs.isEmpty then withRelation(f)
    else
      val innerBuilder = Relation.newBuilder()
        .setCommon(RelationCommon.newBuilder().setPlanId(session.nextPlanId()).build())
      val innerRel = f(innerBuilder).build()
      val uniqueRefs = allRefs.distinctBy(_.getCommon.getPlanId)
      val wrRel = Relation.newBuilder()
        .setCommon(RelationCommon.newBuilder().setPlanId(session.nextPlanId()).build())
        .setWithRelations(
          WithRelations.newBuilder()
            .setRoot(innerRel)
            .addAllReferences(uniqueRefs.asJava)
            .build()
        )
        .build()
      DataFrame(session, wrRel)

  private def setOp(
      other: DataFrame,
      opType: SetOperation.SetOpType,
      byName: Boolean,
      allowMissingColumns: Boolean = false,
      isAll: Boolean = false
  ): DataFrame =
    withRelation(_.setSetOp(
      SetOperation.newBuilder()
        .setLeftInput(relation)
        .setRightInput(other.relation)
        .setSetOpType(opType)
        .setIsAll(isAll)
        .setByName(byName)
        .setAllowMissingColumns(allowMissingColumns)
        .build()
    ))

  private[sql] def toJoinType(s: String): Join.JoinType =
    s.toLowerCase match
      case "inner"                        => Join.JoinType.JOIN_TYPE_INNER
      case "left" | "leftouter"           => Join.JoinType.JOIN_TYPE_LEFT_OUTER
      case "right" | "rightouter"         => Join.JoinType.JOIN_TYPE_RIGHT_OUTER
      case "full" | "outer" | "fullouter" => Join.JoinType.JOIN_TYPE_FULL_OUTER
      case "cross"                        => Join.JoinType.JOIN_TYPE_CROSS
      case "semi" | "leftsemi"            => Join.JoinType.JOIN_TYPE_LEFT_SEMI
      case "anti" | "leftanti"            => Join.JoinType.JOIN_TYPE_LEFT_ANTI
      case _                              => Join.JoinType.JOIN_TYPE_INNER

object DataFrame:
  private[sql] def apply(session: SparkSession, relation: Relation): DataFrame =
    new DataFrame(session, relation)
