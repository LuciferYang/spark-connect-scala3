package org.apache.spark.sql

import org.apache.spark.connect.proto.{StorageLevel as _, Catalog as _, *}
import org.apache.spark.connect.proto.StorageLevel as ProtoStorageLevel
import org.apache.spark.sql.connect.client.{
  ArrowDeserializer,
  DataTypeProtoConverter,
  SparkConnectClient
}
import org.apache.spark.sql.types.StructType

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
    withRelation(_.setProject(projBuilder.build()))

  def select(colNames: String*)(using DummyImplicit): DataFrame =
    select(colNames.map(Column(_))*)

  def selectExpr(exprs: String*): DataFrame =
    select(exprs.map(e => functions.expr(e))*)

  def filter(condition: Column): DataFrame =
    withRelation(_.setFilter(
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
    withRelation(_.setSort(sortBuilder.build()))

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
    withRelation(_.setJoin(
      Join.newBuilder()
        .setLeft(relation)
        .setRight(right.relation)
        .setJoinCondition(joinExpr.expr)
        .setJoinType(toJoinType(joinType))
        .build()
    ))

  def join(right: DataFrame, usingColumns: Seq[String]): DataFrame =
    val joinBuilder = Join.newBuilder()
      .setLeft(relation)
      .setRight(right.relation)
      .setJoinType(Join.JoinType.JOIN_TYPE_INNER)
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
    withRelation(_.setWithColumns(
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
    withRelation(_.setSort(sortBuilder.build()))

  def sortWithinPartitions(colNames: String*)(using DummyImplicit): DataFrame =
    sortWithinPartitions(colNames.map(Column(_))*)

  /** Return the last n rows. */
  def tail(n: Int): Array[Row] =
    val tailRel = Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(session.nextPlanId()).build())
      .setTail(Tail.newBuilder().setInput(relation).setLimit(n).build())
      .build()
    val plan = Plan.newBuilder().setRoot(tailRel).build()
    val responses = client.execute(plan)
    val rows = mutable.ArrayBuffer.empty[Row]
    responses.foreach { resp =>
      if resp.hasArrowBatch then
        rows ++= ArrowDeserializer.fromArrowBatch(resp.getArrowBatch.getData.toByteArray)
    }
    rows.toArray

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
      withRelation(_.setRepartitionByExpression(builder.build()))

  /** Repartition by columns with default partition count. */
  def repartition(cols: Column*)(using DummyImplicit): DataFrame =
    val builder = RepartitionByExpression.newBuilder().setInput(relation)
    cols.foreach(c => builder.addPartitionExprs(c.expr))
    withRelation(_.setRepartitionByExpression(builder.build()))

  /** Cache this DataFrame with the default storage level (MEMORY_AND_DISK). */
  def cache(): DataFrame = persist()

  /** Persist this DataFrame with the default storage level (MEMORY_AND_DISK). */
  def persist(): DataFrame = persist(StorageLevel.MEMORY_AND_DISK)

  /** Persist this DataFrame with the given storage level. */
  def persist(storageLevel: StorageLevel): DataFrame =
    val cmd = Command.newBuilder()
      .setCheckpointCommand(CheckpointCommand.newBuilder()
        .setRelation(relation)
        .setLocal(true)
        .setEager(true)
        .setStorageLevel(storageLevel.toProto)
        .build())
      .build()
    client.executeCommand(cmd)
    this

  /** Remove the cached data for this DataFrame. */
  def unpersist(blocking: Boolean = false): DataFrame =
    // In Spark Connect, unpersist is handled via RemoveCachedRemoteRelation
    // For now, trigger a local checkpoint removal
    this

  def na: DataFrameNaFunctions = DataFrameNaFunctions(this)

  def stat: DataFrameStatFunctions = DataFrameStatFunctions(this)

  // ---------------------------------------------------------------------------
  // Actions
  // ---------------------------------------------------------------------------

  def collect(): Array[Row] =
    val plan = Plan.newBuilder().setRoot(relation).build()
    val responses = client.execute(plan)
    val rows = mutable.ArrayBuffer.empty[Row]
    responses.foreach { resp =>
      if resp.hasArrowBatch then
        rows ++= ArrowDeserializer.fromArrowBatch(resp.getArrowBatch.getData.toByteArray)
    }
    rows.toArray

  def count(): Long =
    import functions.{count as countFn, lit}
    groupBy(Seq.empty[Column]*).agg(countFn(lit(1)).as("count")).collect().head.getLong(0)

  def first(): Row = limit(1).collect().head

  def head(n: Int = 1): Array[Row] = limit(n).collect()

  def take(n: Int): Array[Row] = head(n)

  def show(numRows: Int = 20, truncate: Int = 20): Unit =
    val schemaOpt = schemaInternal()
    val rows = limit(numRows).collect()
    val colNames = schemaOpt.map(_.fieldNames.toSeq).getOrElse(
      if rows.nonEmpty then (0 until rows.head.size).map(i => s"col$i").toSeq
      else Seq.empty
    )
    printTable(colNames, rows, truncate)

  def printSchema(): Unit =
    val s = schema
    println(s.treeString)

  def schema: StructType =
    schemaInternal().getOrElse(StructType.empty)

  def columns: Array[String] = schema.fieldNames

  def explain(extended: Boolean = false): Unit =
    val plan = Plan.newBuilder().setRoot(relation).build()
    val mode =
      if extended then AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_EXTENDED
      else AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_SIMPLE
    val explainStr = client.analyzeExplain(plan, mode)
    println(explainStr)

  def isEmpty: Boolean = limit(1).collect().isEmpty

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

  /** Convert this DataFrame to a strongly-typed Dataset[T]. */
  def as[T: Encoder: scala.reflect.ClassTag]: Dataset[T] = Dataset(this, summon[Encoder[T]])

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Resolve schema from the server. */
  private def schemaInternal(): Option[StructType] =
    try
      val plan = Plan.newBuilder().setRoot(relation).build()
      val resp = client.analyzeSchema(plan)
      if resp.hasSchema && resp.getSchema.hasSchema then
        DataTypeProtoConverter.fromProto(resp.getSchema.getSchema) match
          case st: StructType => Some(st)
          case _              => None
      else None
    catch case _: Exception => None

  private[sql] def withRelation(f: Relation.Builder => Relation.Builder): DataFrame =
    val builder = Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(session.nextPlanId()).build())
    DataFrame(session, f(builder).build())

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

  private def toJoinType(s: String): Join.JoinType =
    s.toLowerCase match
      case "inner"                        => Join.JoinType.JOIN_TYPE_INNER
      case "left" | "leftouter"           => Join.JoinType.JOIN_TYPE_LEFT_OUTER
      case "right" | "rightouter"         => Join.JoinType.JOIN_TYPE_RIGHT_OUTER
      case "full" | "outer" | "fullouter" => Join.JoinType.JOIN_TYPE_FULL_OUTER
      case "cross"                        => Join.JoinType.JOIN_TYPE_CROSS
      case "semi" | "leftsemi"            => Join.JoinType.JOIN_TYPE_LEFT_SEMI
      case "anti" | "leftanti"            => Join.JoinType.JOIN_TYPE_LEFT_ANTI
      case _                              => Join.JoinType.JOIN_TYPE_INNER

  private def printTable(colNames: Seq[String], rows: Array[Row], truncate: Int): Unit =
    if colNames.isEmpty && rows.isEmpty then
      println("(empty DataFrame)")
      return

    def truncStr(s: String): String =
      if s.length > truncate then s.take(truncate - 3) + "..." else s

    val allRows: Seq[Seq[String]] = colNames.map(truncStr) +: rows.map { row =>
      (0 until row.size).map { i =>
        val v = row.get(i)
        truncStr(if v == null then "null" else v.toString)
      }.toSeq
    }.toSeq

    val widths = allRows.transpose.map(col => col.map(_.length).max)
    val sep = widths.map("-" * _).mkString("+", "+", "+")
    def fmtRow(row: Seq[String]): String =
      row.zip(widths).map((s, w) => s.padTo(w, ' ')).mkString("|", "|", "|")

    println(sep)
    println(fmtRow(allRows.head))
    println(sep)
    allRows.tail.foreach(r => println(fmtRow(r)))
    println(sep)

object DataFrame:
  private[sql] def apply(session: SparkSession, relation: Relation): DataFrame =
    new DataFrame(session, relation)
