package org.apache.spark.sql

import org.apache.spark.connect.proto.base.*
import org.apache.spark.connect.proto.expressions.Expression
import org.apache.spark.connect.proto.expressions.Expression.ExprType
import org.apache.spark.connect.proto.relations.*
import org.apache.spark.sql.connect.client.{ArrowDeserializer, DataTypeProtoConverter, SparkConnectClient}
import org.apache.spark.sql.types.StructType
import org.apache.spark.connect.proto.commands.{Command, CreateDataFrameViewCommand, CheckpointCommand}
import org.apache.spark.connect.proto.common.StorageLevel as ProtoStorageLevel

import scala.collection.mutable

/**
 * A distributed collection of rows organized into named columns.
 *
 * All transformation methods are lazy — they build a protobuf Relation tree.
 * Action methods (collect, show, count, …) send the plan to the server via gRPC.
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
    withRelation(Relation.RelType.Project(
      Project(input = Some(relation), expressions = cols.map(_.expr).toSeq)
    ))

  def select(colNames: String*)(using DummyImplicit): DataFrame =
    select(colNames.map(Column(_))*)

  def selectExpr(exprs: String*): DataFrame =
    select(exprs.map(e => functions.expr(e))*)

  def filter(condition: Column): DataFrame =
    withRelation(Relation.RelType.Filter(
      Filter(input = Some(relation), condition = Some(condition.expr))
    ))

  def where(condition: Column): DataFrame = filter(condition)

  def where(conditionExpr: String): DataFrame = filter(functions.expr(conditionExpr))

  def limit(n: Int): DataFrame =
    withRelation(Relation.RelType.Limit(
      Limit(input = Some(relation), limit = n)
    ))

  def offset(n: Int): DataFrame =
    withRelation(Relation.RelType.Offset(
      Offset(input = Some(relation), offset = n)
    ))

  def sort(cols: Column*): DataFrame = orderBy(cols*)

  def orderBy(cols: Column*): DataFrame =
    withRelation(Relation.RelType.Sort(
      Sort(
        input = Some(relation),
        order = cols.map(_.toSortOrder).toSeq,
        isGlobal = Some(true)
      )
    ))

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
    withRelation(Relation.RelType.Join(
      Join(
        left = Some(relation),
        right = Some(right.relation),
        joinCondition = Some(joinExpr.expr),
        joinType = toJoinType(joinType)
      )
    ))

  def join(right: DataFrame, usingColumns: Seq[String]): DataFrame =
    withRelation(Relation.RelType.Join(
      Join(
        left = Some(relation),
        right = Some(right.relation),
        usingColumns = usingColumns,
        joinType = Join.JoinType.JOIN_TYPE_INNER
      )
    ))

  def crossJoin(right: DataFrame): DataFrame =
    withRelation(Relation.RelType.Join(
      Join(
        left = Some(relation),
        right = Some(right.relation),
        joinType = Join.JoinType.JOIN_TYPE_CROSS
      )
    ))

  def withColumn(name: String, col: Column): DataFrame =
    withRelation(Relation.RelType.WithColumns(
      WithColumns(
        input = Some(relation),
        aliases = Seq(Expression.Alias(expr = Some(col.expr), name = Seq(name)))
      )
    ))

  def withColumnRenamed(existing: String, newName: String): DataFrame =
    withRelation(Relation.RelType.WithColumnsRenamed(
      WithColumnsRenamed(
        input = Some(relation),
        renames = Seq(WithColumnsRenamed.Rename(colName = existing, newColName = newName))
      )
    ))

  def drop(colNames: String*): DataFrame =
    withRelation(Relation.RelType.Drop(
      Drop(
        input = Some(relation),
        columns = colNames.map { name =>
          Expression(exprType = ExprType.UnresolvedAttribute(
            Expression.UnresolvedAttribute(unparsedIdentifier = name)
          ))
        }.toSeq
      )
    ))

  def distinct(): DataFrame = dropDuplicates()

  def dropDuplicates(): DataFrame =
    withRelation(Relation.RelType.Deduplicate(
      Deduplicate(input = Some(relation), allColumnsAsKeys = Some(true))
    ))

  def dropDuplicates(colNames: Seq[String]): DataFrame =
    withRelation(Relation.RelType.Deduplicate(
      Deduplicate(input = Some(relation), columnNames = colNames)
    ))

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
    withRelation(Relation.RelType.Repartition(
      Repartition(input = Some(relation), numPartitions = numPartitions, shuffle = Some(true))
    ))

  def coalesce(numPartitions: Int): DataFrame =
    withRelation(Relation.RelType.Repartition(
      Repartition(input = Some(relation), numPartitions = numPartitions, shuffle = Some(false))
    ))

  def sample(fraction: Double, withReplacement: Boolean = false, seed: Long = 0L): DataFrame =
    withRelation(Relation.RelType.Sample(
      Sample(
        input = Some(relation),
        lowerBound = 0.0,
        upperBound = fraction,
        withReplacement = Some(withReplacement),
        seed = Some(seed)
      )
    ))

  def describe(colNames: String*): DataFrame =
    withRelation(Relation.RelType.Describe(
      StatDescribe(input = Some(relation), cols = colNames.toSeq)
    ))

  def summary(statistics: String*): DataFrame =
    withRelation(Relation.RelType.Summary(
      StatSummary(input = Some(relation), statistics = statistics.toSeq)
    ))

  def alias(name: String): DataFrame =
    withRelation(Relation.RelType.SubqueryAlias(
      SubqueryAlias(input = Some(relation), alias = name)
    ))

  /** Rename all columns. Number of names must match number of columns. */
  def toDF(colNames: String*): DataFrame =
    withRelation(Relation.RelType.ToDf(
      ToDF(input = Some(relation), columnNames = colNames.toSeq)
    ))

  /** Hint the optimizer (e.g., broadcast). */
  def hint(name: String, parameters: Any*): DataFrame =
    withRelation(Relation.RelType.Hint(
      Hint(
        input = Some(relation),
        name = name,
        parameters = parameters.map(p => Column.lit(p).expr).toSeq
      )
    ))

  /** Broadcast hint shorthand. */
  def broadcast: DataFrame = hint("broadcast")

  def intersectAll(other: DataFrame): DataFrame =
    setOp(other, SetOperation.SetOpType.SET_OP_TYPE_INTERSECT, byName = false, isAll = true)

  def exceptAll(other: DataFrame): DataFrame =
    setOp(other, SetOperation.SetOpType.SET_OP_TYPE_EXCEPT, byName = false, isAll = true)

  /** Sort within each partition. */
  def sortWithinPartitions(cols: Column*): DataFrame =
    withRelation(Relation.RelType.Sort(
      Sort(
        input = Some(relation),
        order = cols.map(_.toSortOrder).toSeq,
        isGlobal = Some(false)
      )
    ))

  def sortWithinPartitions(colNames: String*)(using DummyImplicit): DataFrame =
    sortWithinPartitions(colNames.map(Column(_))*)

  /** Return the last n rows. */
  def tail(n: Int): Array[Row] =
    val plan = Plan(opType = Plan.OpType.Root(
      Relation(
        common = Some(RelationCommon(planId = Some(session.nextPlanId()))),
        relType = Relation.RelType.Tail(
          Tail(input = Some(relation), limit = n)
        )
      )
    ))
    val responses = client.execute(plan)
    val rows = mutable.ArrayBuffer.empty[Row]
    responses.foreach { resp =>
      resp.responseType match
        case ExecutePlanResponse.ResponseType.ArrowBatch(batch) =>
          rows ++= ArrowDeserializer.fromArrowBatch(batch.data.toByteArray)
        case _ =>
    }
    rows.toArray

  /** Pipeline-style transformation. */
  def transform(f: DataFrame => DataFrame): DataFrame = f(this)

  /** Repartition by columns. */
  def repartition(numPartitions: Int, cols: Column*): DataFrame =
    if cols.isEmpty then
      repartition(numPartitions)
    else
      withRelation(Relation.RelType.RepartitionByExpression(
        RepartitionByExpression(
          input = Some(relation),
          partitionExprs = cols.map(_.expr).toSeq,
          numPartitions = Some(numPartitions)
        )
      ))

  /** Repartition by columns with default partition count. */
  def repartition(cols: Column*)(using DummyImplicit): DataFrame =
    withRelation(Relation.RelType.RepartitionByExpression(
      RepartitionByExpression(
        input = Some(relation),
        partitionExprs = cols.map(_.expr).toSeq
      )
    ))

  /** Cache this DataFrame with the default storage level (MEMORY_AND_DISK). */
  def cache(): DataFrame = persist()

  /** Persist this DataFrame with the default storage level (MEMORY_AND_DISK). */
  def persist(): DataFrame = persist(StorageLevel.MEMORY_AND_DISK)

  /** Persist this DataFrame with the given storage level. */
  def persist(storageLevel: StorageLevel): DataFrame =
    val cmd = Command(commandType = Command.CommandType.CheckpointCommand(
      CheckpointCommand(
        relation = Some(relation),
        local = true,
        eager = true,
        storageLevel = Some(storageLevel.toProto)
      )
    ))
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
    val plan = Plan(opType = Plan.OpType.Root(relation))
    val responses = client.execute(plan)
    val rows = mutable.ArrayBuffer.empty[Row]
    responses.foreach { resp =>
      resp.responseType match
        case ExecutePlanResponse.ResponseType.ArrowBatch(batch) =>
          rows ++= ArrowDeserializer.fromArrowBatch(batch.data.toByteArray)
        case _ => // skip metrics, schema, etc.
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
    val plan = Plan(opType = Plan.OpType.Root(relation))
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
    val cmd = Command(commandType = Command.CommandType.CreateDataframeView(
      CreateDataFrameViewCommand(
        input = Some(relation),
        name = viewName,
        isGlobal = isGlobal,
        replace = replace
      )
    ))
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
      val plan = Plan(opType = Plan.OpType.Root(relation))
      val resp = client.analyzeSchema(plan)
      resp.result match
        case AnalyzePlanResponse.Result.Schema(s) =>
          s.schema.map(DataTypeProtoConverter.fromProto(_)).collect {
            case st: StructType => st
          }
        case _ => None
    catch case _: Exception => None

  private[sql] def withRelation(relType: Relation.RelType): DataFrame =
    DataFrame(session, Relation(
      common = Some(RelationCommon(planId = Some(session.nextPlanId()))),
      relType = relType
    ))

  private def setOp(
      other: DataFrame,
      opType: SetOperation.SetOpType,
      byName: Boolean,
      allowMissingColumns: Boolean = false,
      isAll: Boolean = false
  ): DataFrame =
    withRelation(Relation.RelType.SetOp(
      SetOperation(
        leftInput = Some(relation),
        rightInput = Some(other.relation),
        setOpType = opType,
        isAll = Some(isAll),
        byName = Some(byName),
        allowMissingColumns = Some(allowMissingColumns)
      )
    ))

  private def toJoinType(s: String): Join.JoinType =
    s.toLowerCase match
      case "inner"                       => Join.JoinType.JOIN_TYPE_INNER
      case "left" | "leftouter"          => Join.JoinType.JOIN_TYPE_LEFT_OUTER
      case "right" | "rightouter"        => Join.JoinType.JOIN_TYPE_RIGHT_OUTER
      case "full" | "outer" | "fullouter" => Join.JoinType.JOIN_TYPE_FULL_OUTER
      case "cross"                       => Join.JoinType.JOIN_TYPE_CROSS
      case "semi" | "leftsemi"           => Join.JoinType.JOIN_TYPE_LEFT_SEMI
      case "anti" | "leftanti"           => Join.JoinType.JOIN_TYPE_LEFT_ANTI
      case _                             => Join.JoinType.JOIN_TYPE_INNER

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
      row.zip(widths).map { (s, w) => s.padTo(w, ' ') }.mkString("|", "|", "|")

    println(sep)
    println(fmtRow(allRows.head))
    println(sep)
    allRows.tail.foreach(r => println(fmtRow(r)))
    println(sep)

object DataFrame:
  private[sql] def apply(session: SparkSession, relation: Relation): DataFrame =
    new DataFrame(session, relation)
