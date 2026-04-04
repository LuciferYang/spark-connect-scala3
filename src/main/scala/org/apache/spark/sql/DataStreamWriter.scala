package org.apache.spark.sql

import org.apache.spark.connect.proto.*

/** Trigger types for structured streaming queries. */
sealed trait Trigger
object Trigger:
  case class ProcessingTime(intervalMs: Long) extends Trigger
  case object AvailableNow extends Trigger
  case object Once extends Trigger
  case class Continuous(intervalMs: Long) extends Trigger

/** Writer for starting streaming queries from a streaming DataFrame.
  *
  * {{{
  *   df.writeStream
  *     .format("console")
  *     .trigger(Trigger.ProcessingTime(1000))
  *     .start()
  * }}}
  */
final class DataStreamWriter private[sql] (private val df: DataFrame):
  private var source: String = ""
  private var mode: String = ""
  private var triggerOpt: Option[Trigger] = None
  private var name: String = ""
  private var opts: Map[String, String] = Map.empty
  private var partitionCols: Seq[String] = Seq.empty

  def format(fmt: String): DataStreamWriter =
    source = fmt
    this

  def outputMode(m: String): DataStreamWriter =
    mode = m
    this

  def trigger(t: Trigger): DataStreamWriter =
    triggerOpt = Some(t)
    this

  def queryName(qn: String): DataStreamWriter =
    name = qn
    this

  def option(key: String, value: String): DataStreamWriter =
    opts = opts + (key -> value)
    this

  def options(m: Map[String, String]): DataStreamWriter =
    opts = opts ++ m
    this

  def partitionBy(colNames: String*): DataStreamWriter =
    partitionCols = colNames.toSeq
    this

  def start(): StreamingQuery = doStart(None, None)

  def start(path: String): StreamingQuery = doStart(Some(path), None)

  def toTable(tableName: String): StreamingQuery = doStart(None, Some(tableName))

  private def doStart(
      path: Option[String],
      tableName: Option[String]
  ): StreamingQuery =
    val builder = buildWriteStreamOp()
    path.foreach(builder.setPath)
    tableName.foreach(builder.setTableName)
    val command = Command.newBuilder()
      .setWriteStreamOperationStart(builder.build())
      .build()
    val plan = Plan.newBuilder().setCommand(command).build()
    val responses = df.session.client.execute(plan)
    var queryId = ""
    var runId = ""
    var queryName: Option[String] = None
    responses.foreach { resp =>
      if resp.hasWriteStreamOperationStartResult then
        val result = resp.getWriteStreamOperationStartResult
        queryId = result.getQueryId.getId
        runId = result.getQueryId.getRunId
        if result.getName.nonEmpty then queryName = Some(result.getName)
    }
    StreamingQuery(df.session, queryId, runId, queryName)

  private[sql] def buildWriteStreamOp(): WriteStreamOperationStart.Builder =
    val builder = WriteStreamOperationStart.newBuilder()
      .setInput(df.relation)
    if source.nonEmpty then builder.setFormat(source)
    if mode.nonEmpty then builder.setOutputMode(mode)
    if name.nonEmpty then builder.setQueryName(name)
    opts.foreach((k, v) => builder.putOptions(k, v))
    partitionCols.foreach(builder.addPartitioningColumnNames)
    triggerOpt.foreach(setTrigger(builder, _))
    builder

  private def setTrigger(builder: WriteStreamOperationStart.Builder, t: Trigger): Unit =
    t match
      case Trigger.ProcessingTime(ms) =>
        builder.setProcessingTimeInterval(s"$ms milliseconds")
      case Trigger.AvailableNow =>
        builder.setAvailableNow(true)
      case Trigger.Once =>
        builder.setOnce(true)
      case Trigger.Continuous(ms) =>
        builder.setContinuousCheckpointInterval(s"$ms milliseconds")
