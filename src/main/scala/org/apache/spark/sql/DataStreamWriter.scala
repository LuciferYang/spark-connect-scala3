package org.apache.spark.sql

import com.google.protobuf.ByteString
import org.apache.spark.connect.proto.*
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders
import org.apache.spark.sql.connect.client.DataTypeProtoConverter
import org.apache.spark.sql.connect.common.ForeachWriterPacket
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryStartedEvent
import org.apache.spark.sql.types.NullType

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
  private var clusteringCols: Seq[String] = Seq.empty
  private var foreachBatchPayload: Option[Array[Byte]] = None
  private var foreachWriterPayload: Option[Array[Byte]] = None

  def format(fmt: String): DataStreamWriter =
    source = fmt
    this

  def outputMode(m: String): DataStreamWriter =
    mode = m
    this

  def outputMode(outputMode: streaming.OutputMode): DataStreamWriter =
    this.outputMode(outputMode.toString)

  def trigger(t: Trigger): DataStreamWriter =
    triggerOpt = Some(t)
    this

  def queryName(qn: String): DataStreamWriter =
    name = qn
    this

  def option(key: String, value: String): DataStreamWriter =
    opts = opts + (key -> value)
    this

  def option(key: String, value: Boolean): DataStreamWriter = option(key, value.toString)
  def option(key: String, value: Long): DataStreamWriter = option(key, value.toString)
  def option(key: String, value: Double): DataStreamWriter = option(key, value.toString)

  def options(m: Map[String, String]): DataStreamWriter =
    opts = opts ++ m
    this

  def partitionBy(colNames: String*): DataStreamWriter =
    partitionCols = colNames.toSeq
    this

  def clusterBy(colNames: String*): DataStreamWriter =
    clusteringCols = colNames.toSeq
    this

  /** Set a function to process each micro-batch DataFrame with its batch ID. */
  def foreachBatch(func: (DataFrame, Long) => Unit): DataStreamWriter =
    val packet = ForeachWriterPacket(func.asInstanceOf[AnyRef], AgnosticEncoders.UnboundRowEncoder)
    foreachBatchPayload = Some(ForeachWriterPacket.serialize(packet))
    this

  /** Set a ForeachWriter to process each row of the streaming query output. */
  def foreach(writer: ForeachWriter[?]): DataStreamWriter =
    val packet =
      ForeachWriterPacket(writer.asInstanceOf[AnyRef], AgnosticEncoders.UnboundRowEncoder)
    foreachWriterPayload = Some(ForeachWriterPacket.serialize(packet))
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
        // Dispatch QueryStartedEvent synchronously before returning
        if result.hasQueryStartedEventJson then
          val event = QueryStartedEvent.fromJson(result.getQueryStartedEventJson)
          df.session.streams.streamingQueryListenerBus.postToAll(event)
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
    clusteringCols.foreach(builder.addClusteringColumnNames)
    triggerOpt.foreach(setTrigger(builder, _))
    foreachWriterPayload.foreach { payload =>
      val scalaWriterBuilder = ScalarScalaUDF
        .newBuilder()
        .setPayload(ByteString.copyFrom(payload))
      builder.getForeachWriterBuilder.setScalaFunction(scalaWriterBuilder)
    }
    foreachBatchPayload.foreach { payload =>
      builder.getForeachBatchBuilder.getScalaFunctionBuilder
        .setPayload(ByteString.copyFrom(payload))
        .setOutputType(DataTypeProtoConverter.toProto(NullType))
        .setNullable(true)
    }
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
