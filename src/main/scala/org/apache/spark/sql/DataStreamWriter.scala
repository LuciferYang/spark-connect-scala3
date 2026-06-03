package org.apache.spark.sql

import com.google.protobuf.ByteString
import org.apache.spark.connect.proto.*
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, AgnosticEncoders}
import org.apache.spark.sql.connect.client.DataTypeProtoConverter
import org.apache.spark.sql.connect.common.ForeachWriterPacket
import org.apache.spark.sql.execution.streaming.*
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryStartedEvent
import org.apache.spark.sql.streaming.{Trigger as StreamingTrigger}
import org.apache.spark.sql.types.NullType

import java.util.Locale

/** Writer for starting streaming queries from a streaming DataFrame.
  *
  * {{{
  *   import org.apache.spark.sql.streaming.Trigger
  *
  *   df.writeStream
  *     .format("console")
  *     .trigger(Trigger.ProcessingTime(1000))
  *     .start()
  * }}}
  */
final class DataStreamWriter[T] private[sql] (
    private val df: DataFrame,
    private[sql] val encoder: AgnosticEncoder[?]
):
  private var source: String = ""
  private var mode: String = ""
  private var triggerOpt: Option[StreamingTrigger] = None
  private var name: String = ""
  private var opts: Map[String, String] = Map.empty
  private var partitionCols: Seq[String] = Seq.empty
  private var clusteringCols: Seq[String] = Seq.empty
  private var foreachBatchPayload: Option[Array[Byte]] = None
  private var foreachWriterPayload: Option[Array[Byte]] = None

  def format(fmt: String): DataStreamWriter[T] =
    source = fmt
    this

  def outputMode(m: String): DataStreamWriter[T] =
    mode = m
    this

  def outputMode(outputMode: streaming.OutputMode): DataStreamWriter[T] =
    this.outputMode(outputMode.toString.toLowerCase(Locale.ROOT))

  def trigger(t: StreamingTrigger): this.type =
    t match
      case _: RealTimeTrigger =>
        throw new MatchError(t)
      case _: ProcessingTimeTrigger | AvailableNowTrigger | OneTimeTrigger | _: ContinuousTrigger =>
        triggerOpt = Some(t)
        this
      case _ =>
        throw new MatchError(t)

  def queryName(qn: String): DataStreamWriter[T] =
    name = qn
    this

  def option(key: String, value: String): DataStreamWriter[T] =
    opts = opts + (key -> value)
    this

  def option(key: String, value: Boolean): DataStreamWriter[T] = option(key, value.toString)
  def option(key: String, value: Long): DataStreamWriter[T] = option(key, value.toString)
  def option(key: String, value: Double): DataStreamWriter[T] = option(key, value.toString)

  def options(m: Map[String, String]): DataStreamWriter[T] =
    opts = opts ++ m
    this

  def partitionBy(colNames: String*): DataStreamWriter[T] =
    partitionCols = colNames.toSeq
    this

  def clusterBy(colNames: String*): DataStreamWriter[T] =
    clusteringCols = colNames.toSeq
    this

  /** Set a function to process each micro-batch DataFrame with its batch ID. */
  def foreachBatch(func: (DataFrame, Long) => Unit): DataStreamWriter[T] =
    val packet = ForeachWriterPacket(func.asInstanceOf[AnyRef], encoder)
    foreachBatchPayload = Some(ForeachWriterPacket.serialize(packet))
    this

  /** Set a ForeachWriter to process each row of the streaming query output. */
  def foreach(writer: ForeachWriter[T]): DataStreamWriter[T] =
    val packet = ForeachWriterPacket(writer.asInstanceOf[AnyRef], encoder)
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
    try
      responses.foreach { resp =>
        if resp.hasWriteStreamOperationStartResult then
          val result = resp.getWriteStreamOperationStartResult
          queryId = result.getQueryId.getId
          runId = result.getQueryId.getRunId
          if result.getName.nonEmpty then queryName = Some(result.getName)
          if result.hasQueryStartedEventJson then
            val event = QueryStartedEvent.fromJson(result.getQueryStartedEventJson)
            df.session.streams.streamingQueryListenerBus.postToAll(event)
      }
    finally
      (responses: Any) match
        case c: AutoCloseable => c.close()
        case _                => ()
    if queryId.isEmpty || runId.isEmpty then
      throw new RuntimeException(
        "Server response did not contain WriteStreamOperationStartResult with query IDs"
      )
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

  private def setTrigger(builder: WriteStreamOperationStart.Builder, t: StreamingTrigger): Unit =
    t match
      case ProcessingTimeTrigger(intervalMs) =>
        builder.setProcessingTimeInterval(s"$intervalMs milliseconds")
      case AvailableNowTrigger =>
        builder.setAvailableNow(true)
      case OneTimeTrigger =>
        builder.setOnce(true)
      case ContinuousTrigger(intervalMs) =>
        builder.setContinuousCheckpointInterval(s"$intervalMs milliseconds")
      case _: RealTimeTrigger =>
        throw UnsupportedOperationException(
          "Trigger.RealTime is not supported by the Spark Connect write stream proto"
        )
      case _ =>
        throw new MatchError(t)

private[sql] object DataStreamWriter:
  /** Convenience constructor: untyped writer over a DataFrame, defaults to Row encoder. */
  def apply(df: DataFrame): DataStreamWriter[Row] =
    new DataStreamWriter[Row](df, AgnosticEncoders.UnboundRowEncoder)
