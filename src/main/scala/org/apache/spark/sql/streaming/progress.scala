package org.apache.spark.sql.streaming

import java.{util => ju}
import java.lang.{Long => JLong}
import java.util.UUID

import scala.jdk.CollectionConverters.*

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode}

/** Information about updates made to stateful operators in a [[StreamingQuery]] during a
  * trigger.
  */
class StateOperatorProgress private[spark] (
    val operatorName: String,
    val numRowsTotal: Long,
    val numRowsUpdated: Long,
    val allUpdatesTimeMs: Long,
    val numRowsRemoved: Long,
    val allRemovalsTimeMs: Long,
    val commitTimeMs: Long,
    val memoryUsedBytes: Long,
    val numRowsDroppedByWatermark: Long,
    val numShufflePartitions: Long,
    val numStateStoreInstances: Long,
    val customMetrics: ju.Map[String, JLong] = new ju.HashMap()
) extends Serializable:

  def json: String = StreamingQueryProgress.mapper.writeValueAsString(toJsonNode)

  def prettyJson: String =
    StreamingQueryProgress.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(toJsonNode)

  override def toString: String = prettyJson

  private[sql] def toJsonNode: ObjectNode =
    val node = JsonNodeFactory.instance.objectNode()
    node.put("operatorName", operatorName)
    node.put("numRowsTotal", numRowsTotal)
    node.put("numRowsUpdated", numRowsUpdated)
    node.put("allUpdatesTimeMs", allUpdatesTimeMs)
    node.put("numRowsRemoved", numRowsRemoved)
    node.put("allRemovalsTimeMs", allRemovalsTimeMs)
    node.put("commitTimeMs", commitTimeMs)
    node.put("memoryUsedBytes", memoryUsedBytes)
    node.put("numRowsDroppedByWatermark", numRowsDroppedByWatermark)
    node.put("numShufflePartitions", numShufflePartitions)
    node.put("numStateStoreInstances", numStateStoreInstances)
    if !customMetrics.isEmpty then
      val cm = node.putObject("customMetrics")
      customMetrics.asScala.toSeq.sortBy(_._1).foreach { case (k, v) => cm.put(k, v.toLong) }
    node

end StateOperatorProgress

/** Information about progress made in the execution of a [[StreamingQuery]] during a trigger.
  *
  * Each event relates to processing done for a single trigger of the streaming query. Events
  * are emitted even when no new data is available to be processed.
  *
  * Note: `observedMetrics` from upstream is intentionally omitted in this client port — it
  * requires `GenericRowWithSchema` infrastructure not present in this thin client.
  */
class StreamingQueryProgress private[spark] (
    val id: UUID,
    val runId: UUID,
    val name: String,
    val timestamp: String,
    val batchId: Long,
    val batchDuration: Long,
    val durationMs: ju.Map[String, JLong],
    val eventTime: ju.Map[String, String],
    val stateOperators: Array[StateOperatorProgress],
    val sources: Array[SourceProgress],
    val sink: SinkProgress
) extends Serializable:

  /** The aggregate (across all sources) number of records processed in a trigger. */
  def numInputRows: Long = sources.map(_.numInputRows).sum

  /** The aggregate (across all sources) rate of data arriving. */
  def inputRowsPerSecond: Double = sources.map(_.inputRowsPerSecond).sum

  /** The aggregate (across all sources) rate at which Spark is processing data. */
  def processedRowsPerSecond: Double = sources.map(_.processedRowsPerSecond).sum

  /** The compact JSON representation of this progress. */
  def json: String = StreamingQueryProgress.mapper.writeValueAsString(toJsonNode)

  /** The pretty (i.e. indented) JSON representation of this progress. */
  def prettyJson: String =
    StreamingQueryProgress.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(toJsonNode)

  override def toString: String = prettyJson

  private def toJsonNode: ObjectNode =
    val node = JsonNodeFactory.instance.objectNode()
    node.put("id", id.toString)
    node.put("runId", runId.toString)
    node.put("name", name)
    node.put("timestamp", timestamp)
    node.put("batchId", batchId)
    node.put("batchDuration", batchDuration)
    node.put("numInputRows", numInputRows)
    StreamingQueryProgress.putFiniteDouble(node, "inputRowsPerSecond", inputRowsPerSecond)
    StreamingQueryProgress.putFiniteDouble(node, "processedRowsPerSecond", processedRowsPerSecond)
    StreamingQueryProgress.putLongMap(node, "durationMs", durationMs)
    StreamingQueryProgress.putStringMap(node, "eventTime", eventTime)
    val states = node.putArray("stateOperators")
    stateOperators.foreach(p => states.add(p.toJsonNode))
    val srcs = node.putArray("sources")
    sources.foreach(p => srcs.add(p.toJsonNode))
    if sink != null then node.set[ObjectNode]("sink", sink.toJsonNode)
    node

end StreamingQueryProgress

object StreamingQueryProgress:
  private[streaming] val mapper = new ObjectMapper()

  /** Parse a JSON string emitted by the Connect server into a typed `StreamingQueryProgress`. */
  def fromJson(json: String): StreamingQueryProgress =
    val node = mapper.readTree(json)
    new StreamingQueryProgress(
      id = readUuidOrZero(node, "id"),
      runId = readUuidOrZero(node, "runId"),
      name = readNullableString(node, "name"),
      timestamp = readNullableString(node, "timestamp"),
      batchId = readLong(node, "batchId"),
      batchDuration = readLong(node, "batchDuration"),
      durationMs = readLongMap(node, "durationMs"),
      eventTime = readStringMap(node, "eventTime"),
      stateOperators = readStateOperators(node.get("stateOperators")),
      sources = readSources(node.get("sources")),
      sink = readSink(node.get("sink"))
    )

  private def readUuidOrZero(node: JsonNode, field: String): UUID =
    val v = node.get(field)
    if v == null || v.isNull then new UUID(0L, 0L)
    else UUID.fromString(v.asText)

  private def readNullableString(node: JsonNode, field: String): String =
    val v = node.get(field)
    if v == null || v.isNull then null else v.asText

  private def readLong(node: JsonNode, field: String): Long =
    val v = node.get(field)
    if v == null || v.isNull then 0L else v.asLong()

  private def readDouble(node: JsonNode, field: String): Double =
    val v = node.get(field)
    if v == null || v.isNull then 0.0 else v.asDouble()

  private def readLongMap(node: JsonNode, field: String): ju.Map[String, JLong] =
    val out = new ju.HashMap[String, JLong]()
    val v = node.get(field)
    if v != null && v.isObject then
      v.properties().asScala.foreach(e => out.put(e.getKey, JLong.valueOf(e.getValue.asLong())))
    out

  private def readStringMap(node: JsonNode, field: String): ju.Map[String, String] =
    val out = new ju.HashMap[String, String]()
    val v = node.get(field)
    if v != null && v.isObject then
      v.properties().asScala.foreach(e => out.put(e.getKey, e.getValue.asText))
    out

  private def readMetrics(node: JsonNode): ju.Map[String, String] =
    val out = new ju.HashMap[String, String]()
    if node != null && node.isObject then
      node.properties().asScala.foreach(e => out.put(e.getKey, e.getValue.asText))
    out

  private def readStateOperators(node: JsonNode): Array[StateOperatorProgress] =
    if node == null || !node.isArray then Array.empty[StateOperatorProgress]
    else
      node.asInstanceOf[ArrayNode].asScala.map { n =>
        new StateOperatorProgress(
          operatorName = readNullableString(n, "operatorName"),
          numRowsTotal = readLong(n, "numRowsTotal"),
          numRowsUpdated = readLong(n, "numRowsUpdated"),
          allUpdatesTimeMs = readLong(n, "allUpdatesTimeMs"),
          numRowsRemoved = readLong(n, "numRowsRemoved"),
          allRemovalsTimeMs = readLong(n, "allRemovalsTimeMs"),
          commitTimeMs = readLong(n, "commitTimeMs"),
          memoryUsedBytes = readLong(n, "memoryUsedBytes"),
          numRowsDroppedByWatermark = readLong(n, "numRowsDroppedByWatermark"),
          numShufflePartitions = readLong(n, "numShufflePartitions"),
          numStateStoreInstances = readLong(n, "numStateStoreInstances"),
          customMetrics = readLongMap(n, "customMetrics")
        )
      }.toArray

  private def readSources(node: JsonNode): Array[SourceProgress] =
    if node == null || !node.isArray then Array.empty[SourceProgress]
    else
      node.asInstanceOf[ArrayNode].asScala.map { n =>
        new SourceProgress(
          description = readNullableString(n, "description"),
          startOffset = sourceOffsetToString(n.get("startOffset")),
          endOffset = sourceOffsetToString(n.get("endOffset")),
          latestOffset = sourceOffsetToString(n.get("latestOffset")),
          numInputRows = readLong(n, "numInputRows"),
          inputRowsPerSecond = readDouble(n, "inputRowsPerSecond"),
          processedRowsPerSecond = readDouble(n, "processedRowsPerSecond"),
          metrics = readMetrics(n.get("metrics"))
        )
      }.toArray

  private def readSink(node: JsonNode): SinkProgress =
    if node == null || !node.isObject then null
    else
      new SinkProgress(
        description = readNullableString(node, "description"),
        numOutputRows = readLong(node, "numOutputRows"),
        metrics = readMetrics(node.get("metrics"))
      )

  /** Server returns startOffset/endOffset as raw JSON values (object/string/null). Our typed
    * field is `String`; preserve the JSON literal for non-string nodes so callers can re-parse.
    */
  private def sourceOffsetToString(node: JsonNode): String =
    if node == null || node.isNull then null
    else if node.isTextual then node.asText
    else node.toString

  private[streaming] def putFiniteDouble(node: ObjectNode, field: String, v: Double): Unit =
    if !v.isNaN && !v.isInfinity then node.put(field, v)

  private[streaming] def putLongMap(
      node: ObjectNode,
      field: String,
      map: ju.Map[String, JLong]
  ): Unit =
    if map != null && !map.isEmpty then
      val sub = node.putObject(field)
      map.asScala.toSeq.sortBy(_._1).foreach { case (k, v) => sub.put(k, v.toLong) }

  private[streaming] def putStringMap(
      node: ObjectNode,
      field: String,
      map: ju.Map[String, String]
  ): Unit =
    if map != null && !map.isEmpty then
      val sub = node.putObject(field)
      map.asScala.toSeq.sortBy(_._1).foreach { case (k, v) => sub.put(k, v) }

end StreamingQueryProgress

/** Information about progress made for a source in the execution of a [[StreamingQuery]] during
  * a trigger.
  */
class SourceProgress protected[spark] (
    val description: String,
    val startOffset: String,
    val endOffset: String,
    val latestOffset: String,
    val numInputRows: Long,
    val inputRowsPerSecond: Double,
    val processedRowsPerSecond: Double,
    val metrics: ju.Map[String, String] = new ju.HashMap[String, String]()
) extends Serializable:

  def json: String = StreamingQueryProgress.mapper.writeValueAsString(toJsonNode)

  def prettyJson: String =
    StreamingQueryProgress.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(toJsonNode)

  override def toString: String = prettyJson

  private[sql] def toJsonNode: ObjectNode =
    val node = JsonNodeFactory.instance.objectNode()
    node.put("description", description)
    SourceProgress.putOffset(node, "startOffset", startOffset)
    SourceProgress.putOffset(node, "endOffset", endOffset)
    SourceProgress.putOffset(node, "latestOffset", latestOffset)
    node.put("numInputRows", numInputRows)
    StreamingQueryProgress.putFiniteDouble(node, "inputRowsPerSecond", inputRowsPerSecond)
    StreamingQueryProgress.putFiniteDouble(node, "processedRowsPerSecond", processedRowsPerSecond)
    StreamingQueryProgress.putStringMap(node, "metrics", metrics)
    node

end SourceProgress

private object SourceProgress:
  /** Offsets may be JSON literals or plain strings. Try to re-parse JSON; fall back to a
    * string literal on failure (matches upstream's `tryParse` semantics).
    */
  def putOffset(node: ObjectNode, field: String, value: String): Unit =
    if value == null then node.putNull(field)
    else
      try
        val parsed = StreamingQueryProgress.mapper.readTree(value)
        node.set[ObjectNode](field, parsed)
        ()
      catch case _: Exception => node.put(field, value)

/** Information about progress made for a sink in the execution of a [[StreamingQuery]] during
  * a trigger.
  */
class SinkProgress protected[spark] (
    val description: String,
    val numOutputRows: Long,
    val metrics: ju.Map[String, String] = new ju.HashMap[String, String]()
) extends Serializable:

  protected[sql] def this(description: String) =
    this(description, SinkProgress.DEFAULT_NUM_OUTPUT_ROWS)

  def json: String = StreamingQueryProgress.mapper.writeValueAsString(toJsonNode)

  def prettyJson: String =
    StreamingQueryProgress.mapper.writerWithDefaultPrettyPrinter().writeValueAsString(toJsonNode)

  override def toString: String = prettyJson

  private[sql] def toJsonNode: ObjectNode =
    val node = JsonNodeFactory.instance.objectNode()
    node.put("description", description)
    node.put("numOutputRows", numOutputRows)
    StreamingQueryProgress.putStringMap(node, "metrics", metrics)
    node

end SinkProgress

private[sql] object SinkProgress:
  val DEFAULT_NUM_OUTPUT_ROWS: Long = -1L

  def apply(
      description: String,
      numOutputRows: Option[Long],
      metrics: ju.Map[String, String] = new ju.HashMap[String, String]()
  ): SinkProgress =
    new SinkProgress(description, numOutputRows.getOrElse(DEFAULT_NUM_OUTPUT_ROWS), metrics)
