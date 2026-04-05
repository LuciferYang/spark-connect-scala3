package org.apache.spark.sql.streaming

import java.util.UUID

/** Interface for listening to events related to streaming queries.
  *
  * The methods are not thread-safe as they may be called from different threads.
  */
abstract class StreamingQueryListener extends Serializable:

  /** Called when a query is started. This is called synchronously with `DataStreamWriter.start()`.
    */
  def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit

  /** Called when there is some status update (ingestion rate updated, etc.) */
  def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit

  /** Called when the query is idle and waiting for new data to process. */
  def onQueryIdle(event: StreamingQueryListener.QueryIdleEvent): Unit = ()

  /** Called when a query is stopped, with or without error. */
  def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit

object StreamingQueryListener:

  /** Base type of StreamingQueryListener events. */
  sealed trait Event

  /** Event representing the start of a query.
    *
    * @param id
    *   A unique query id that persists across restarts.
    * @param runId
    *   A query id that is unique for every start/restart.
    * @param name
    *   User-specified name of the query, null if not specified.
    * @param timestamp
    *   The timestamp to start a query.
    */
  case class QueryStartedEvent(
      id: UUID,
      runId: UUID,
      name: String,
      timestamp: String
  ) extends Event:

    def json: String =
      val nameStr = if name == null then "null" else s""""$name""""
      s"""{"id":"$id","runId":"$runId","name":$nameStr,"timestamp":"$timestamp"}"""

  object QueryStartedEvent:
    def fromJson(json: String): QueryStartedEvent =
      val fields = SimpleJsonParser.parse(json)
      QueryStartedEvent(
        id = UUID.fromString(fields("id")),
        runId = UUID.fromString(fields("runId")),
        name = fields.get("name").orNull,
        timestamp = fields("timestamp")
      )

  /** Event representing any progress updates in a query. The progress is stored as raw JSON.
    *
    * @param progressJson
    *   The raw JSON string of the query progress.
    */
  case class QueryProgressEvent(progressJson: String) extends Event:
    def json: String = s"""{"progress":$progressJson}"""

  object QueryProgressEvent:
    def fromJson(json: String): QueryProgressEvent =
      // The event JSON wraps a "progress" key whose value is the full progress JSON.
      // For simplicity, we store the entire event JSON and let callers access the raw string.
      QueryProgressEvent(json)

  /** Event representing that query is idle and waiting for new data to process.
    *
    * @param id
    *   A unique query id that persists across restarts.
    * @param runId
    *   A query id that is unique for every start/restart.
    * @param timestamp
    *   The timestamp when the latest no-batch trigger happened.
    */
  case class QueryIdleEvent(
      id: UUID,
      runId: UUID,
      timestamp: String
  ) extends Event:

    def json: String =
      s"""{"id":"$id","runId":"$runId","timestamp":"$timestamp"}"""

  object QueryIdleEvent:
    def fromJson(json: String): QueryIdleEvent =
      val fields = SimpleJsonParser.parse(json)
      QueryIdleEvent(
        id = UUID.fromString(fields("id")),
        runId = UUID.fromString(fields("runId")),
        timestamp = fields("timestamp")
      )

  /** Event representing that termination of a query.
    *
    * @param id
    *   A unique query id that persists across restarts.
    * @param runId
    *   A query id that is unique for every start/restart.
    * @param exception
    *   The exception message of the query if the query was terminated with an exception.
    * @param errorClassOnException
    *   The error class from the exception if the query was terminated with an exception which is a
    *   part of error class framework.
    */
  case class QueryTerminatedEvent(
      id: UUID,
      runId: UUID,
      exception: Option[String],
      errorClassOnException: Option[String] = None
  ) extends Event:

    def json: String =
      val excStr = exception.map(e => s""""$e"""").getOrElse("null")
      val errStr = errorClassOnException.map(e => s""""$e"""").getOrElse("null")
      s"""{"id":"$id","runId":"$runId","exception":$excStr,"errorClassOnException":$errStr}"""

  object QueryTerminatedEvent:
    def fromJson(json: String): QueryTerminatedEvent =
      val fields = SimpleJsonParser.parse(json)
      QueryTerminatedEvent(
        id = UUID.fromString(fields("id")),
        runId = UUID.fromString(fields("runId")),
        exception = fields.get("exception"),
        errorClassOnException = fields.get("errorClassOnException")
      )

  /** Minimal JSON parser for flat objects with string/null values. Avoids Jackson dependency. */
  private[streaming] object SimpleJsonParser:
    def parse(json: String): Map[String, String] =
      val trimmed = json.trim
      if !trimmed.startsWith("{") || !trimmed.endsWith("}") then
        throw new IllegalArgumentException(s"Expected JSON object: $json")
      val inner = trimmed.substring(1, trimmed.length - 1).trim
      if inner.isEmpty then return Map.empty

      val result = scala.collection.mutable.Map.empty[String, String]
      var i = 0

      def skipWhitespace(): Unit =
        while i < inner.length && inner(i).isWhitespace do i += 1

      def readString(): String =
        if inner(i) != '"' then
          throw new IllegalArgumentException(s"Expected '\"' at pos $i in: $json")
        i += 1
        val sb = new StringBuilder
        while i < inner.length && inner(i) != '"' do
          if inner(i) == '\\' then
            i += 1
            if i < inner.length then
              inner(i) match
                case '"'  => sb.append('"')
                case '\\' => sb.append('\\')
                case '/'  => sb.append('/')
                case 'n'  => sb.append('\n')
                case 't'  => sb.append('\t')
                case 'r'  => sb.append('\r')
                case c    => sb.append('\\'); sb.append(c)
          else sb.append(inner(i))
          i += 1
        i += 1 // skip closing quote
        sb.toString

      def readValue(): Option[String] =
        if inner(i) == '"' then Some(readString())
        else if inner.substring(i).startsWith("null") then
          i += 4
          None
        else if inner(i) == '{' then
          // Nested object — collect the entire object as a raw string
          val start = i
          var depth = 0
          while i < inner.length do
            if inner(i) == '{' then depth += 1
            else if inner(i) == '}' then
              depth -= 1
              if depth == 0 then
                i += 1
                return Some(inner.substring(start, i))
            else if inner(i) == '"' then
              i += 1
              while i < inner.length && inner(i) != '"' do
                if inner(i) == '\\' then i += 1
                i += 1
              // i now points at closing quote
            i += 1
          Some(inner.substring(start, i))
        else
          // Number, boolean, or array — read until comma or end
          val start = i
          var depth = 0
          while i < inner.length && !(inner(i) == ',' && depth == 0) do
            if inner(i) == '[' || inner(i) == '{' then depth += 1
            else if inner(i) == ']' || inner(i) == '}' then depth -= 1
            i += 1
          Some(inner.substring(start, i).trim)

      while i < inner.length do
        skipWhitespace()
        if i < inner.length && inner(i) == '"' then
          val key = readString()
          skipWhitespace()
          if i < inner.length && inner(i) == ':' then i += 1
          skipWhitespace()
          val value = readValue()
          value match
            case Some(v) => result(key) = v
            case None    => // null — skip
          skipWhitespace()
          if i < inner.length && inner(i) == ',' then i += 1
        else i += 1

      result.toMap
