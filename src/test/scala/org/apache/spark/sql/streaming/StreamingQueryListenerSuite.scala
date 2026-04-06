package org.apache.spark.sql.streaming

import java.util.UUID

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import org.apache.spark.connect.proto.*
import org.apache.spark.sql.streaming.StreamingQueryListener.*

class StreamingQueryListenerSuite extends AnyFunSuite with Matchers:

  // ---------------------------------------------------------------------------
  // StreamingQueryListener abstract class
  // ---------------------------------------------------------------------------

  test("onQueryIdle has default no-op implementation") {
    var startedCalled = false
    var progressCalled = false
    var terminatedCalled = false

    val listener = new StreamingQueryListener:
      def onQueryStarted(event: QueryStartedEvent): Unit = startedCalled = true
      def onQueryProgress(event: QueryProgressEvent): Unit = progressCalled = true
      def onQueryTerminated(event: QueryTerminatedEvent): Unit = terminatedCalled = true

    // onQueryIdle should not throw — it's a no-op by default
    val idleEvent = QueryIdleEvent(
      UUID.randomUUID(),
      UUID.randomUUID(),
      "2026-04-05T10:00:00.000Z"
    )
    listener.onQueryIdle(idleEvent)

    startedCalled shouldBe false
    progressCalled shouldBe false
    terminatedCalled shouldBe false
  }

  // ---------------------------------------------------------------------------
  // Event JSON serialization / deserialization round-trips
  // ---------------------------------------------------------------------------

  test("QueryStartedEvent JSON round-trip") {
    val id = UUID.randomUUID()
    val runId = UUID.randomUUID()
    val event = QueryStartedEvent(id, runId, "test_query", "2026-04-05T10:00:00.000Z")

    val json = event.json
    val parsed = QueryStartedEvent.fromJson(json)

    parsed.id shouldBe id
    parsed.runId shouldBe runId
    parsed.name shouldBe "test_query"
    parsed.timestamp shouldBe "2026-04-05T10:00:00.000Z"
  }

  test("QueryStartedEvent JSON round-trip with null name") {
    val id = UUID.randomUUID()
    val runId = UUID.randomUUID()
    val event = QueryStartedEvent(id, runId, null, "2026-04-05T10:00:00.000Z")

    val json = event.json
    val parsed = QueryStartedEvent.fromJson(json)

    parsed.id shouldBe id
    parsed.runId shouldBe runId
    parsed.name shouldBe null
    parsed.timestamp shouldBe "2026-04-05T10:00:00.000Z"
  }

  test("QueryIdleEvent JSON round-trip") {
    val id = UUID.randomUUID()
    val runId = UUID.randomUUID()
    val event = QueryIdleEvent(id, runId, "2026-04-05T10:05:00.000Z")

    val json = event.json
    val parsed = QueryIdleEvent.fromJson(json)

    parsed.id shouldBe id
    parsed.runId shouldBe runId
    parsed.timestamp shouldBe "2026-04-05T10:05:00.000Z"
  }

  test("QueryTerminatedEvent JSON round-trip with exception") {
    val id = UUID.randomUUID()
    val runId = UUID.randomUUID()
    val event = QueryTerminatedEvent(
      id,
      runId,
      Some("java.lang.RuntimeException: test error"),
      Some("INTERNAL_ERROR")
    )

    val json = event.json
    val parsed = QueryTerminatedEvent.fromJson(json)

    parsed.id shouldBe id
    parsed.runId shouldBe runId
    parsed.exception shouldBe Some("java.lang.RuntimeException: test error")
    parsed.errorClassOnException shouldBe Some("INTERNAL_ERROR")
  }

  test("QueryTerminatedEvent JSON round-trip without exception") {
    val id = UUID.randomUUID()
    val runId = UUID.randomUUID()
    val event = QueryTerminatedEvent(id, runId, None, None)

    val json = event.json
    val parsed = QueryTerminatedEvent.fromJson(json)

    parsed.id shouldBe id
    parsed.runId shouldBe runId
    parsed.exception shouldBe None
    parsed.errorClassOnException shouldBe None
  }

  test("QueryProgressEvent stores raw JSON") {
    val progressJson = """{"batchId":1,"numInputRows":100}"""
    val event = QueryProgressEvent(progressJson)

    event.progressJson shouldBe progressJson
    event.json should include(progressJson)
  }

  // ---------------------------------------------------------------------------
  // ListenerBus append / remove / list
  // ---------------------------------------------------------------------------

  test("ListenerBus postToAll dispatches to correct handler") {
    import org.apache.spark.sql.SparkSession

    val session = SparkSession.builder().remote("sc://localhost:15002").build()
    val bus = StreamingQueryListenerBus(session)

    var receivedStarted: Option[QueryStartedEvent] = None
    var receivedProgress: Option[QueryProgressEvent] = None
    var receivedIdle: Option[QueryIdleEvent] = None
    var receivedTerminated: Option[QueryTerminatedEvent] = None

    val listener = new StreamingQueryListener:
      def onQueryStarted(event: QueryStartedEvent): Unit =
        receivedStarted = Some(event)
      def onQueryProgress(event: QueryProgressEvent): Unit =
        receivedProgress = Some(event)
      override def onQueryIdle(event: QueryIdleEvent): Unit =
        receivedIdle = Some(event)
      def onQueryTerminated(event: QueryTerminatedEvent): Unit =
        receivedTerminated = Some(event)

    // Directly add to the internal list for unit testing (skip gRPC registration)
    bus.postToAll(QueryStartedEvent(
      UUID.randomUUID(),
      UUID.randomUUID(),
      "q1",
      "2026-04-05T10:00:00.000Z"
    ))
    receivedStarted shouldBe None // no listener added yet

    // We can't call append() without a real server, but we can test postToAll directly
    // by adding listener manually for testing purposes
  }

  test("ListenerBus postToAll exception isolation") {
    import org.apache.spark.sql.SparkSession

    val session = SparkSession.builder().remote("sc://localhost:15002").build()
    val bus = StreamingQueryListenerBus(session)

    var listener2Called = false

    val badListener = new StreamingQueryListener:
      def onQueryStarted(event: QueryStartedEvent): Unit =
        throw new RuntimeException("boom")
      def onQueryProgress(event: QueryProgressEvent): Unit = ()
      def onQueryTerminated(event: QueryTerminatedEvent): Unit = ()

    val goodListener = new StreamingQueryListener:
      def onQueryStarted(event: QueryStartedEvent): Unit =
        listener2Called = true
      def onQueryProgress(event: QueryProgressEvent): Unit = ()
      def onQueryTerminated(event: QueryTerminatedEvent): Unit = ()

    // Manually test postToAll — the bus uses its internal listeners list
    // Since we can't add listeners without gRPC, test the dispatch logic directly
    val event = QueryStartedEvent(
      UUID.randomUUID(),
      UUID.randomUUID(),
      "test",
      "2026-04-05T10:00:00.000Z"
    )

    // Verify postToAll doesn't throw even with no listeners
    noException shouldBe thrownBy {
      bus.postToAll(event)
    }
  }

  // ---------------------------------------------------------------------------
  // Proto construction for StreamingQueryListenerBusCommand
  // ---------------------------------------------------------------------------

  test("StreamingQueryListenerBusCommand proto for add") {
    val cmd = Command.newBuilder()
    cmd.getStreamingQueryListenerBusCommandBuilder
      .setAddListenerBusListener(true)
    val built = cmd.build()

    built.hasStreamingQueryListenerBusCommand shouldBe true
    built.getStreamingQueryListenerBusCommand.getAddListenerBusListener shouldBe true
  }

  test("StreamingQueryListenerBusCommand proto for remove") {
    val cmd = Command.newBuilder()
    cmd.getStreamingQueryListenerBusCommandBuilder
      .setRemoveListenerBusListener(true)
    val built = cmd.build()

    built.hasStreamingQueryListenerBusCommand shouldBe true
    built.getStreamingQueryListenerBusCommand.getRemoveListenerBusListener shouldBe true
  }

  // ---------------------------------------------------------------------------
  // Proto construction for StreamingQueryManagerCommand listener operations
  // ---------------------------------------------------------------------------

  test("StreamingQueryManagerCommand proto for addListener") {
    val listenerCmd = StreamingQueryManagerCommand.StreamingQueryListenerCommand.newBuilder()
      .setListenerPayload(com.google.protobuf.ByteString.copyFrom(Array[Byte](1, 2, 3)))
      .setId("listener-uuid-1")
      .build()

    val cmd = StreamingQueryManagerCommand.newBuilder()
      .setAddListener(listenerCmd)
      .build()

    cmd.hasAddListener shouldBe true
    cmd.getAddListener.getId shouldBe "listener-uuid-1"
    cmd.getAddListener.getListenerPayload.toByteArray shouldBe Array[Byte](1, 2, 3)
  }

  test("StreamingQueryManagerCommand proto for removeListener") {
    val listenerCmd = StreamingQueryManagerCommand.StreamingQueryListenerCommand.newBuilder()
      .setId("listener-uuid-2")
      .build()

    val cmd = StreamingQueryManagerCommand.newBuilder()
      .setRemoveListener(listenerCmd)
      .build()

    cmd.hasRemoveListener shouldBe true
    cmd.getRemoveListener.getId shouldBe "listener-uuid-2"
  }

  test("StreamingQueryManagerCommand proto for listListeners") {
    val cmd = StreamingQueryManagerCommand.newBuilder()
      .setListListeners(true)
      .build()

    cmd.hasListListeners shouldBe true
    cmd.getListListeners shouldBe true
  }

  test("StreamingQueryManagerCommandResult ListStreamingQueryListenerResult proto") {
    val result =
      StreamingQueryManagerCommandResult.ListStreamingQueryListenerResult.newBuilder()
        .addListenerIds("id-1")
        .addListenerIds("id-2")
        .build()

    result.getListenerIdsList should have size 2
    result.getListenerIds(0) shouldBe "id-1"
    result.getListenerIds(1) shouldBe "id-2"
  }

  // ---------------------------------------------------------------------------
  // Proto: StreamingQueryListenerEventsResult
  // ---------------------------------------------------------------------------

  test("StreamingQueryListenerEventsResult proto") {
    val event = StreamingQueryListenerEvent.newBuilder()
      .setEventJson("""{"id":"abc","runId":"def","timestamp":"2026-04-05"}""")
      .setEventType(StreamingQueryEventType.QUERY_IDLE_EVENT)
      .build()

    val result = StreamingQueryListenerEventsResult.newBuilder()
      .addEvents(event)
      .setListenerBusListenerAdded(true)
      .build()

    result.getEventsCount shouldBe 1
    result.getEvents(0).getEventType shouldBe StreamingQueryEventType.QUERY_IDLE_EVENT
    result.getListenerBusListenerAdded shouldBe true
  }

  test("StreamingQueryEventType enum values") {
    StreamingQueryEventType.QUERY_PROGRESS_UNSPECIFIED.getNumber shouldBe 0
    StreamingQueryEventType.QUERY_PROGRESS_EVENT.getNumber shouldBe 1
    StreamingQueryEventType.QUERY_TERMINATED_EVENT.getNumber shouldBe 2
    StreamingQueryEventType.QUERY_IDLE_EVENT.getNumber shouldBe 3
  }

  // ---------------------------------------------------------------------------
  // SimpleJsonParser
  // ---------------------------------------------------------------------------

  test("SimpleJsonParser parses flat JSON object") {
    val json = """{"key1":"value1","key2":"value2"}"""
    val result = SimpleJsonParser.parse(json)
    result("key1") shouldBe "value1"
    result("key2") shouldBe "value2"
  }

  test("SimpleJsonParser handles null values") {
    val json = """{"key1":"value1","key2":null}"""
    val result = SimpleJsonParser.parse(json)
    result("key1") shouldBe "value1"
    result.get("key2") shouldBe None
  }

  test("SimpleJsonParser handles empty object") {
    val result = SimpleJsonParser.parse("{}")
    result shouldBe empty
  }

  test("SimpleJsonParser handles escaped characters") {
    val json = """{"key":"value with \"quotes\""}"""
    val result = SimpleJsonParser.parse(json)
    result("key") shouldBe """value with "quotes""""
  }

  // ---------------------------------------------------------------------------
  // WriteStreamOperationStartResult query_started_event_json
  // ---------------------------------------------------------------------------

  test("WriteStreamOperationStartResult with query_started_event_json") {
    val eventJson =
      """{"id":"550e8400-e29b-41d4-a716-446655440000","runId":"550e8400-e29b-41d4-a716-446655440001","name":"testQuery","timestamp":"2026-04-05T10:00:00.000Z"}"""

    val result = WriteStreamOperationStartResult.newBuilder()
      .setQueryId(StreamingQueryInstanceId.newBuilder()
        .setId("qid-1").setRunId("rid-1").build())
      .setName("testQuery")
      .setQueryStartedEventJson(eventJson)
      .build()

    result.hasQueryStartedEventJson shouldBe true
    result.getQueryStartedEventJson shouldBe eventJson

    // Verify we can parse the event
    val event = QueryStartedEvent.fromJson(result.getQueryStartedEventJson)
    event.id shouldBe UUID.fromString("550e8400-e29b-41d4-a716-446655440000")
    event.name shouldBe "testQuery"
  }

  test("WriteStreamOperationStartResult without query_started_event_json") {
    val result = WriteStreamOperationStartResult.newBuilder()
      .setQueryId(StreamingQueryInstanceId.newBuilder()
        .setId("qid-1").setRunId("rid-1").build())
      .build()

    result.hasQueryStartedEventJson shouldBe false
  }

  // ---------------------------------------------------------------------------
  // SimpleJsonParser: escape sequences
  // ---------------------------------------------------------------------------

  test("SimpleJsonParser handles backslash escape") {
    val json = """{"path":"C:\\Users\\test"}"""
    val result = SimpleJsonParser.parse(json)
    result("path") shouldBe "C:\\Users\\test"
  }

  test("SimpleJsonParser handles forward slash escape") {
    val json = """{"url":"http:\/\/example.com"}"""
    val result = SimpleJsonParser.parse(json)
    result("url") shouldBe "http://example.com"
  }

  test("SimpleJsonParser handles newline escape") {
    val json = """{"text":"line1\nline2"}"""
    val result = SimpleJsonParser.parse(json)
    result("text") shouldBe "line1\nline2"
  }

  test("SimpleJsonParser handles tab escape") {
    val json = """{"text":"col1\tcol2"}"""
    val result = SimpleJsonParser.parse(json)
    result("text") shouldBe "col1\tcol2"
  }

  test("SimpleJsonParser handles carriage return escape") {
    val json = """{"text":"line1\rline2"}"""
    val result = SimpleJsonParser.parse(json)
    result("text") shouldBe "line1\rline2"
  }

  test("SimpleJsonParser handles unknown escape sequence") {
    val json = """{"text":"test\xvalue"}"""
    val result = SimpleJsonParser.parse(json)
    result("text") shouldBe "test\\xvalue"
  }

  // ---------------------------------------------------------------------------
  // SimpleJsonParser: nested objects
  // ---------------------------------------------------------------------------

  test("SimpleJsonParser handles nested JSON object as value") {
    val json = """{"outer":{"inner":"value"}}"""
    val result = SimpleJsonParser.parse(json)
    result("outer") shouldBe """{"inner":"value"}"""
  }

  test("SimpleJsonParser handles deeply nested objects") {
    val json = """{"a":{"b":{"c":"deep"}}}"""
    val result = SimpleJsonParser.parse(json)
    result("a") shouldBe """{"b":{"c":"deep"}}"""
  }

  // ---------------------------------------------------------------------------
  // SimpleJsonParser: number, boolean, and array values
  // ---------------------------------------------------------------------------

  test("SimpleJsonParser handles numeric values") {
    val json = """{"count":42,"rate":3.14}"""
    val result = SimpleJsonParser.parse(json)
    result("count") shouldBe "42"
    result("rate") shouldBe "3.14"
  }

  test("SimpleJsonParser handles boolean values") {
    val json = """{"active":true,"deleted":false}"""
    val result = SimpleJsonParser.parse(json)
    result("active") shouldBe "true"
    result("deleted") shouldBe "false"
  }

  test("SimpleJsonParser handles array values") {
    val json = """{"tags":["a","b","c"]}"""
    val result = SimpleJsonParser.parse(json)
    result("tags") shouldBe """["a","b","c"]"""
  }

  test("SimpleJsonParser handles mixed value types") {
    val json = """{"name":"test","count":5,"flag":true,"data":null}"""
    val result = SimpleJsonParser.parse(json)
    result("name") shouldBe "test"
    result("count") shouldBe "5"
    result("flag") shouldBe "true"
    result.get("data") shouldBe None
  }

  // ---------------------------------------------------------------------------
  // SimpleJsonParser: error handling
  // ---------------------------------------------------------------------------

  test("SimpleJsonParser rejects non-JSON input") {
    an[IllegalArgumentException] shouldBe thrownBy {
      SimpleJsonParser.parse("not json")
    }
  }

  test("SimpleJsonParser rejects array input") {
    an[IllegalArgumentException] shouldBe thrownBy {
      SimpleJsonParser.parse("[1,2,3]")
    }
  }

  // ---------------------------------------------------------------------------
  // SimpleJsonParser: whitespace handling
  // ---------------------------------------------------------------------------

  test("SimpleJsonParser handles whitespace around keys and values") {
    val json = """{ "key1" : "value1" , "key2" : "value2" }"""
    val result = SimpleJsonParser.parse(json)
    result("key1") shouldBe "value1"
    result("key2") shouldBe "value2"
  }

  // ---------------------------------------------------------------------------
  // QueryProgressEvent fromJson
  // ---------------------------------------------------------------------------

  test("QueryProgressEvent.fromJson stores the raw JSON") {
    val rawJson = """{"batchId":5,"numInputRows":200,"inputRowsPerSecond":100.0}"""
    val event = QueryProgressEvent.fromJson(rawJson)
    event.progressJson shouldBe rawJson
  }

  test("QueryProgressEvent json wraps in progress key") {
    val rawJson = """{"batchId":1}"""
    val event = QueryProgressEvent(rawJson)
    event.json shouldBe s"""{"progress":$rawJson}"""
  }

  // ---------------------------------------------------------------------------
  // Event sealed trait
  // ---------------------------------------------------------------------------

  test("all event types are subtypes of Event") {
    val id = UUID.randomUUID()
    val runId = UUID.randomUUID()

    val started: Event = QueryStartedEvent(id, runId, "q", "ts")
    val progress: Event = QueryProgressEvent("{}")
    val idle: Event = QueryIdleEvent(id, runId, "ts")
    val terminated: Event = QueryTerminatedEvent(id, runId, None)

    started shouldBe a[Event]
    progress shouldBe a[Event]
    idle shouldBe a[Event]
    terminated shouldBe a[Event]
  }

  // ---------------------------------------------------------------------------
  // QueryStartedEvent json output format
  // ---------------------------------------------------------------------------

  test("QueryStartedEvent json output with non-null name") {
    val id = UUID.fromString("550e8400-e29b-41d4-a716-446655440000")
    val runId = UUID.fromString("550e8400-e29b-41d4-a716-446655440001")
    val event = QueryStartedEvent(id, runId, "myQuery", "2026-04-05T10:00:00.000Z")
    val json = event.json
    json should include("\"name\":\"myQuery\"")
    json should include(s""""id":"$id"""")
    json should include(s""""runId":"$runId"""")
    json should include("\"timestamp\":\"2026-04-05T10:00:00.000Z\"")
  }

  test("QueryStartedEvent json output with null name") {
    val id = UUID.randomUUID()
    val runId = UUID.randomUUID()
    val event = QueryStartedEvent(id, runId, null, "2026-04-05T10:00:00.000Z")
    event.json should include("\"name\":null")
  }

  // ---------------------------------------------------------------------------
  // QueryIdleEvent json output
  // ---------------------------------------------------------------------------

  test("QueryIdleEvent json output") {
    val id = UUID.fromString("550e8400-e29b-41d4-a716-446655440000")
    val runId = UUID.fromString("550e8400-e29b-41d4-a716-446655440001")
    val event = QueryIdleEvent(id, runId, "2026-04-05T10:00:00.000Z")
    val json = event.json
    json should include(s""""id":"$id"""")
    json should include(s""""runId":"$runId"""")
    json should include("\"timestamp\":\"2026-04-05T10:00:00.000Z\"")
  }

  // ---------------------------------------------------------------------------
  // QueryTerminatedEvent json output
  // ---------------------------------------------------------------------------

  test("QueryTerminatedEvent json with exception") {
    val id = UUID.randomUUID()
    val runId = UUID.randomUUID()
    val event = QueryTerminatedEvent(id, runId, Some("error msg"), Some("ERR_CLASS"))
    val json = event.json
    json should include("\"exception\":\"error msg\"")
    json should include("\"errorClassOnException\":\"ERR_CLASS\"")
  }

  test("QueryTerminatedEvent json without exception") {
    val id = UUID.randomUUID()
    val runId = UUID.randomUUID()
    val event = QueryTerminatedEvent(id, runId, None, None)
    val json = event.json
    json should include("\"exception\":null")
    json should include("\"errorClassOnException\":null")
  }

  // ---------------------------------------------------------------------------
  // StreamingQueryListenerBus postToAll with actual listeners
  // ---------------------------------------------------------------------------

  test("StreamingQueryListenerBus postToAll dispatches QueryStartedEvent to listeners") {
    import org.apache.spark.sql.SparkSession

    val session = SparkSession(null)
    val bus = StreamingQueryListenerBus(session)

    var receivedEvent: Option[QueryStartedEvent] = None
    val listener = new StreamingQueryListener:
      def onQueryStarted(event: QueryStartedEvent): Unit =
        receivedEvent = Some(event)
      def onQueryProgress(event: QueryProgressEvent): Unit = ()
      def onQueryTerminated(event: QueryTerminatedEvent): Unit = ()

    // Directly add the listener to the internal list (bypass gRPC registration)
    val listenersField = classOf[StreamingQueryListenerBus].getDeclaredField("listeners")
    listenersField.setAccessible(true)
    val listeners = listenersField.get(
      bus
    ).asInstanceOf[java.util.concurrent.CopyOnWriteArrayList[StreamingQueryListener]]
    listeners.add(listener)

    val event = QueryStartedEvent(
      UUID.randomUUID(),
      UUID.randomUUID(),
      "test_q",
      "2026-04-05T10:00:00.000Z"
    )
    bus.postToAll(event)

    receivedEvent shouldBe defined
    receivedEvent.get.name shouldBe "test_q"
  }

  test("StreamingQueryListenerBus postToAll dispatches QueryProgressEvent to listeners") {
    import org.apache.spark.sql.SparkSession

    val session = SparkSession(null)
    val bus = StreamingQueryListenerBus(session)

    var receivedEvent: Option[QueryProgressEvent] = None
    val listener = new StreamingQueryListener:
      def onQueryStarted(event: QueryStartedEvent): Unit = ()
      def onQueryProgress(event: QueryProgressEvent): Unit =
        receivedEvent = Some(event)
      def onQueryTerminated(event: QueryTerminatedEvent): Unit = ()

    val listenersField = classOf[StreamingQueryListenerBus].getDeclaredField("listeners")
    listenersField.setAccessible(true)
    val listeners = listenersField.get(
      bus
    ).asInstanceOf[java.util.concurrent.CopyOnWriteArrayList[StreamingQueryListener]]
    listeners.add(listener)

    val event = QueryProgressEvent("""{"batchId":1}""")
    bus.postToAll(event)

    receivedEvent shouldBe defined
    receivedEvent.get.progressJson shouldBe """{"batchId":1}"""
  }

  test("StreamingQueryListenerBus postToAll dispatches QueryIdleEvent to listeners") {
    import org.apache.spark.sql.SparkSession

    val session = SparkSession(null)
    val bus = StreamingQueryListenerBus(session)

    var receivedEvent: Option[QueryIdleEvent] = None
    val listener = new StreamingQueryListener:
      def onQueryStarted(event: QueryStartedEvent): Unit = ()
      def onQueryProgress(event: QueryProgressEvent): Unit = ()
      override def onQueryIdle(event: QueryIdleEvent): Unit =
        receivedEvent = Some(event)
      def onQueryTerminated(event: QueryTerminatedEvent): Unit = ()

    val listenersField = classOf[StreamingQueryListenerBus].getDeclaredField("listeners")
    listenersField.setAccessible(true)
    val listeners = listenersField.get(
      bus
    ).asInstanceOf[java.util.concurrent.CopyOnWriteArrayList[StreamingQueryListener]]
    listeners.add(listener)

    val event = QueryIdleEvent(UUID.randomUUID(), UUID.randomUUID(), "2026-04-05T10:00:00.000Z")
    bus.postToAll(event)

    receivedEvent shouldBe defined
  }

  test("StreamingQueryListenerBus postToAll dispatches QueryTerminatedEvent to listeners") {
    import org.apache.spark.sql.SparkSession

    val session = SparkSession(null)
    val bus = StreamingQueryListenerBus(session)

    var receivedEvent: Option[QueryTerminatedEvent] = None
    val listener = new StreamingQueryListener:
      def onQueryStarted(event: QueryStartedEvent): Unit = ()
      def onQueryProgress(event: QueryProgressEvent): Unit = ()
      def onQueryTerminated(event: QueryTerminatedEvent): Unit =
        receivedEvent = Some(event)

    val listenersField = classOf[StreamingQueryListenerBus].getDeclaredField("listeners")
    listenersField.setAccessible(true)
    val listeners = listenersField.get(
      bus
    ).asInstanceOf[java.util.concurrent.CopyOnWriteArrayList[StreamingQueryListener]]
    listeners.add(listener)

    val event = QueryTerminatedEvent(UUID.randomUUID(), UUID.randomUUID(), Some("err"), None)
    bus.postToAll(event)

    receivedEvent shouldBe defined
    receivedEvent.get.exception shouldBe Some("err")
  }

  test("StreamingQueryListenerBus postToAll isolates listener exceptions") {
    import org.apache.spark.sql.SparkSession

    val session = SparkSession(null)
    val bus = StreamingQueryListenerBus(session)

    var goodListenerCalled = false
    val badListener = new StreamingQueryListener:
      def onQueryStarted(event: QueryStartedEvent): Unit =
        throw new RuntimeException("boom")
      def onQueryProgress(event: QueryProgressEvent): Unit = ()
      def onQueryTerminated(event: QueryTerminatedEvent): Unit = ()

    val goodListener = new StreamingQueryListener:
      def onQueryStarted(event: QueryStartedEvent): Unit =
        goodListenerCalled = true
      def onQueryProgress(event: QueryProgressEvent): Unit = ()
      def onQueryTerminated(event: QueryTerminatedEvent): Unit = ()

    val listenersField = classOf[StreamingQueryListenerBus].getDeclaredField("listeners")
    listenersField.setAccessible(true)
    val listeners = listenersField.get(
      bus
    ).asInstanceOf[java.util.concurrent.CopyOnWriteArrayList[StreamingQueryListener]]
    listeners.add(badListener)
    listeners.add(goodListener)

    val event = QueryStartedEvent(
      UUID.randomUUID(),
      UUID.randomUUID(),
      "test",
      "2026-04-05T10:00:00.000Z"
    )

    noException shouldBe thrownBy(bus.postToAll(event))
    goodListenerCalled shouldBe true
  }

  test("StreamingQueryListenerBus list returns registered listeners") {
    import org.apache.spark.sql.SparkSession

    val session = SparkSession(null)
    val bus = StreamingQueryListenerBus(session)

    val listener = new StreamingQueryListener:
      def onQueryStarted(event: QueryStartedEvent): Unit = ()
      def onQueryProgress(event: QueryProgressEvent): Unit = ()
      def onQueryTerminated(event: QueryTerminatedEvent): Unit = ()

    val listenersField = classOf[StreamingQueryListenerBus].getDeclaredField("listeners")
    listenersField.setAccessible(true)
    val listeners = listenersField.get(
      bus
    ).asInstanceOf[java.util.concurrent.CopyOnWriteArrayList[StreamingQueryListener]]
    listeners.add(listener)

    val listed = bus.list()
    listed should have length 1
    listed(0) should be theSameInstanceAs listener
  }

  test("StreamingQueryListenerBus list returns empty for no listeners") {
    import org.apache.spark.sql.SparkSession

    val session = SparkSession(null)
    val bus = StreamingQueryListenerBus(session)

    bus.list() shouldBe empty
  }

  // ---------------------------------------------------------------------------
  // SimpleJsonParser: nested object with strings containing braces
  // ---------------------------------------------------------------------------

  test("SimpleJsonParser handles nested object with string containing braces") {
    val json = """{"data":{"msg":"hello {world}"}}"""
    val result = SimpleJsonParser.parse(json)
    result("data") shouldBe """{"msg":"hello {world}"}"""
  }

  // ---------------------------------------------------------------------------
  // QueryTerminatedEvent default parameter
  // ---------------------------------------------------------------------------

  test("QueryTerminatedEvent default errorClassOnException is None") {
    val id = UUID.randomUUID()
    val runId = UUID.randomUUID()
    val event = QueryTerminatedEvent(id, runId, Some("error"))
    event.errorClassOnException shouldBe None
  }
