package org.apache.spark.sql.streaming

import java.util.concurrent.CopyOnWriteArrayList

import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

import org.apache.spark.connect.proto.{Command, ExecutePlanResponse, Plan, StreamingQueryEventType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener.*

/** Client-side event bus for streaming query listeners.
  *
  * Manages a list of [[StreamingQueryListener]] instances. When the first listener is added, a
  * server-side listener bus listener is registered via gRPC, and a background thread begins
  * consuming events from the long-running response stream. When the last listener is removed, the
  * server-side listener is unregistered and the background thread is stopped.
  */
final class StreamingQueryListenerBus private[sql] (session: SparkSession):
  private val listeners = CopyOnWriteArrayList[StreamingQueryListener]()
  private var executionThread: Option[Thread] = None

  private val lock = new Object()

  def close(): Unit =
    lock.synchronized {
      val snapshot = listeners.asScala.toList
      snapshot.foreach(remove)
    }

  def append(listener: StreamingQueryListener): Unit = lock.synchronized {
    listeners.add(listener)

    if listeners.size() == 1 then
      var iter: Option[Iterator[ExecutePlanResponse]] = None
      try iter = Some(registerServerSideListener())
      catch
        case e: Exception =>
          listeners.remove(listener)
          throw e
      val it = iter.get
      val thread = Thread(() => queryEventHandler(it))
      thread.setDaemon(true)
      thread.setName("StreamingQueryListenerBus-handler")
      executionThread = Some(thread)
      thread.start()
  }

  def remove(listener: StreamingQueryListener): Unit = lock.synchronized {
    if listeners.size() == 1 && listeners.contains(listener) then
      val cmdBuilder = Command.newBuilder()
      cmdBuilder.getStreamingQueryListenerBusCommandBuilder
        .setRemoveListenerBusListener(true)
      try
        val plan = Plan.newBuilder().setCommand(cmdBuilder.build()).build()
        val responses = session.client.execute(plan)
        responses.foreach(_ => ()) // drain
      catch case NonFatal(_) => () // best-effort

      executionThread.foreach(_.interrupt())
      executionThread = None
    listeners.remove(listener)
  }

  def list(): Array[StreamingQueryListener] = lock.synchronized {
    listeners.asScala.toArray
  }

  private[sql] def registerServerSideListener(): Iterator[ExecutePlanResponse] =
    val cmdBuilder = Command.newBuilder()
    cmdBuilder.getStreamingQueryListenerBusCommandBuilder
      .setAddListenerBusListener(true)

    val plan = Plan.newBuilder().setCommand(cmdBuilder.build()).build()
    val iterator = session.client.execute(plan)
    while iterator.hasNext do
      val response = iterator.next()
      val result = response.getStreamingQueryListenerEventsResult
      if result.hasListenerBusListenerAdded && result.getListenerBusListenerAdded then
        return iterator
    iterator

  private def queryEventHandler(iter: Iterator[ExecutePlanResponse]): Unit =
    try
      while iter.hasNext do
        val response = iter.next()
        val listenerEvents =
          response.getStreamingQueryListenerEventsResult.getEventsList
        listenerEvents.forEach { event =>
          event.getEventType match
            case StreamingQueryEventType.QUERY_PROGRESS_EVENT =>
              postToAll(QueryProgressEvent.fromJson(event.getEventJson))
            case StreamingQueryEventType.QUERY_IDLE_EVENT =>
              postToAll(QueryIdleEvent.fromJson(event.getEventJson))
            case StreamingQueryEventType.QUERY_TERMINATED_EVENT =>
              postToAll(QueryTerminatedEvent.fromJson(event.getEventJson))
            case _ => () // unknown event type, skip
        }
    catch
      case _: InterruptedException => () // normal shutdown
      case NonFatal(_)             =>
        // Handler thread failed — remove all listeners
        lock.synchronized {
          executionThread = None
          val snapshot = listeners.asScala.toList
          snapshot.foreach(l => listeners.remove(l))
        }

  private[sql] def postToAll(event: Event): Unit = lock.synchronized {
    listeners.forEach { listener =>
      try
        event match
          case e: QueryStartedEvent    => listener.onQueryStarted(e)
          case e: QueryProgressEvent   => listener.onQueryProgress(e)
          case e: QueryIdleEvent       => listener.onQueryIdle(e)
          case e: QueryTerminatedEvent => listener.onQueryTerminated(e)
      catch case NonFatal(_) => () // one listener failure should not affect others
    }
  }
