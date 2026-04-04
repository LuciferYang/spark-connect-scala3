package org.apache.spark.sql

import org.apache.spark.connect.proto.*

import scala.jdk.CollectionConverters.*

/** Manager for active streaming queries on a SparkSession.
  *
  * Access via `spark.streams`.
  */
final class StreamingQueryManager private[sql] (private val session: SparkSession):

  /** Returns a list of all active streaming queries. */
  def active: Seq[StreamingQuery] =
    val result = executeManagerCmd(_.setActive(true))
    result.getActive.getActiveQueriesList.asScala.map { qi =>
      val name = if qi.hasName then Some(qi.getName) else None
      StreamingQuery(session, qi.getId.getId, qi.getId.getRunId, name)
    }.toSeq

  /** Returns the streaming query identified by the given id. */
  def get(id: String): StreamingQuery =
    val result = executeManagerCmd(_.setGetQuery(id))
    val qi = result.getQuery
    val name = if qi.hasName then Some(qi.getName) else None
    StreamingQuery(session, qi.getId.getId, qi.getId.getRunId, name)

  /** Block until any streaming query terminates. */
  def awaitAnyTermination(): Unit =
    executeManagerCmd(_.setAwaitAnyTermination(
      StreamingQueryManagerCommand.AwaitAnyTerminationCommand.getDefaultInstance
    ))
    ()

  /** Block until any streaming query terminates or the timeout expires. Returns true if terminated. */
  def awaitAnyTermination(timeoutMs: Long): Boolean =
    val result = executeManagerCmd(_.setAwaitAnyTermination(
      StreamingQueryManagerCommand.AwaitAnyTerminationCommand.newBuilder()
        .setTimeoutMs(timeoutMs).build()
    ))
    result.getAwaitAnyTermination.getTerminated

  /** Forget about past terminated queries so that `awaitAnyTermination()` can be used again. */
  def resetTerminated(): Unit =
    executeManagerCmd(_.setResetTerminated(true))
    ()

  private def executeManagerCmd(
      setCmdFn: StreamingQueryManagerCommand.Builder => StreamingQueryManagerCommand.Builder
  ): StreamingQueryManagerCommandResult =
    val cmdBuilder = StreamingQueryManagerCommand.newBuilder()
    setCmdFn(cmdBuilder)
    val command = Command.newBuilder()
      .setStreamingQueryManagerCommand(cmdBuilder.build())
      .build()
    val plan = Plan.newBuilder().setCommand(command).build()
    val responses = session.client.execute(plan)
    var result: StreamingQueryManagerCommandResult = null
    responses.foreach { resp =>
      if resp.hasStreamingQueryManagerCommandResult then
        result = resp.getStreamingQueryManagerCommandResult
    }
    if result == null then
      throw new RuntimeException("No StreamingQueryManagerCommandResult in response")
    result
