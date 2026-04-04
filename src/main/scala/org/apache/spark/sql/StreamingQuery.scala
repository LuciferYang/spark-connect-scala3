package org.apache.spark.sql

import org.apache.spark.connect.proto.*

/** Handle to a streaming query, used to manage its lifecycle.
  *
  * Obtained from `DataStreamWriter.start()` or `StreamingQueryManager.get()`.
  */
final class StreamingQuery private[sql] (
    private[sql] val session: SparkSession,
    val id: String,
    val runId: String,
    val name: Option[String]
):

  /** Whether the query is currently active. */
  def isActive: Boolean =
    val result = executeQueryCmd(_.setStatus(true))
    result.getStatus.getIsActive

  /** Block until the query terminates. */
  def awaitTermination(): Unit =
    executeQueryCmd(_.setAwaitTermination(
      StreamingQueryCommand.AwaitTerminationCommand.getDefaultInstance
    ))
    ()

  /** Block until the query terminates or the timeout expires. Returns true if terminated. */
  def awaitTermination(timeoutMs: Long): Boolean =
    val result = executeQueryCmd(_.setAwaitTermination(
      StreamingQueryCommand.AwaitTerminationCommand.newBuilder()
        .setTimeoutMs(timeoutMs).build()
    ))
    result.getAwaitTermination.getTerminated

  /** Stop the query. */
  def stop(): Unit =
    executeQueryCmd(_.setStop(true))
    ()

  /** Block until all available data has been processed. */
  def processAllAvailable(): Unit =
    executeQueryCmd(_.setProcessAllAvailable(true))
    ()

  /** Returns the most recent progress updates as JSON strings. */
  def recentProgress: Seq[String] =
    val result = executeQueryCmd(_.setRecentProgress(true))
    import scala.jdk.CollectionConverters.*
    result.getRecentProgress.getRecentProgressJsonList.asScala.toSeq

  /** Returns the most recent progress update as a JSON string, if any. */
  def lastProgress: Option[String] =
    val result = executeQueryCmd(_.setLastProgress(true))
    val progresses = result.getRecentProgress.getRecentProgressJsonList
    if progresses.isEmpty then None
    else Some(progresses.get(progresses.size() - 1))

  /** Returns the current status of the query. */
  def status: StreamingQueryCommandResult.StatusResult =
    val result = executeQueryCmd(_.setStatus(true))
    result.getStatus

  /** Explain the query plan. */
  def explain(): Unit = explain(extended = false)

  /** Explain the query plan, optionally with extended details. */
  def explain(extended: Boolean): Unit =
    val result = executeQueryCmd(_.setExplain(
      StreamingQueryCommand.ExplainCommand.newBuilder()
        .setExtended(extended).build()
    ))
    println(result.getExplain.getResult)

  /** Returns the exception message if the query has terminated with an exception. */
  def exception: Option[String] =
    val result = executeQueryCmd(_.setException(true))
    if result.hasException && result.getException.hasExceptionMessage then
      Some(result.getException.getExceptionMessage)
    else None

  private def executeQueryCmd(
      setCmdFn: StreamingQueryCommand.Builder => StreamingQueryCommand.Builder
  ): StreamingQueryCommandResult =
    val cmdBuilder = StreamingQueryCommand.newBuilder()
      .setQueryId(StreamingQueryInstanceId.newBuilder()
        .setId(id).setRunId(runId).build())
    setCmdFn(cmdBuilder)
    val command = Command.newBuilder()
      .setStreamingQueryCommand(cmdBuilder.build())
      .build()
    val plan = Plan.newBuilder().setCommand(command).build()
    val responses = session.client.execute(plan)
    var result: StreamingQueryCommandResult = null
    responses.foreach { resp =>
      if resp.hasStreamingQueryCommandResult then
        result = resp.getStreamingQueryCommandResult
    }
    if result == null then
      throw new RuntimeException("No StreamingQueryCommandResult in response")
    result
