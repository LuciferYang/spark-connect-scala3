package org.apache.spark.sql.connect.client

import io.grpc.{ManagedChannel, Status, StatusRuntimeException}
import io.grpc.stub.StreamObserver
import org.apache.spark.connect.proto.*

import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** An iterator over [[ExecutePlanResponse]] that supports reattaching to an in-progress execution
  * after a transient gRPC failure. Ported from the upstream Spark Connect client and adapted for
  * Scala 3.
  *
  * The iterator automatically:
  *   - Sets `ReattachOptions(reattachable=true)` on the initial request.
  *   - Tracks the last received `response_id` so it can resume from where it left off.
  *   - Calls `ReattachExecute` when the stream ends unexpectedly (no `ResultComplete`).
  *   - Calls `ReleaseExecute` to free server-side resources as responses are consumed.
  *   - Falls back to a full re-execute (`ExecutePlan`) on `OPERATION_NOT_FOUND` /
  *     `SESSION_NOT_FOUND`, throwing a [[GrpcRetryHandler.RetryException]] to trigger the outer
  *     retry loop.
  */
class ExecutePlanResponseReattachableIterator private[client] (
    initialRequest: ExecutePlanRequest,
    channel: ManagedChannel,
    retryHandler: GrpcRetryHandler
) extends Iterator[ExecutePlanResponse]
    with AutoCloseable:

  private val operationId: String =
    if initialRequest.hasOperationId && initialRequest.getOperationId.nonEmpty then
      initialRequest.getOperationId
    else UUID.randomUUID().toString

  // Build the request with reattach options + operationId.
  private val request: ExecutePlanRequest =
    val reattachOpt = ExecutePlanRequest.RequestOption.newBuilder()
      .setReattachOptions(ReattachOptions.newBuilder().setReattachable(true).build())
      .build()
    initialRequest.toBuilder
      .setOperationId(operationId)
      .clearRequestOptions()
      .addRequestOptions(reattachOpt)
      // Preserve any existing non-reattach options.
      .addAllRequestOptions(
        initialRequest.getRequestOptionsList.asScala
          .filterNot(_.hasReattachOptions)
          .asJava
      )
      .build()

  // Raw stubs — NOT wrapped by ResponseValidator or GrpcExceptionConverter.
  private val blockingStub = SparkConnectServiceGrpc.newBlockingStub(channel)
  private val asyncStub = SparkConnectServiceGrpc.newStub(channel)

  // Stream state.
  @volatile private var iter: java.util.Iterator[ExecutePlanResponse] = _
  @volatile private var lastReturnedResponseId: Option[String] = None
  @volatile private var resultComplete: Boolean = false
  @volatile private var closed: Boolean = false

  // Start the initial execution.
  iter = rawExecutePlan(request)

  // ---------------------------------------------------------------------------
  // Iterator interface
  // ---------------------------------------------------------------------------

  override def hasNext: Boolean =
    if closed then return false
    if resultComplete then return false
    retryHandler.retry {
      var hasNextVal = callIter(_.hasNext)
      // If stream ended but no ResultComplete, reattach.
      while !hasNextVal && !resultComplete do
        iter = rawReattachExecute()
        hasNextVal = callIter(_.hasNext)
      hasNextVal
    }

  override def next(): ExecutePlanResponse =
    if closed then throw java.util.NoSuchElementException("Iterator is closed")
    retryHandler.retry {
      val resp = callIter(_.next())
      lastReturnedResponseId = Some(resp.getResponseId)
      if resp.hasResultComplete then
        resultComplete = true
        releaseAll()
      else
        releaseUntil(resp.getResponseId)
      resp
    }

  override def close(): Unit =
    if !closed then
      closed = true
      releaseAll()

  // ---------------------------------------------------------------------------
  // Core helpers
  // ---------------------------------------------------------------------------

  /** Apply `f` to the current iterator. On `OPERATION_NOT_FOUND` or `SESSION_NOT_FOUND`, re-execute
    * the original plan and throw [[GrpcRetryHandler.RetryException]] so the outer retry loop
    * restarts.
    */
  private def callIter[T](f: java.util.Iterator[ExecutePlanResponse] => T): T =
    try
      if iter == null then iter = rawReattachExecute()
      f(iter)
    catch
      case e: StatusRuntimeException if isRetryableExecuteStatus(e) =>
        iter = rawExecutePlan(request)
        throw GrpcRetryHandler.RetryException()

  private def isRetryableExecuteStatus(e: StatusRuntimeException): Boolean =
    val desc = Option(e.getStatus.getDescription).getOrElse("")
    val code = e.getStatus.getCode
    // OPERATION_NOT_FOUND or SESSION_NOT_FOUND → need full re-execute
    code == Status.Code.NOT_FOUND ||
    (code == Status.Code.INTERNAL &&
      (desc.contains("OPERATION_NOT_FOUND") || desc.contains("SESSION_NOT_FOUND")))

  // ---------------------------------------------------------------------------
  // gRPC calls
  // ---------------------------------------------------------------------------

  private def rawExecutePlan(req: ExecutePlanRequest): java.util.Iterator[ExecutePlanResponse] =
    blockingStub.executePlan(req)

  private def rawReattachExecute(): java.util.Iterator[ExecutePlanResponse] =
    val rb = ReattachExecuteRequest.newBuilder()
      .setSessionId(request.getSessionId)
      .setUserContext(request.getUserContext)
      .setOperationId(operationId)
    if request.hasClientObservedServerSideSessionId then
      rb.setClientObservedServerSideSessionId(request.getClientObservedServerSideSessionId)
    lastReturnedResponseId.foreach(rb.setLastResponseId)
    blockingStub.reattachExecute(rb.build())

  /** Release all server-side buffered responses asynchronously. */
  private def releaseAll(): Unit =
    val rb = ReleaseExecuteRequest.newBuilder()
      .setSessionId(request.getSessionId)
      .setUserContext(request.getUserContext)
      .setOperationId(operationId)
      .setReleaseAll(ReleaseExecuteRequest.ReleaseAll.getDefaultInstance)
    if request.hasClientObservedServerSideSessionId then
      rb.setClientObservedServerSideSessionId(request.getClientObservedServerSideSessionId)
    releaseAsync(rb.build())

  /** Release responses up to the given response ID asynchronously. */
  private def releaseUntil(responseId: String): Unit =
    val rb = ReleaseExecuteRequest.newBuilder()
      .setSessionId(request.getSessionId)
      .setUserContext(request.getUserContext)
      .setOperationId(operationId)
      .setReleaseUntil(
        ReleaseExecuteRequest.ReleaseUntil.newBuilder().setResponseId(responseId).build()
      )
    if request.hasClientObservedServerSideSessionId then
      rb.setClientObservedServerSideSessionId(request.getClientObservedServerSideSessionId)
    releaseAsync(rb.build())

  /** Fire-and-forget async release. On failure, retry synchronously once. */
  private def releaseAsync(req: ReleaseExecuteRequest): Unit =
    try
      asyncStub.releaseExecute(
        req,
        new StreamObserver[ReleaseExecuteResponse]:
          def onNext(value: ReleaseExecuteResponse): Unit = ()
          def onError(t: Throwable): Unit =
            // Best-effort: try blocking release once on async failure.
            try blockingStub.releaseExecute(req)
            catch case NonFatal(_) => () // give up silently
          def onCompleted(): Unit = ()
      )
    catch case NonFatal(_) => () // best-effort
