package org.apache.spark.sql.connect.client

import io.grpc.{ManagedChannel, ManagedChannelBuilder, Metadata, Status, StatusRuntimeException}
import io.grpc.stub.MetadataUtils
import org.apache.spark.connect.proto.*

import java.net.URI
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal
import org.apache.spark.sql.StorageLevel

/** Core gRPC client for communicating with Spark Connect Server.
  *
  * All methods are synchronous (blocking). The thin API layer in SparkSession/DataFrame calls these
  * directly — no effect wrapper needed for the MVP.
  */
final class SparkConnectClient private (
    private val channel: ManagedChannel,
    private val bstub: SparkConnectServiceGrpc.SparkConnectServiceBlockingStub,
    private val asyncStub: SparkConnectServiceGrpc.SparkConnectServiceStub,
    val sessionId: String,
    val userId: String,
    private val retryHandler: GrpcRetryHandler
):

  /** Manages uploading artifacts (class files, JARs) to the server. */
  val artifactManager: ArtifactManager = ArtifactManager(sessionId, userId, asyncStub)

  // ---------------------------------------------------------------------------
  // Execute
  // ---------------------------------------------------------------------------

  /** Execute a plan and return a lazy iterator of responses. */
  def execute(plan: Plan): Iterator[ExecutePlanResponse] =
    artifactManager.uploadAllClassFileArtifacts()
    val request = ExecutePlanRequest.newBuilder()
      .setSessionId(sessionId)
      .setUserContext(UserContext.newBuilder().setUserId(userId).build())
      .setPlan(plan)
      .setOperationId(UUID.randomUUID().toString)
      .build()
    GrpcExceptionConverter.convert(retryHandler.retry(bstub.executePlan(request).asScala))

  // ---------------------------------------------------------------------------
  // Analyze
  // ---------------------------------------------------------------------------

  /** Retrieve the schema of a plan without executing it. */
  def analyzeSchema(plan: Plan): AnalyzePlanResponse =
    artifactManager.uploadAllClassFileArtifacts()
    val request = AnalyzePlanRequest.newBuilder()
      .setSessionId(sessionId)
      .setUserContext(UserContext.newBuilder().setUserId(userId).build())
      .setSchema(AnalyzePlanRequest.Schema.newBuilder().setPlan(plan).build())
      .build()
    GrpcExceptionConverter.convert(retryHandler.retry(bstub.analyzePlan(request)))

  /** Retrieve the explain string for a plan without executing it. */
  def analyzeExplain(
      plan: Plan,
      mode: AnalyzePlanRequest.Explain.ExplainMode =
        AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_SIMPLE
  ): String =
    val request = AnalyzePlanRequest.newBuilder()
      .setSessionId(sessionId)
      .setUserContext(UserContext.newBuilder().setUserId(userId).build())
      .setExplain(AnalyzePlanRequest.Explain.newBuilder().setPlan(
        plan
      ).setExplainMode(mode).build())
      .build()
    val resp = GrpcExceptionConverter.convert(retryHandler.retry(bstub.analyzePlan(request)))
    if resp.hasExplain then resp.getExplain.getExplainString
    else "(no explain output)"

  /** Retrieve the Spark version from the server. */
  def version(): String =
    val request = AnalyzePlanRequest.newBuilder()
      .setSessionId(sessionId)
      .setUserContext(UserContext.newBuilder().setUserId(userId).build())
      .setSparkVersion(AnalyzePlanRequest.SparkVersion.getDefaultInstance)
      .build()
    val resp = GrpcExceptionConverter.convert(retryHandler.retry(bstub.analyzePlan(request)))
    if resp.hasSparkVersion then resp.getSparkVersion.getVersion
    else "unknown"

  // ---------------------------------------------------------------------------
  // Config
  // ---------------------------------------------------------------------------

  def getConfig(key: String): String =
    val request = ConfigRequest.newBuilder()
      .setSessionId(sessionId)
      .setUserContext(UserContext.newBuilder().setUserId(userId).build())
      .setOperation(ConfigRequest.Operation.newBuilder()
        .setGet(ConfigRequest.Get.newBuilder().addKeys(key).build())
        .build())
      .build()
    val resp = GrpcExceptionConverter.convert(retryHandler.retry(bstub.config(request)))
    val pairs = resp.getPairsList.asScala
    pairs.headOption.map(_.getValue).getOrElse("")

  def setConfig(key: String, value: String): Unit =
    val request = ConfigRequest.newBuilder()
      .setSessionId(sessionId)
      .setUserContext(UserContext.newBuilder().setUserId(userId).build())
      .setOperation(ConfigRequest.Operation.newBuilder()
        .setSet(ConfigRequest.Set.newBuilder()
          .addPairs(KeyValue.newBuilder().setKey(key).setValue(value).build())
          .build())
        .build())
      .build()
    GrpcExceptionConverter.convert(retryHandler.retry(bstub.config(request)))

  // ---------------------------------------------------------------------------
  // Execute Command
  // ---------------------------------------------------------------------------

  /** Execute a command (write, create view, etc.) and consume all responses. */
  def executeCommand(command: Command): Unit =
    val plan = Plan.newBuilder().setCommand(command).build()
    val responses = execute(plan)
    responses.foreach(_ => ()) // drain iterator

  // ---------------------------------------------------------------------------
  // Interrupt / Close
  // ---------------------------------------------------------------------------

  def interrupt(): Unit =
    val request = InterruptRequest.newBuilder()
      .setSessionId(sessionId)
      .setUserContext(UserContext.newBuilder().setUserId(userId).build())
      .setInterruptType(InterruptRequest.InterruptType.INTERRUPT_TYPE_ALL)
      .build()
    try GrpcExceptionConverter.convert(retryHandler.retry(bstub.interrupt(request)))
    catch case NonFatal(_) => () // best-effort

  def close(): Unit =
    try
      channel.shutdown()
      if !channel.awaitTermination(5, TimeUnit.SECONDS) then
        channel.shutdownNow()
    catch case NonFatal(_) => channel.shutdownNow()

  // ---------------------------------------------------------------------------
  // Generic Analyze helper
  // ---------------------------------------------------------------------------

  /** Send an AnalyzePlan request built by the caller. */
  def analyzePlan(
      f: AnalyzePlanRequest.Builder => AnalyzePlanRequest.Builder
  ): AnalyzePlanResponse =
    val base = AnalyzePlanRequest.newBuilder()
      .setSessionId(sessionId)
      .setUserContext(UserContext.newBuilder().setUserId(userId).build())
    val request = f(base).build()
    GrpcExceptionConverter.convert(retryHandler.retry(bstub.analyzePlan(request)))

object SparkConnectClient:

  /** Parse a `sc://host:port` URL into (host, port, params). */
  private def parseUrl(url: String): (String, Int, Map[String, String]) =
    // sc://host:port;key=value;key=value
    val stripped = url.stripPrefix("sc://")
    val parts = stripped.split(";").toSeq
    val hostPort = parts.head.split(":")
    val host = hostPort(0)
    val port = if hostPort.length > 1 then hostPort(1).toInt else 15002
    val params = parts.tail.flatMap { p =>
      p.split("=", 2) match
        case Array(k, v) => Some(k -> v)
        case _           => None
    }.toMap
    (host, port, params)

  /** Create a client connected to the given `sc://` URL. */
  def create(
      url: String,
      sessionId: String = UUID.randomUUID().toString,
      configs: Map[String, String] = Map.empty
  ): SparkConnectClient =
    val (host, port, params) = parseUrl(url)
    val userId = params.getOrElse("user_id", System.getProperty("user.name", "anonymous"))
    val token =
      params.get("token").orElse(Option(System.getenv("SPARK_CONNECT_AUTHENTICATE_TOKEN")))
    val useSsl = params.get("use_ssl").exists(_.equalsIgnoreCase("true"))

    val channelBuilder = ManagedChannelBuilder
      .forAddress(host, port)
      .maxInboundMessageSize(128 * 1024 * 1024) // 128 MB
      .userAgent("spark-connect-scala3/0.1.0")

    if !useSsl then channelBuilder.usePlaintext()

    val channel = channelBuilder.build()

    val baseStub = SparkConnectServiceGrpc.newBlockingStub(channel)
    val baseAsyncStub = SparkConnectServiceGrpc.newStub(channel)
    val (stub, aStub) = token match
      case Some(t) =>
        val metadata = Metadata()
        metadata.put(
          Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
          s"Bearer $t"
        )
        val interceptor = MetadataUtils.newAttachHeadersInterceptor(metadata)
        (baseStub.withInterceptors(interceptor), baseAsyncStub.withInterceptors(interceptor))
      case None => (baseStub, baseAsyncStub)

    val client = SparkConnectClient(
      channel,
      stub,
      aStub,
      sessionId,
      userId,
      GrpcRetryHandler(RetryPolicy.defaultPolicy())
    )

    configs.foreach((k, v) => client.setConfig(k, v))
    client
