package org.apache.spark.sql.connect.client

import io.grpc.{ManagedChannel, ManagedChannelBuilder, Metadata, Status, StatusRuntimeException}
import io.grpc.stub.MetadataUtils
import org.apache.spark.connect.proto.base.*
// KeyValue is in base.* wildcard import

import java.net.URI
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/**
 * Core gRPC client for communicating with Spark Connect Server.
 *
 * All methods are synchronous (blocking). The thin API layer in SparkSession/DataFrame
 * calls these directly — no effect wrapper needed for the MVP.
 */
final class SparkConnectClient private (
    private val channel: ManagedChannel,
    private val bstub: SparkConnectServiceGrpc.SparkConnectServiceBlockingStub,
    val sessionId: String,
    val userId: String
):

  // ---------------------------------------------------------------------------
  // Execute
  // ---------------------------------------------------------------------------

  /** Execute a plan and return a lazy iterator of responses. */
  def execute(plan: Plan): Iterator[ExecutePlanResponse] =
    val request = ExecutePlanRequest(
      sessionId = sessionId,
      userContext = Some(UserContext(userId = userId)),
      plan = Some(plan),
      operationId = Some(UUID.randomUUID().toString)
    )
    retryOnUnavailable { bstub.executePlan(request) }

  // ---------------------------------------------------------------------------
  // Analyze
  // ---------------------------------------------------------------------------

  /** Retrieve the schema of a plan without executing it. */
  def analyzeSchema(plan: Plan): AnalyzePlanResponse =
    val request = AnalyzePlanRequest(
      sessionId = sessionId,
      userContext = Some(UserContext(userId = userId)),
      analyze = AnalyzePlanRequest.Analyze.Schema(
        AnalyzePlanRequest.Schema(plan = Some(plan))
      )
    )
    retryOnUnavailable { bstub.analyzePlan(request) }

  /** Retrieve the Spark version from the server. */
  def version(): String =
    val request = AnalyzePlanRequest(
      sessionId = sessionId,
      userContext = Some(UserContext(userId = userId)),
      analyze = AnalyzePlanRequest.Analyze.SparkVersion(
        AnalyzePlanRequest.SparkVersion()
      )
    )
    val resp = retryOnUnavailable { bstub.analyzePlan(request) }
    resp.result match
      case AnalyzePlanResponse.Result.SparkVersion(v) => v.version
      case _ => "unknown"

  // ---------------------------------------------------------------------------
  // Config
  // ---------------------------------------------------------------------------

  def getConfig(key: String): String =
    val request = ConfigRequest(
      sessionId = sessionId,
      userContext = Some(UserContext(userId = userId)),
      operation = Some(ConfigRequest.Operation(
        opType = ConfigRequest.Operation.OpType.Get(ConfigRequest.Get(keys = Seq(key)))
      ))
    )
    val resp = retryOnUnavailable { bstub.config(request) }
    resp.pairs.headOption.flatMap(_.value).getOrElse("")

  def setConfig(key: String, value: String): Unit =
    val request = ConfigRequest(
      sessionId = sessionId,
      userContext = Some(UserContext(userId = userId)),
      operation = Some(ConfigRequest.Operation(
        opType = ConfigRequest.Operation.OpType.Set(
          ConfigRequest.Set(pairs = Seq(KeyValue(key = key, value = Some(value))))
        )
      ))
    )
    retryOnUnavailable { bstub.config(request) }

  // ---------------------------------------------------------------------------
  // Interrupt / Close
  // ---------------------------------------------------------------------------

  def interrupt(): Unit =
    val request = InterruptRequest(
      sessionId = sessionId,
      userContext = Some(UserContext(userId = userId)),
      interruptType = InterruptRequest.InterruptType.INTERRUPT_TYPE_ALL
    )
    try retryOnUnavailable { bstub.interrupt(request) }
    catch case NonFatal(_) => () // best-effort

  def close(): Unit =
    try
      channel.shutdown()
      if !channel.awaitTermination(5, TimeUnit.SECONDS) then
        channel.shutdownNow()
    catch case NonFatal(_) => channel.shutdownNow()

  // ---------------------------------------------------------------------------
  // Retry helper (simple: retry once on UNAVAILABLE)
  // ---------------------------------------------------------------------------

  private def retryOnUnavailable[T](op: => T): T =
    try op
    catch
      case e: StatusRuntimeException if e.getStatus.getCode == Status.Code.UNAVAILABLE =>
        Thread.sleep(500)
        op

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
        case _ => None
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
    val token = params.get("token").orElse(Option(System.getenv("SPARK_CONNECT_AUTHENTICATE_TOKEN")))
    val useSsl = params.get("use_ssl").exists(_.equalsIgnoreCase("true"))

    val channelBuilder = ManagedChannelBuilder
      .forAddress(host, port)
      .maxInboundMessageSize(128 * 1024 * 1024) // 128 MB
      .userAgent("spark-connect-scala3/0.1.0")

    if !useSsl then channelBuilder.usePlaintext()

    val channel = channelBuilder.build()

    val baseStub = SparkConnectServiceGrpc.blockingStub(channel)
    val stub = token match
      case Some(t) =>
        val metadata = Metadata()
        metadata.put(
          Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
          s"Bearer $t"
        )
        baseStub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata))
      case None => baseStub

    val client = SparkConnectClient(channel, stub, sessionId, userId)

    configs.foreach { (k, v) => client.setConfig(k, v) }
    client
