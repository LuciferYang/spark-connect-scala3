package org.apache.spark.sql.connect.client

import io.grpc.{ClientInterceptor, ManagedChannelBuilder, Metadata}
import io.grpc.stub.MetadataUtils
import org.apache.spark.connect.proto.*

import java.util.UUID
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Core gRPC client for communicating with Spark Connect Server.
  *
  * All methods are synchronous (blocking). The thin API layer in SparkSession/DataFrame calls these
  * directly — no effect wrapper needed for the MVP.
  */
final class SparkConnectClient private (
    private val sharedChannel: SharedChannel,
    private val bstub: SparkConnectServiceGrpc.SparkConnectServiceBlockingStub,
    private val asyncStub: SparkConnectServiceGrpc.SparkConnectServiceStub,
    val sessionId: String,
    val userId: String,
    private val retryHandler: GrpcRetryHandler,
    private val connectionUrl: String,
    private val token: Option[String],
    private val planCompressionEnabled: Boolean = true
):

  /** The connection URL with any token redacted — safe for logging/display. */
  private[sql] def redactedUrl: String = connectionUrl

  /** Reconstruct the full URL (with token) for internal use only.
    *
    * SECURITY: contains plaintext token — never log, serialize, or expose this value. Use
    * [[redactedUrl]] anywhere user-visible.
    */
  private def fullUrl: String = token match
    case Some(t) =>
      s"$connectionUrl;token=${UrlEncoding.encode(t)}"
    case None => connectionUrl

  override def toString: String =
    s"SparkConnectClient(session=$sessionId, url=$connectionUrl)"

  /** Manages uploading artifacts (class files, JARs) to the server. */
  val artifactManager: ArtifactManager = ArtifactManager(sessionId, userId, asyncStub)

  // ---------------------------------------------------------------------------
  // Operation Tags (InheritableThreadLocal — child threads inherit a copy)
  // ---------------------------------------------------------------------------

  private val tags = new InheritableThreadLocal[mutable.Set[String]]:
    override def childValue(parent: mutable.Set[String]): mutable.Set[String] =
      if parent == null then mutable.HashSet.empty[String] else parent.clone()
    override protected def initialValue(): mutable.Set[String] = mutable.HashSet.empty[String]

  def addTag(tag: String): Unit =
    require(tag != null && tag.nonEmpty, "Tag cannot be null or empty")
    require(!tag.contains(","), "Tag cannot contain ','")
    tags.get.add(tag)

  def removeTag(tag: String): Unit =
    require(tag != null && tag.nonEmpty, "Tag cannot be null or empty")
    tags.get.remove(tag)

  def getTags(): Set[String] = tags.get.toSet

  def clearTags(): Unit = tags.get.clear()

  /** Validates server-side session ID consistency across all responses. */
  private val responseValidator = ResponseValidator()

  /** The server-side session ID observed from the server's first response. */
  def serverSideSessionId: Option[String] = responseValidator.getServerSideSessionId

  /** Whether the underlying gRPC channel has been shut down. */
  def isChannelShutdown: Boolean = sharedChannel.isShutdown

  // ---------------------------------------------------------------------------
  // Common helpers
  // ---------------------------------------------------------------------------

  private def userContext: UserContext =
    UserContext.newBuilder().setUserId(userId).build()

  private def addClientObservedSessionId(
      setFn: String => Unit
  ): Unit =
    serverSideSessionId.foreach(setFn)

  // ---------------------------------------------------------------------------
  // Plan Compression (ZSTD)
  // ---------------------------------------------------------------------------

  private[client] case class PlanCompressionOptions(thresholdBytes: Int, algorithm: String)

  /** Lifecycle of the cached plan-compression configuration. Modeled as an explicit three-state
    * enum so the cases (not yet fetched / known-unavailable / available with options) are
    * self-describing rather than encoded as `Option[Option[_]]`.
    */
  private[client] enum CompressionState:
    case Uninitialized
    case Disabled
    case Enabled(opts: PlanCompressionOptions)

  @volatile private var _planCompressionOptions: CompressionState = CompressionState.Uninitialized

  /** Lazily fetch compression options from server config (cached after first call).
    *
    * Permanently disables compression on ClassNotFoundException, UnsupportedOperationException
    * (feature truly unavailable), or NumberFormatException (server returned an unparsable threshold
    * — typically empty string when the config key is unknown). Transient errors (e.g., network
    * issues) leave the cache `Uninitialized` so callers can retry.
    */
  private def getPlanCompressionOptions: CompressionState =
    if !planCompressionEnabled then return CompressionState.Disabled
    _planCompressionOptions match
      case CompressionState.Uninitialized =>
        synchronized {
          _planCompressionOptions match
            case CompressionState.Uninitialized =>
              try
                val state = CompressionState.Enabled(PlanCompressionOptions(
                  thresholdBytes =
                    getConfig("spark.connect.session.planCompression.threshold").toInt,
                  algorithm =
                    getConfig("spark.connect.session.planCompression.defaultAlgorithm")
                ))
                _planCompressionOptions = state
                state
              catch
                case _: ClassNotFoundException | _: UnsupportedOperationException |
                    _: NumberFormatException =>
                  _planCompressionOptions = CompressionState.Disabled
                  CompressionState.Disabled
                case e if NonFatal(e) =>
                  // Transient error — do NOT cache; let caller retry later
                  CompressionState.Uninitialized
            case cached => cached // double-check
        }
      case cached => cached

  /** Try to compress the plan if it exceeds the threshold. Returns the original plan if compression
    * is disabled, not needed, or not effective.
    */
  private[client] def tryCompressPlan(plan: Plan): Plan =
    getPlanCompressionOptions match
      case CompressionState.Enabled(opts)
          if opts.algorithm == "ZSTD" && opts.thresholdBytes >= 0 =>
        val opTypeCase = plan.getOpTypeCase
        val (innerBytes, opType) = opTypeCase match
          case Plan.OpTypeCase.ROOT =>
            (plan.getRoot.toByteArray, Plan.CompressedOperation.OpType.OP_TYPE_RELATION)
          case Plan.OpTypeCase.COMMAND =>
            (plan.getCommand.toByteArray, Plan.CompressedOperation.OpType.OP_TYPE_COMMAND)
          case _ => return plan
        if innerBytes.length <= opts.thresholdBytes then return plan
        try
          import com.github.luben.zstd.Zstd
          val compressed = Zstd.compress(innerBytes)
          if compressed.length >= innerBytes.length then return plan
          Plan.newBuilder().setCompressedOperation(
            Plan.CompressedOperation.newBuilder()
              .setData(com.google.protobuf.ByteString.copyFrom(compressed))
              .setOpType(opType)
              .setCompressionCodec(CompressionCodec.COMPRESSION_CODEC_ZSTD)
              .build()
          ).build()
        catch
          // Intentional fallback: if the zstd-jni library is not on the classpath,
          // permanently disable compression for this session. This is expected in
          // environments where the optional zstd dependency is not provided.
          case _: NoClassDefFoundError | _: ClassNotFoundException =>
            _planCompressionOptions = CompressionState.Disabled; plan
          case NonFatal(_) =>
            _planCompressionOptions = CompressionState.Disabled; plan
      case _ => plan

  // ---------------------------------------------------------------------------
  // Execute
  // ---------------------------------------------------------------------------

  /** Execute a plan and return a lazy iterator of responses. The returned iterator is
    * `AutoCloseable` — callers should close it after use to release server-side resources.
    */
  def execute(plan: Plan): Iterator[ExecutePlanResponse] & AutoCloseable =
    artifactManager.uploadAllClassFileArtifacts()
    val rb = ExecutePlanRequest.newBuilder()
      .setSessionId(sessionId)
      .setUserContext(userContext)
      .setPlan(tryCompressPlan(plan))
      .setOperationId(UUID.randomUUID().toString)
    addClientObservedSessionId(rb.setClientObservedServerSideSessionId)
    val currentTags = tags.get
    if currentTags.nonEmpty then currentTags.foreach(rb.addTags)
    val inner =
      ExecutePlanResponseReattachableIterator(rb.build(), sharedChannel.underlying, retryHandler)
    val validated = responseValidator.wrapIterator(inner)
    // Wrap with GrpcExceptionConverter (with FetchErrorDetails support).
    new Iterator[ExecutePlanResponse] with AutoCloseable:
      def hasNext: Boolean = GrpcExceptionConverter.convert(validated.hasNext, fetchErrorDetails)
      def next(): ExecutePlanResponse =
        GrpcExceptionConverter.convert(validated.next(), fetchErrorDetails)
      def close(): Unit = validated.close()

  // ---------------------------------------------------------------------------
  // Analyze
  // ---------------------------------------------------------------------------

  /** Retrieve the schema of a plan without executing it. */
  def analyzeSchema(plan: Plan): AnalyzePlanResponse =
    artifactManager.uploadAllClassFileArtifacts()
    val rb = AnalyzePlanRequest.newBuilder()
      .setSessionId(sessionId)
      .setUserContext(userContext)
      .setSchema(AnalyzePlanRequest.Schema.newBuilder().setPlan(plan).build())
    addClientObservedSessionId(rb.setClientObservedServerSideSessionId)
    responseValidator.verifyResponse(
      GrpcExceptionConverter.convert(retryHandler.retry(bstub.analyzePlan(rb.build())))
    )

  /** Retrieve the explain string for a plan without executing it. */
  def analyzeExplain(
      plan: Plan,
      mode: AnalyzePlanRequest.Explain.ExplainMode =
        AnalyzePlanRequest.Explain.ExplainMode.EXPLAIN_MODE_SIMPLE
  ): String =
    val rb = AnalyzePlanRequest.newBuilder()
      .setSessionId(sessionId)
      .setUserContext(userContext)
      .setExplain(
        AnalyzePlanRequest.Explain.newBuilder().setPlan(plan).setExplainMode(mode).build()
      )
    addClientObservedSessionId(rb.setClientObservedServerSideSessionId)
    val resp = responseValidator.verifyResponse(
      GrpcExceptionConverter.convert(retryHandler.retry(bstub.analyzePlan(rb.build())))
    )
    if resp.hasExplain then resp.getExplain.getExplainString
    else "(no explain output)"

  /** Retrieve the Spark version from the server. */
  def version(): String =
    val rb = AnalyzePlanRequest.newBuilder()
      .setSessionId(sessionId)
      .setUserContext(userContext)
      .setSparkVersion(AnalyzePlanRequest.SparkVersion.getDefaultInstance)
    addClientObservedSessionId(rb.setClientObservedServerSideSessionId)
    val resp = responseValidator.verifyResponse(
      GrpcExceptionConverter.convert(retryHandler.retry(bstub.analyzePlan(rb.build())))
    )
    if resp.hasSparkVersion then resp.getSparkVersion.getVersion
    else "unknown"

  // ---------------------------------------------------------------------------
  // Config
  // ---------------------------------------------------------------------------

  /** Execute a config operation and return the response. */
  private def executeConfigOp(
      setOp: ConfigRequest.Operation.Builder => ConfigRequest.Operation.Builder
  ): ConfigResponse =
    val rb = ConfigRequest.newBuilder()
      .setSessionId(sessionId)
      .setUserContext(userContext)
      .setOperation(setOp(ConfigRequest.Operation.newBuilder()).build())
    addClientObservedSessionId(rb.setClientObservedServerSideSessionId)
    responseValidator.verifyResponse(
      GrpcExceptionConverter.convert(retryHandler.retry(bstub.config(rb.build())))
    )

  def getConfig(key: String): String =
    val resp = executeConfigOp(_.setGet(ConfigRequest.Get.newBuilder().addKeys(key).build()))
    resp.getPairsList.asScala.headOption.map(_.getValue).getOrElse("")

  def setConfig(key: String, value: String): Unit =
    executeConfigOp(
      _.setSet(
        ConfigRequest.Set.newBuilder()
          .addPairs(KeyValue.newBuilder().setKey(key).setValue(value).build())
          .build()
      )
    )

  def getConfigOption(key: String): Option[String] =
    val resp =
      executeConfigOp(_.setGetOption(ConfigRequest.GetOption.newBuilder().addKeys(key).build()))
    resp.getPairsList.asScala.headOption.flatMap(p => if p.hasValue then Some(p.getValue) else None)

  def getAllConfig(): Map[String, String] =
    val resp = executeConfigOp(_.setGetAll(ConfigRequest.GetAll.newBuilder().build()))
    resp.getPairsList.asScala.map(p => p.getKey -> p.getValue).toMap

  def unsetConfig(key: String): Unit =
    executeConfigOp(_.setUnset(ConfigRequest.Unset.newBuilder().addKeys(key).build()))

  def isModifiableConfig(key: String): Boolean =
    val resp = executeConfigOp(
      _.setIsModifiable(ConfigRequest.IsModifiable.newBuilder().addKeys(key).build())
    )
    resp.getPairsList.asScala.headOption.exists(_.getValue == "true")

  // ---------------------------------------------------------------------------
  // Execute Command
  // ---------------------------------------------------------------------------

  /** Execute a command (write, create view, etc.) and consume all responses. */
  def executeCommand(command: Command): Unit =
    val plan = Plan.newBuilder().setCommand(command).build()
    val responses = execute(plan)
    try responses.foreach(_ => ()) // drain iterator
    finally (responses: Any) match
        case c: AutoCloseable => c.close()
        case _                => ()

  /** Execute a command and return all responses (for commands that produce results). */
  def executeCommandWithResponses(command: Command): Seq[ExecutePlanResponse] =
    val plan = Plan.newBuilder().setCommand(command).build()
    val responses = execute(plan)
    try responses.toSeq
    finally (responses: Any) match
        case c: AutoCloseable => c.close()
        case _                => ()

  // ---------------------------------------------------------------------------
  // Interrupt / Close / New Client
  // ---------------------------------------------------------------------------

  // ---------------------------------------------------------------------------
  // Interrupt
  // ---------------------------------------------------------------------------

  /** Interrupt all running operations (backward-compatible alias). */
  def interrupt(): Unit = interruptAll()

  /** Interrupt all running operations in this session. */
  def interruptAll(): Seq[String] =
    doInterrupt(InterruptRequest.InterruptType.INTERRUPT_TYPE_ALL, None, None)

  /** Interrupt all running operations tagged with the given tag. */
  def interruptTag(tag: String): Seq[String] =
    doInterrupt(InterruptRequest.InterruptType.INTERRUPT_TYPE_TAG, Some(tag), None)

  /** Interrupt the running operation with the given operation ID. */
  def interruptOperation(operationId: String): Seq[String] =
    doInterrupt(InterruptRequest.InterruptType.INTERRUPT_TYPE_OPERATION_ID, None, Some(operationId))

  private def doInterrupt(
      intType: InterruptRequest.InterruptType,
      operationTag: Option[String],
      opId: Option[String]
  ): Seq[String] =
    val rb = InterruptRequest.newBuilder()
      .setSessionId(sessionId)
      .setUserContext(userContext)
      .setInterruptType(intType)
    operationTag.foreach(rb.setOperationTag)
    opId.foreach(rb.setOperationId)
    addClientObservedSessionId(rb.setClientObservedServerSideSessionId)
    try
      val resp = responseValidator.verifyResponse(
        GrpcExceptionConverter.convert(retryHandler.retry(bstub.interrupt(rb.build())))
      )
      resp.getInterruptedIdsList.asScala.toSeq
    catch
      // Known limitation: interrupt failures are swallowed to maintain API compatibility.
      // The caller receives an empty Seq and cannot distinguish "nothing interrupted" from
      // "interrupt RPC itself failed". This matches the official Spark Connect client behavior.
      case NonFatal(_) => Seq.empty

  def close(): Unit =
    sharedChannel.release()

  /** Create a new client connected to the same server but with a fresh session ID. */
  def newClient(): SparkConnectClient =
    SparkConnectClient.create(fullUrl)

  /** Clone the current session on the server (preserving config, temp views, UDFs). */
  def cloneSession(): SparkConnectClient =
    val rb = CloneSessionRequest.newBuilder()
      .setSessionId(sessionId)
      .setUserContext(userContext)
    serverSideSessionId.foreach(rb.setClientObservedServerSideSessionId)
    val resp = responseValidator.verifyResponse(
      GrpcExceptionConverter.convert(retryHandler.retry(bstub.cloneSession(rb.build())))
    )
    val clonedSessionId = resp.getSessionId
    // Retain after RPC success to avoid leaking a ref count on failure
    val retained = sharedChannel.retain()
    try
      SparkConnectClient(
        retained,
        bstub,
        asyncStub,
        clonedSessionId,
        userId,
        retryHandler,
        connectionUrl,
        token
      )
    catch
      case e: Exception =>
        retained.release()
        throw e

  /** Fetch enriched error details from the server for the given error ID. */
  private[client] def fetchErrorDetails(
      errorId: String
  ): Option[FetchErrorDetailsResponse] =
    try
      val rb = FetchErrorDetailsRequest.newBuilder()
        .setSessionId(sessionId)
        .setUserContext(userContext)
        .setErrorId(errorId)
      serverSideSessionId.foreach(rb.setClientObservedServerSideSessionId)
      Some(retryHandler.retry(bstub.fetchErrorDetails(rb.build())))
    catch case NonFatal(_) => None

  // ---------------------------------------------------------------------------
  // Generic Analyze helper
  // ---------------------------------------------------------------------------

  /** Send an AnalyzePlan request built by the caller. */
  def analyzePlan(
      f: AnalyzePlanRequest.Builder => AnalyzePlanRequest.Builder
  ): AnalyzePlanResponse =
    val base = AnalyzePlanRequest.newBuilder()
      .setSessionId(sessionId)
      .setUserContext(userContext)
    addClientObservedSessionId(base.setClientObservedServerSideSessionId)
    val request = f(base).build()
    responseValidator.verifyResponse(
      GrpcExceptionConverter.convert(retryHandler.retry(bstub.analyzePlan(request)))
    )

object SparkConnectClient:

  /** Maximum inbound gRPC message size (128 MB).
    *
    * S-13: This is a client-side cap on the largest single gRPC response frame. Combined with the
    * per-batch Arrow allocator (capped at 256 GB), a malicious server could still push large
    * payloads — but the 128 MB frame limit bounds the per-message memory spike. Override via a
    * custom `ManagedChannelBuilder` if a tighter limit is needed.
    */
  private val MaxInboundMessageSize: Int = 128 * 1024 * 1024

  /** Default Spark Connect server port, used when `sc://host` is given with no explicit `:port`. */
  private[client] val DefaultPort: Int = 15002

  /** Client identifier sent to the server (without version). Kept stable across releases. */
  private val ClientName: String = "spark-connect-scala3"

  /** Client version, read from the JAR manifest's `Implementation-Version` attribute populated by
    * sbt at publish time. Falls back to `"unknown"` when running from sbt in dev mode where no
    * manifest is available.
    */
  private val ClientVersion: String =
    Option(classOf[SparkConnectClient].getPackage)
      .flatMap(p => Option(p.getImplementationVersion))
      .getOrElse("unknown")

  /** User-Agent string sent with all gRPC requests, e.g. `spark-connect-scala3/0.5.0`. */
  private val UserAgentString: String = s"$ClientName/$ClientVersion"

  /** Parse a `sc://host:port` URL into (host, port, params). Params preserve insertion order. Note:
    * IPv6 literal addresses (e.g., `[::1]`) are not supported.
    */
  private[client] def parseUrl(url: String): (String, Int, Seq[(String, String)]) =
    require(url != null, "Spark Connect URL must not be null")
    if !url.startsWith("sc://") then
      throw new IllegalArgumentException(
        "Invalid Spark Connect URL: must start with 'sc://'. Expected format: sc://host[:port][;key=value...]"
      )
    // sc://host:port;key=value;key=value
    val stripped = url.stripPrefix("sc://")
    val parts = stripped.split(";").toSeq
    val hostPortStr = parts.head
    // Split host and port — only split on the LAST colon to be forward-compatible
    val (host, port) = hostPortStr.lastIndexOf(':') match
      case -1 =>
        val h = hostPortStr.trim
        if h.isEmpty then
          throw new IllegalArgumentException(
            "Invalid Spark Connect URL: host must not be empty. Expected format: sc://host[:port][;key=value...]"
          )
        (h, DefaultPort)
      case idx =>
        val h = hostPortStr.substring(0, idx).trim
        val portStr = hostPortStr.substring(idx + 1).trim
        if h.isEmpty then
          throw new IllegalArgumentException(
            "Invalid Spark Connect URL: host must not be empty. Expected format: sc://host[:port][;key=value...]"
          )
        if portStr.isEmpty then (h, DefaultPort)
        else
          val p =
            try portStr.toInt
            catch
              case _: NumberFormatException =>
                throw new IllegalArgumentException(
                  s"Invalid Spark Connect URL: port must be a valid integer, got '$portStr'"
                )
          if p < 1 || p > 65535 then
            throw new IllegalArgumentException(
              s"Invalid Spark Connect URL: port must be between 1 and 65535, got $p"
            )
          (h, p)
    val params = parts.tail.flatMap { p =>
      p.split("=", 2) match
        case Array(k, v) =>
          val (decodedKey, decodedValue) =
            try
              (UrlEncoding.decode(k), UrlEncoding.decode(v))
            catch
              case e: IllegalArgumentException =>
                // Redact the raw value to prevent token/credential leakage in error messages.
                val redacted = s"${k.take(20)}=<redacted>"
                throw IllegalArgumentException(
                  s"Invalid URL-encoded value in Spark Connect URL parameter '$redacted': " +
                    s"${e.getMessage}",
                  e
                )
          val trimmedKey = decodedKey.trim
          require(
            trimmedKey.nonEmpty,
            // Don't echo the raw value — the key alone is enough context.
            s"Invalid Spark Connect URL parameter (key must be non-empty)"
          )
          // Store the trimmed key so e.g. ` user_id =bob` matches paramMap.get("user_id")
          // later in `create()`. Values are kept verbatim — users may legitimately need
          // trailing space in an option value.
          Some(trimmedKey -> decodedValue)
        case _ =>
          throw IllegalArgumentException(
            // Include only the key part, not the value, to avoid credential leakage.
            s"Invalid Spark Connect URL parameter '${p.takeWhile(_ != '=')}': expected 'key=value' format"
          )
    }
    (host, port, params)

  /** Create a client connected to the given `sc://` URL.
    *
    * `interceptors` are gRPC `ClientInterceptor` instances applied to the channel for cross-cutting
    * concerns (tracing, metrics, custom auth). They are applied in addition to the token-based auth
    * interceptor (when a token is supplied) and follow gRPC's "interceptors added last are executed
    * first" ordering.
    */
  def create(
      url: String,
      sessionId: String = UUID.randomUUID().toString,
      configs: Map[String, String] = Map.empty,
      interceptors: List[ClientInterceptor] = List.empty,
      maxInboundMessageSize: Option[Int] = None,
      retryPolicy: RetryPolicy = RetryPolicy.defaultPolicy(),
      planCompressionEnabled: Boolean = true
  ): SparkConnectClient =
    val (host, port, params) = parseUrl(url)
    val paramMap = params.toMap
    // S-12: userId defaults to the OS `user.name` system property. This is the same behavior as
    // upstream Spark Connect. Override via `sc://host:port;user_id=<explicit>` if the default
    // is undesirable (e.g., in shared environments where the OS user is a service account).
    val userId = paramMap.getOrElse("user_id", System.getProperty("user.name", "anonymous"))
    val token =
      paramMap.get("token").orElse(Option(System.getenv("SPARK_CONNECT_AUTHENTICATE_TOKEN")))
    val useSsl = paramMap.get("use_ssl").exists(_.equalsIgnoreCase("true")) ||
      token.isDefined // upstream: setting a token implicitly enables TLS

    // Build a sanitized URL without the token for storage (preserving original param order)
    val sanitizedParams = params.filterNot(_._1 == "token")
    val sanitizedUrl = buildSanitizedUrl(host, port, sanitizedParams)

    val channelBuilder = ManagedChannelBuilder
      .forAddress(host, port)
      .maxInboundMessageSize(maxInboundMessageSize.getOrElse(MaxInboundMessageSize))
      .userAgent(UserAgentString)

    if !useSsl then channelBuilder.usePlaintext()

    val channel = channelBuilder.build()

    val baseStub = SparkConnectServiceGrpc.newBlockingStub(channel)
    val baseAsyncStub = SparkConnectServiceGrpc.newStub(channel)
    val tokenInterceptor: Option[io.grpc.ClientInterceptor] = token.map { t =>
      val metadata = Metadata()
      metadata.put(
        Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
        s"Bearer $t"
      )
      MetadataUtils.newAttachHeadersInterceptor(metadata)
    }
    // Apply user-supplied interceptors first, then the token interceptor (if any). gRPC
    // executes interceptors in reverse-of-addition order, so the token interceptor runs
    // closest to the wire — matching upstream behavior where MetadataHeaderClientInterceptor
    // is appended after user interceptors at channel-builder time.
    val allInterceptors = interceptors ++ tokenInterceptor.toList
    val (stub, aStub) =
      if allInterceptors.isEmpty then (baseStub, baseAsyncStub)
      else
        val arr = allInterceptors.toArray
        (baseStub.withInterceptors(arr*), baseAsyncStub.withInterceptors(arr*))

    val client = SparkConnectClient(
      SharedChannel(channel),
      stub,
      aStub,
      sessionId,
      userId,
      GrpcRetryHandler(retryPolicy),
      sanitizedUrl,
      token,
      planCompressionEnabled
    )

    configs.foreach((k, v) => client.setConfig(k, v))
    client

  /** Build a sanitized `sc://` URL from host, port, and ordered params (token excluded).
    *
    * Keys and values are URL-encoded so the output can round-trip through `parseUrl` — values
    * containing `;` or `=` would otherwise corrupt the URL and break `cloneSession` / retry
    * workflows that re-parse `fullUrl`.
    */
  private def buildSanitizedUrl(host: String, port: Int, params: Seq[(String, String)]): String =
    val base = s"sc://$host:$port"
    if params.isEmpty then base
    else
      base + ";" + params.map { (k, v) =>
        s"${UrlEncoding.encode(k)}=${UrlEncoding.encode(v)}"
      }.mkString(";")
