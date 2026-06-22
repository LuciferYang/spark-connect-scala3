package org.apache.spark.sql

import org.apache.spark.connect.proto.{Catalog as _, StorageLevel as _, *}
import org.apache.spark.sql.connect.SessionCleaner
import org.apache.spark.sql.connect.client.{ClassFinder, SparkConnectClient}

import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

/** Entry point for Spark Connect Scala 3 client.
  *
  * Use the Builder to create a session:
  * {{{
  *   val spark = SparkSession.builder()
  *     .remote("sc://localhost:15002")
  *     .build()
  * }}}
  */
final class SparkSession private[sql] (
    private[sql] val client: SparkConnectClient
) extends java.io.Closeable:
  private val planIdCounter = AtomicLong(0L)

  // Registry of Observations keyed by the plan ID of their CollectMetrics node.
  private[sql] val observationRegistry = ConcurrentHashMap[Long, Observation]()

  /** GC-based cleanup of CachedRemoteRelation references on the server. */
  private[sql] lazy val cleaner: SessionCleaner = SessionCleaner(this)

  def sessionId: String = client.sessionId

  def sqlContext: SQLContext = SQLContext(this)

  private[sql] def nextPlanId(): Long = planIdCounter.getAndIncrement()

  // scalastyle:off
  /** Scala 3 imports bound to this session, matching `import spark.implicits.*`. */
  object implicits extends SQLImplicits:
    protected def session: SparkSession = SparkSession.this
  // scalastyle:on

  // ---------------------------------------------------------------------------
  // SQL
  // ---------------------------------------------------------------------------

  def sql(query: String): DataFrame =
    DataFrame(
      this,
      Relation.newBuilder()
        .setCommon(RelationCommon.newBuilder().setPlanId(nextPlanId()).build())
        .setSql(SQL.newBuilder().setQuery(query).build())
        .build()
    )

  /** Execute a SQL query with named parameters.
    *
    * {{{
    *   spark.sql("SELECT :name AS name", Map("name" -> "hello")).show()
    * }}}
    *
    * '''Note''': argument values are converted via `Column.lit(value)`. Supported types are:
    * Boolean, Byte, Short, Int, Long, Float, Double, String, and Column. Unsupported types are
    * silently converted via `.toString` — this matches upstream Spark behavior but may produce
    * unexpected results for complex objects.
    */
  def sql(query: String, args: Map[String, Any]): DataFrame =
    val sqlBuilder = SQL.newBuilder().setQuery(query)
    args.foreach { (name, value) =>
      sqlBuilder.putNamedArguments(name, Column.lit(value).expr)
    }
    DataFrame(
      this,
      Relation.newBuilder()
        .setCommon(RelationCommon.newBuilder().setPlanId(nextPlanId()).build())
        .setSql(sqlBuilder.build())
        .build()
    )

  /** Execute a SQL query with positional parameters (Column expressions).
    *
    * {{{
    *   spark.sql("SELECT ? + ? AS sum", functions.lit(1), functions.lit(2)).show()
    * }}}
    */
  def sql(query: String, args: Column*)(using DummyImplicit): DataFrame =
    val sqlBuilder = SQL.newBuilder().setQuery(query)
    args.foreach(c => sqlBuilder.addPosArguments(c.expr))
    DataFrame(
      this,
      Relation.newBuilder()
        .setCommon(RelationCommon.newBuilder().setPlanId(nextPlanId()).build())
        .setSql(sqlBuilder.build())
        .build()
    )

  /** Execute a SQL query with positional parameters from an Array. */
  def sql(query: String, args: Array[?]): DataFrame =
    val sqlBuilder = SQL.newBuilder().setQuery(query)
    args.foreach(v => sqlBuilder.addPosArguments(Column.lit(v).expr))
    DataFrame(
      this,
      Relation.newBuilder()
        .setCommon(RelationCommon.newBuilder().setPlanId(nextPlanId()).build())
        .setSql(sqlBuilder.build())
        .build()
    )

  /** Java-friendly variant of sql with named parameters using java.util.Map. */
  def sql(query: String, args: java.util.Map[String, Any]): DataFrame =
    import scala.jdk.CollectionConverters.*
    sql(query, args.asScala.toMap)

  // ---------------------------------------------------------------------------
  // Table / Range / Empty
  // ---------------------------------------------------------------------------

  def table(tableName: String): DataFrame = read.table(tableName)

  /** Create a `Dataset[java.lang.Long]` with elements `[0, end)` step 1.
    *
    * Returns typed `Dataset[java.lang.Long]` matching upstream Spark contract — supports typed
    * pipelines like `spark.range(10).map(x => x * 2)` directly.
    */
  def range(end: Long): Dataset[java.lang.Long] = range(0, end, 1, None)

  /** Create a `Dataset[java.lang.Long]` with elements `[start, end)` step 1. */
  def range(start: Long, end: Long): Dataset[java.lang.Long] = range(start, end, 1, None)

  /** Create a `Dataset[java.lang.Long]` with elements `[start, end)` and the given step. */
  def range(start: Long, end: Long, step: Long): Dataset[java.lang.Long] =
    range(start, end, step, None)

  /** Create a `Dataset[java.lang.Long]` with elements `[start, end)`, given step, and partitions.
    */
  def range(start: Long, end: Long, step: Long, numPartitions: Int): Dataset[java.lang.Long] =
    range(start, end, step, Some(numPartitions))

  private def range(
      start: Long,
      end: Long,
      step: Long,
      numPartitions: Option[Int]
  ): Dataset[java.lang.Long] =
    require(step != 0, "step must not be zero")
    val rangeBuilder = Range.newBuilder().setStart(start).setEnd(end).setStep(step)
    numPartitions.foreach(rangeBuilder.setNumPartitions)
    val rel = Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(nextPlanId()).build())
      .setRange(rangeBuilder.build())
      .build()
    Dataset(DataFrame(this, rel), Encoders.LONG)

  def emptyDataset[T: Encoder: scala.reflect.ClassTag]: Dataset[T] =
    val enc = summon[Encoder[T]]
    val rel = Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(nextPlanId()).build())
      .setLocalRelation(LocalRelation.newBuilder().setSchema(enc.schema.toDDL).build())
      .build()
    Dataset(DataFrame(this, rel), enc)

  def emptyDataFrame: DataFrame =
    // The server requires a schema string for LocalRelation when no data is provided.
    // Use JSON format matching upstream: {"type":"struct","fields":[]}
    val emptySchemaJson = """{"type":"struct","fields":[]}"""
    DataFrame(
      this,
      Relation.newBuilder()
        .setCommon(RelationCommon.newBuilder().setPlanId(nextPlanId()).build())
        .setLocalRelation(LocalRelation.newBuilder().setSchema(emptySchemaJson).build())
        .build()
    )

  // ---------------------------------------------------------------------------
  // createDataFrame (Arrow IPC serialization)
  // ---------------------------------------------------------------------------

  def createDataFrame(rows: Seq[Row], schema: types.StructType): DataFrame =
    val arrowData = ArrowSerializer.encodeRows(rows, schema)
    val localRelBuilder = LocalRelation.newBuilder().setSchema(schema.toDDL)
    if arrowData.nonEmpty then
      localRelBuilder.setData(com.google.protobuf.ByteString.copyFrom(arrowData))
    DataFrame(
      this,
      Relation.newBuilder()
        .setCommon(RelationCommon.newBuilder().setPlanId(nextPlanId()).build())
        .setLocalRelation(localRelBuilder.build())
        .build()
    )

  /** Java-friendly variant of createDataFrame using java.util.List. */
  def createDataFrame(rows: java.util.List[Row], schema: types.StructType): DataFrame =
    import scala.jdk.CollectionConverters.*
    createDataFrame(rows.asScala.toSeq, schema)

  /** Create a DataFrame from a `Seq` of case-class / `Product` values, deriving the schema and row
    * encoding at compile time.
    *
    * Mirrors upstream `SparkSession.createDataFrame[A &lt;: Product](data: Seq[A])` so callers can
    * write `spark.createDataFrame(Seq(Person("a", 1)))` without hand-rolling `Row` + `StructType`.
    *
    * The Java-bean variant `createDataFrame(util.List[_], Class[_])` is intentionally not provided
    * — Scala 3 client users have no idiomatic call site for it; Java users should use the upstream
    * Scala 2.13 / Java client.
    */
  inline def createDataFrame[A <: Product](data: Seq[A])(
      using
      scala.deriving.Mirror.ProductOf[A],
      scala.reflect.ClassTag[A]
  ): DataFrame =
    val enc = Encoder.derived[A]
    val rows = data.map(enc.toRow)
    createDataFrame(rows, enc.schema)

  // ---------------------------------------------------------------------------
  // createDataset (typed)
  // ---------------------------------------------------------------------------

  /** Create a Dataset[T] from a local Seq using a compile-time derived Encoder. */
  def createDataset[T: Encoder: scala.reflect.ClassTag](data: Seq[T]): Dataset[T] =
    val enc = summon[Encoder[T]]
    val rows = data.map(enc.toRow)
    val df = createDataFrame(rows, enc.schema)
    Dataset(df, enc)

  /** Java-friendly variant of createDataset using java.util.List. */
  def createDataset[T: Encoder: scala.reflect.ClassTag](data: java.util.List[T]): Dataset[T] =
    import scala.jdk.CollectionConverters.*
    createDataset(data.asScala.toSeq)

  // ---------------------------------------------------------------------------
  // Table-Valued Functions
  // ---------------------------------------------------------------------------

  /** Access table-valued functions (explode, inline, posexplode, etc.). */
  def tvf: TableValuedFunction = TableValuedFunction(this)

  // ---------------------------------------------------------------------------
  // Reader
  // ---------------------------------------------------------------------------

  def read: DataFrameReader = DataFrameReader(this)

  // ---------------------------------------------------------------------------
  // Streaming Reader / Query Manager
  // ---------------------------------------------------------------------------

  def readStream: DataStreamReader = DataStreamReader(this)

  lazy val streams: StreamingQueryManager =
    _streamsInitialised = true
    StreamingQueryManager(this)

  /** Tracks whether [[streams]] was ever accessed so `close()` only shuts down the listener bus
    * when it is actually live — avoids forcing a `StreamingQueryManager` into existence just to
    * tear it down.
    */
  private var _streamsInitialised = false

  // ---------------------------------------------------------------------------
  // Catalog
  // ---------------------------------------------------------------------------

  def catalog: Catalog = Catalog(this)

  // ---------------------------------------------------------------------------
  // Config
  // ---------------------------------------------------------------------------

  def conf: RuntimeConfig = RuntimeConfig(client)

  // ---------------------------------------------------------------------------
  // UDF Registration
  // ---------------------------------------------------------------------------

  /** Access the UDF registration interface. */
  def udf: UDFRegistration = UDFRegistration(client)

  // ---------------------------------------------------------------------------
  // Artifact Management
  // ---------------------------------------------------------------------------

  /** Upload a JAR or class file from the local filesystem. */
  def addArtifact(path: String): Unit = client.artifactManager.addArtifact(path)

  /** Upload in-memory bytes as an artifact with the given target path. */
  def addArtifact(bytes: Array[Byte], target: String): Unit =
    client.artifactManager.addArtifact(bytes, target)

  /** Upload all `.class` files under a directory.
    *
    * @param exclude
    *   optional predicate on relative path; return `true` to skip.
    */
  def addClassDir(base: Path, exclude: Path => Boolean = _ => false): Unit =
    client.artifactManager.addClassDir(base, exclude)

  /** Register a `ClassFinder` for automatic class upload before each execution. */
  def registerClassFinder(finder: ClassFinder): Unit =
    client.artifactManager.registerClassFinder(finder)

  // ---------------------------------------------------------------------------
  // Version
  // ---------------------------------------------------------------------------

  def version: String = client.version()

  // ---------------------------------------------------------------------------
  // Observation Management
  // ---------------------------------------------------------------------------

  private[sql] def registerObservation(planId: Long, obs: Observation): Unit =
    observationRegistry.put(planId, obs)

  private[sql] def processObservedMetrics(
      metrics: Seq[(Long, Row)]
  ): Unit =
    metrics.foreach { (planId, row) =>
      val obs = observationRegistry.get(planId)
      if obs != null then
        obs.setMetrics(row)
        observationRegistry.remove(planId)
    }

  /** Fail every [[Observation]] bound to a CollectMetrics node in `rel`, completing its `get` with
    * the given cause instead of leaving it blocked forever when the observed action fails.
    */
  private[sql] def failObservedMetrics(rel: Relation, cause: Throwable): Unit =
    collectMetricsPlanIds(rel).foreach { planId =>
      val obs = observationRegistry.remove(planId)
      if obs != null then obs.failMetrics(cause)
    }

  /** Collect the plan IDs of every CollectMetrics relation in a relation tree. */
  private def collectMetricsPlanIds(rel: Relation): Set[Long] =
    import scala.jdk.CollectionConverters.*
    val ids = Set.newBuilder[Long]
    def walk(value: Any): Unit =
      if value.isInstanceOf[Relation] then
        val r = value.asInstanceOf[Relation]
        if r.hasCollectMetrics then ids += r.getCommon.getPlanId
        r.getAllFields.values.asScala.foreach(walk)
      else if value.isInstanceOf[com.google.protobuf.Message] then
        value.asInstanceOf[com.google.protobuf.Message].getAllFields.values.asScala.foreach(walk)
      else if value.isInstanceOf[java.util.List[?]] then
        value.asInstanceOf[java.util.List[?]].asScala.foreach(walk)
    walk(rel)
    ids.result()

  // ---------------------------------------------------------------------------
  // Operation Tags
  // ---------------------------------------------------------------------------

  def addTag(tag: String): Unit = client.addTag(tag)
  def removeTag(tag: String): Unit = client.removeTag(tag)
  def getTags(): Set[String] = client.getTags()
  def clearTags(): Unit = client.clearTags()

  // ---------------------------------------------------------------------------
  // Interrupt
  // ---------------------------------------------------------------------------

  def interruptAll(): Seq[String] = client.interruptAll()
  def interruptTag(tag: String): Seq[String] = client.interruptTag(tag)
  def interruptOperation(operationId: String): Seq[String] = client.interruptOperation(operationId)

  // ---------------------------------------------------------------------------
  // Time
  // ---------------------------------------------------------------------------

  /** Executes the given closure and measures wall-clock time, printing it to stdout.
    *
    * Returns the result of the closure.
    */
  def time[T](f: => T): T =
    val start = System.nanoTime()
    val result = f
    val elapsed = (System.nanoTime() - start) / 1e6
    println(f"Time taken: $elapsed%.0f ms")
    result

  /** Execute a block with this session set as active, restoring the previous active session. */
  def withActive[T](block: => T): T =
    val previous = SparkSession.getActiveSession
    SparkSession.setActiveSession(this)
    try block
    finally
      previous match
        case Some(session) => SparkSession.setActiveSession(session)
        case None          => SparkSession.clearActiveSession()

  // ---------------------------------------------------------------------------
  // Stop
  // ---------------------------------------------------------------------------

  def stop(): Unit = close()

  /** Implements `java.io.Closeable`. Releases session resources.
    *
    * Shuts down the streaming query listener bus before closing the gRPC channel so the daemon
    * event-handler thread can observe the closed channel cleanly and no stale listener
    * registrations are left on the server side.
    */
  override def close(): Unit =
    // Close the listener bus only if `streams` was already materialised — accessing the lazy val
    // here would create a StreamingQueryManager just to immediately tear it down.
    if _streamsInitialised then streams.streamingQueryListenerBus.close()
    client.close()
    if SparkSession.getActiveSession.contains(this) then SparkSession.clearActiveSession()
    if SparkSession.getDefaultSession.contains(this) then SparkSession.clearDefaultSession()

  // ---------------------------------------------------------------------------
  // New Session
  // ---------------------------------------------------------------------------

  /** Create a new session connected to the same server but with independent state.
    *
    * The new session has its own session ID and does not share temporary views, UDFs, or SQL
    * configurations with this session.
    */
  def newSession(): SparkSession = SparkSession(client.newClient())

  /** Clone this session, preserving all SQL configuration, temporary views, and UDFs.
    *
    * Uses the server-side CloneSession RPC to create a copy of the current session state.
    */
  def cloneSession(): SparkSession = SparkSession(client.cloneSession())

  // ---------------------------------------------------------------------------
  // Execute External Command (DeveloperApi)
  // ---------------------------------------------------------------------------

  /** Execute an external command via the server.
    *
    * This is a DeveloperApi for running commands through an external runner (e.g., shell, Python).
    */
  def executeCommand(
      runner: String,
      command: String,
      options: Map[String, String] = Map.empty
  ): DataFrame =
    val cmdBuilder = ExecuteExternalCommand.newBuilder()
      .setRunner(runner).setCommand(command)
    options.foreach((k, v) => cmdBuilder.putOptions(k, v))
    val cmd = Command.newBuilder().setExecuteExternalCommand(cmdBuilder.build()).build()
    val responses = client.executeCommandWithResponses(cmd)
    // Look for a SqlCommandResult that has a relation
    val relation = responses
      .find(_.hasSqlCommandResult)
      .filter(_.getSqlCommandResult.hasRelation)
      .map(_.getSqlCommandResult.getRelation)
    relation match
      case Some(rel) => DataFrame(this, rel)
      case None      => emptyDataFrame

  // ---------------------------------------------------------------------------
  // Unsupported Operations (compatibility stubs)
  // ---------------------------------------------------------------------------

  /** Not supported in Spark Connect. */
  def sparkContext: Nothing =
    throw UnsupportedOperationException("sparkContext is not supported in Spark Connect")

  /** Not supported in Spark Connect. */
  def sharedState: Nothing =
    throw UnsupportedOperationException("sharedState is not supported in Spark Connect")

  /** Not supported in Spark Connect. */
  def sessionState: Nothing =
    throw UnsupportedOperationException("sessionState is not supported in Spark Connect")

  /** Not supported in Spark Connect. */
  def listenerManager: Nothing =
    throw UnsupportedOperationException("listenerManager is not supported in Spark Connect")

  /** Not supported in Spark Connect. */
  def experimental: Nothing =
    throw UnsupportedOperationException("experimental is not supported in Spark Connect")

  /** Not supported in Spark Connect. */
  def baseRelationToDataFrame: Nothing =
    throw UnsupportedOperationException(
      "baseRelationToDataFrame is not supported in Spark Connect"
    )

object SparkSession:
  private val activeSession = InheritableThreadLocal[SparkSession]()
  private val defaultSession = AtomicReference[SparkSession]()

  /** Default Spark Connect server URL when no explicit `.remote(...)` is supplied. */
  private[sql] val DefaultRemoteUrl: String = "sc://localhost:15002"

  def builder(): Builder = Builder()

  /** Return the active SparkSession for the current thread. */
  def getActiveSession: Option[SparkSession] = Option(activeSession.get)

  /** Return the default SparkSession. */
  def getDefaultSession: Option[SparkSession] = Option(defaultSession.get)

  /** Return the active SparkSession, or the default one, or throw. */
  def active: SparkSession =
    getActiveSession.orElse(getDefaultSession).getOrElse(
      throw IllegalStateException("No active or default SparkSession found")
    )

  /** Set the active SparkSession for the current thread. */
  def setActiveSession(session: SparkSession): Unit = activeSession.set(session)

  /** Clear the active SparkSession for the current thread. */
  def clearActiveSession(): Unit = activeSession.remove()

  /** Set the default SparkSession. */
  def setDefaultSession(session: SparkSession): Unit = defaultSession.set(session)

  /** Clear the default SparkSession. */
  def clearDefaultSession(): Unit = defaultSession.set(null)

  final class Builder:
    private var url: String = SparkSession.DefaultRemoteUrl
    private var configs: Map[String, String] = Map.empty
    private var interceptors: List[io.grpc.ClientInterceptor] = List.empty
    private var maxInboundMessageSizeBytes: Option[Int] = None

    def remote(connectionString: String): Builder =
      url = connectionString
      this

    /** Override the maximum inbound gRPC message size, in bytes (default 128 MB).
      *
      * Raise it for very large `collect()` results, or lower it to bound the per-message memory
      * spike in constrained containers.
      */
    def maxInboundMessageSize(bytes: Int): Builder =
      maxInboundMessageSizeBytes = Some(bytes)
      this

    /** Add a gRPC `ClientInterceptor` to be used during channel creation.
      *
      * Note: interceptors added last are executed first by gRPC.
      */
    def interceptor(interceptor: io.grpc.ClientInterceptor): Builder =
      interceptors = interceptors :+ interceptor
      this

    def config(key: String, value: String): Builder =
      configs = configs + (key -> value)
      this

    def config(key: String, value: Boolean): Builder = config(key, value.toString)
    def config(key: String, value: Long): Builder = config(key, value.toString)
    def config(key: String, value: Double): Builder = config(key, value.toString)

    def config(map: Map[String, Any]): Builder =
      map.foreach((k, v) => config(k, v.toString))
      this

    def config(map: java.util.Map[String, Any]): Builder =
      import scala.jdk.CollectionConverters.*
      config(map.asScala.toMap)

    /** Set the application name config for API compatibility with upstream Spark. */
    def appName(name: String): Builder = config("spark.app.name", name)

    /** Set the classic Spark master config for API compatibility; use `remote` for Connect. */
    def master(master: String): Builder = config("spark.master", master)

    /** Set the Hive catalog implementation config for API compatibility. */
    def enableHiveSupport(): Builder = config("spark.sql.catalogImplementation", "hive")

    def build(): SparkSession =
      require(
        url != null && url.startsWith("sc://"),
        s"Invalid Spark Connect URL: '$url'. URL must start with 'sc://'."
      )
      val client = SparkConnectClient.create(url, configs = configs, interceptors = interceptors)
      val session = SparkSession(client)
      if defaultSession.get() == null then defaultSession.compareAndSet(null, session)
      activeSession.set(session)
      session

    /** Create a new session — alias for `build()`, matching the official API. */
    def create(): SparkSession = build()

    /** Return an existing active/default session, or create a new one.
      *
      * '''Note''': if an existing session is returned, any `.config(k, v)` calls accumulated on
      * this Builder are silently discarded — they are NOT applied to the returned session. This
      * matches upstream Spark Connect behavior. If you need to ensure configs are applied, use
      * `build()` to always create a fresh session.
      */
    def getOrCreate(): SparkSession =
      val existing = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
      existing.getOrElse(build())

/** Thin wrapper around client config get/set. */
final class RuntimeConfig private[sql] (private val client: SparkConnectClient):
  /** Look up a config value by key.
    *
    * @throws java.util.NoSuchElementException
    *   if the key is unset and has no default.
    */
  @throws[java.util.NoSuchElementException]
  def get(key: String): String =
    getOption(key).getOrElse(
      throw java.util.NoSuchElementException(s"Spark config not set: $key")
    )

  def get(key: String, default: String): String = getOption(key).getOrElse(default)
  def getOption(key: String): Option[String] = client.getConfigOption(key)
  def getAll: Map[String, String] = client.getAllConfig()
  def set(key: String, value: String): Unit = client.setConfig(key, value)
  def set(key: String, value: Boolean): Unit = set(key, value.toString)
  def set(key: String, value: Long): Unit = set(key, value.toString)
  def unset(key: String): Unit = client.unsetConfig(key)
  def isModifiable(key: String): Boolean = client.isModifiableConfig(key)
