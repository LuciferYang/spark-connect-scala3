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

  private[sql] def nextPlanId(): Long = planIdCounter.getAndIncrement()

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

  // ---------------------------------------------------------------------------
  // Table / Range / Empty
  // ---------------------------------------------------------------------------

  def table(tableName: String): DataFrame =
    DataFrame(
      this,
      Relation.newBuilder()
        .setCommon(RelationCommon.newBuilder().setPlanId(nextPlanId()).build())
        .setRead(Read.newBuilder()
          .setNamedTable(Read.NamedTable.newBuilder()
            .setUnparsedIdentifier(tableName).build())
          .build())
        .build()
    )

  def range(end: Long): DataFrame = range(0, end, 1)

  def range(start: Long, end: Long, step: Long = 1): DataFrame =
    DataFrame(
      this,
      Relation.newBuilder()
        .setCommon(RelationCommon.newBuilder().setPlanId(nextPlanId()).build())
        .setRange(Range.newBuilder()
          .setStart(start).setEnd(end).setStep(step).build())
        .build()
    )

  def range(start: Long, end: Long, step: Long, numPartitions: Int): DataFrame =
    DataFrame(
      this,
      Relation.newBuilder()
        .setCommon(RelationCommon.newBuilder().setPlanId(nextPlanId()).build())
        .setRange(Range.newBuilder()
          .setStart(start).setEnd(end).setStep(step).setNumPartitions(numPartitions).build())
        .build()
    )

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

  // ---------------------------------------------------------------------------
  // createDataset (typed)
  // ---------------------------------------------------------------------------

  /** Create a Dataset[T] from a local Seq using a compile-time derived Encoder. */
  def createDataset[T: Encoder: scala.reflect.ClassTag](data: Seq[T]): Dataset[T] =
    val enc = summon[Encoder[T]]
    val rows = data.map(enc.toRow)
    val df = createDataFrame(rows, enc.schema)
    Dataset(df, enc)

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

  lazy val streams: StreamingQueryManager = StreamingQueryManager(this)

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
    * @param exclude optional predicate on relative path; return `true` to skip.
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
      if obs != null then obs.setMetrics(row)
    }

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
  // Stop
  // ---------------------------------------------------------------------------

  def stop(): Unit = close()

  /** Implements `java.io.Closeable`. Releases session resources. */
  override def close(): Unit =
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

object SparkSession:
  private val activeSession = InheritableThreadLocal[SparkSession]()
  private val defaultSession = AtomicReference[SparkSession]()

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
    private var url: String = "sc://localhost:15002"
    private var configs: Map[String, String] = Map.empty

    def remote(connectionString: String): Builder =
      url = connectionString
      this

    def config(key: String, value: String): Builder =
      configs = configs + (key -> value)
      this

    def build(): SparkSession =
      val client = SparkConnectClient.create(url, configs = configs)
      val session = SparkSession(client)
      if defaultSession.get() == null then defaultSession.compareAndSet(null, session)
      session

    /** Create a new session — alias for `build()`, matching the official API. */
    def create(): SparkSession = build()

    /** Return an existing active/default session, or create a new one. */
    def getOrCreate(): SparkSession =
      val existing = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
      existing.getOrElse(build())

/** Thin wrapper around client config get/set. */
final class RuntimeConfig private[sql] (private val client: SparkConnectClient):
  def get(key: String): String = client.getConfig(key)
  def get(key: String, default: String): String = getOption(key).getOrElse(default)
  def getOption(key: String): Option[String] = client.getConfigOption(key)
  def getAll: Map[String, String] = client.getAllConfig()
  def set(key: String, value: String): Unit = client.setConfig(key, value)
  def set(key: String, value: Boolean): Unit = set(key, value.toString)
  def set(key: String, value: Long): Unit = set(key, value.toString)
  def unset(key: String): Unit = client.unsetConfig(key)
  def isModifiable(key: String): Boolean = client.isModifiableConfig(key)
