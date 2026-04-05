package org.apache.spark.sql

import org.apache.spark.connect.proto.{Catalog as _, StorageLevel as _, *}
import org.apache.spark.sql.connect.SessionCleaner
import org.apache.spark.sql.connect.client.{ClassFinder, SparkConnectClient}

import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

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
):
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

  def emptyDataFrame: DataFrame =
    DataFrame(
      this,
      Relation.newBuilder()
        .setCommon(RelationCommon.newBuilder().setPlanId(nextPlanId()).build())
        .setLocalRelation(LocalRelation.getDefaultInstance)
        .build()
    )

  // ---------------------------------------------------------------------------
  // createDataFrame (Arrow IPC serialization)
  // ---------------------------------------------------------------------------

  def createDataFrame(rows: Seq[Row], schema: types.StructType): DataFrame =
    val arrowData = ArrowSerializer.encodeRows(rows, schema)
    val schemaStr = schema.fields.map(f => s"${f.name} ${f.dataType.simpleString}").mkString(", ")
    DataFrame(
      this,
      Relation.newBuilder()
        .setCommon(RelationCommon.newBuilder().setPlanId(nextPlanId()).build())
        .setLocalRelation(LocalRelation.newBuilder()
          .setData(com.google.protobuf.ByteString.copyFrom(arrowData))
          .setSchema(schemaStr)
          .build())
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

  /** Upload all `.class` files under a directory. */
  def addClassDir(base: Path): Unit = client.artifactManager.addClassDir(base)

  /** Register a [[ClassFinder]] for automatic class upload before each execution. */
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
  // Stop
  // ---------------------------------------------------------------------------

  def stop(): Unit = client.close()

  // ---------------------------------------------------------------------------
  // New Session
  // ---------------------------------------------------------------------------

  /** Create a new session connected to the same server but with independent state.
    *
    * The new session has its own session ID and does not share temporary views, UDFs, or SQL
    * configurations with this session.
    */
  def newSession(): SparkSession = SparkSession(client.newClient())

object SparkSession:
  def builder(): Builder = Builder()

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
      SparkSession(client)

/** Thin wrapper around client config get/set. */
final class RuntimeConfig private[sql] (private val client: SparkConnectClient):
  def get(key: String): String = client.getConfig(key)
  def set(key: String, value: String): Unit = client.setConfig(key, value)
