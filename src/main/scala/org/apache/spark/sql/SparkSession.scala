package org.apache.spark.sql

import org.apache.spark.connect.proto.base.*
import org.apache.spark.connect.proto.relations.*
import org.apache.spark.sql.connect.client.SparkConnectClient

import java.util.concurrent.atomic.AtomicLong

/**
 * Entry point for Spark Connect Scala 3 client.
 *
 * Use the Builder to create a session:
 * {{{
 *   val spark = SparkSession.builder()
 *     .remote("sc://localhost:15002")
 *     .build()
 * }}}
 */
final class SparkSession private (
    private[sql] val client: SparkConnectClient
):
  private val planIdCounter = AtomicLong(0L)

  def sessionId: String = client.sessionId

  private[sql] def nextPlanId(): Long = planIdCounter.getAndIncrement()

  // ---------------------------------------------------------------------------
  // SQL
  // ---------------------------------------------------------------------------

  def sql(query: String): DataFrame =
    DataFrame(this, Relation(
      common = Some(RelationCommon(planId = Some(nextPlanId()))),
      relType = Relation.RelType.Sql(SQL(query = query))
    ))

  // ---------------------------------------------------------------------------
  // Table / Range / Empty
  // ---------------------------------------------------------------------------

  def table(tableName: String): DataFrame =
    DataFrame(this, Relation(
      common = Some(RelationCommon(planId = Some(nextPlanId()))),
      relType = Relation.RelType.Read(Read(
        readType = Read.ReadType.NamedTable(
          Read.NamedTable(unparsedIdentifier = tableName)
        )
      ))
    ))

  def range(end: Long): DataFrame = range(0, end, 1)

  def range(start: Long, end: Long, step: Long = 1): DataFrame =
    DataFrame(this, Relation(
      common = Some(RelationCommon(planId = Some(nextPlanId()))),
      relType = Relation.RelType.Range(
        Range(start = Some(start), end = end, step = step)
      )
    ))

  def emptyDataFrame: DataFrame =
    DataFrame(this, Relation(
      common = Some(RelationCommon(planId = Some(nextPlanId()))),
      relType = Relation.RelType.LocalRelation(LocalRelation())
    ))

  // ---------------------------------------------------------------------------
  // createDataFrame (Arrow IPC serialization)
  // ---------------------------------------------------------------------------

  def createDataFrame(rows: Seq[Row], schema: types.StructType): DataFrame =
    val arrowData = ArrowSerializer.encodeRows(rows, schema)
    val schemaStr = schema.fields.map(f => s"${f.name} ${f.dataType.simpleString}").mkString(", ")
    DataFrame(this, Relation(
      common = Some(RelationCommon(planId = Some(nextPlanId()))),
      relType = Relation.RelType.LocalRelation(
        LocalRelation(
          data = Some(com.google.protobuf.ByteString.copyFrom(arrowData)),
          schema = Some(schemaStr)
        )
      )
    ))

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
  // Reader
  // ---------------------------------------------------------------------------

  def read: DataFrameReader = DataFrameReader(this)

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
  // Version
  // ---------------------------------------------------------------------------

  def version: String = client.version()

  // ---------------------------------------------------------------------------
  // Stop
  // ---------------------------------------------------------------------------

  def stop(): Unit = client.close()

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
