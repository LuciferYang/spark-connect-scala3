package org.apache.spark.sql

import org.apache.spark.connect.proto.*

/** Reader for loading streaming DataFrames from external sources.
  *
  * {{{
  *   val df = spark.readStream.format("rate").load()
  *   val df = spark.readStream.format("kafka").option("subscribe", "topic").load()
  * }}}
  */
final class DataStreamReader private[sql] (private val session: SparkSession):
  private var source: String = ""
  private var opts: Map[String, String] = Map.empty
  private var userSchema: Option[String] = None

  def format(fmt: String): DataStreamReader =
    source = fmt
    this

  def option(key: String, value: String): DataStreamReader =
    opts = opts + (key -> value)
    this

  def option(key: String, value: Boolean): DataStreamReader = option(key, value.toString)

  def option(key: String, value: Long): DataStreamReader = option(key, value.toString)

  def option(key: String, value: Double): DataStreamReader = option(key, value.toString)

  def options(m: Map[String, String]): DataStreamReader =
    opts = opts ++ m
    this

  def schema(schemaString: String): DataStreamReader =
    userSchema = Some(schemaString)
    this

  def schema(schema: types.StructType): DataStreamReader =
    this.schema(schema.toDDL)

  def load(): DataFrame = load(Seq.empty)

  def load(path: String): DataFrame = load(Seq(path))

  def load(paths: Seq[String]): DataFrame =
    val dsBuilder = Read.DataSource.newBuilder()
    if source.nonEmpty then dsBuilder.setFormat(source)
    opts.foreach((k, v) => dsBuilder.putOptions(k, v))
    userSchema.foreach(dsBuilder.setSchema)
    paths.foreach(dsBuilder.addPaths)
    DataFrame(
      session,
      Relation.newBuilder()
        .setCommon(RelationCommon.newBuilder().setPlanId(session.nextPlanId()).build())
        .setRead(Read.newBuilder()
          .setIsStreaming(true)
          .setDataSource(dsBuilder.build())
          .build())
        .build()
    )

  def json(path: String): DataFrame = format("json").load(path)

  def csv(path: String): DataFrame = format("csv").load(path)

  def parquet(path: String): DataFrame = format("parquet").load(path)

  def orc(path: String): DataFrame = format("orc").load(path)

  def text(path: String): DataFrame = format("text").load(path)

  def xml(path: String): DataFrame = format("xml").load(path)

  def textFile(path: String): DataFrame =
    text(path).select(Column("value"))

  def table(tableName: String): DataFrame =
    DataFrame(
      session,
      Relation.newBuilder()
        .setCommon(RelationCommon.newBuilder().setPlanId(session.nextPlanId()).build())
        .setRead(Read.newBuilder()
          .setIsStreaming(true)
          .setNamedTable(Read.NamedTable.newBuilder()
            .setUnparsedIdentifier(tableName).build())
          .build())
        .build()
    )
