package org.apache.spark.sql

import org.apache.spark.connect.proto.*

/** Reader for loading DataFrames from external storage.
  *
  * {{{
  *   val df = spark.read.format("parquet").load("/path")
  *   val df = spark.read.json("/data.json")
  * }}}
  */
final class DataFrameReader private[sql] (private val session: SparkSession):
  private var source: String = "parquet"
  private var opts: Map[String, String] = Map.empty
  private var userSchema: Option[String] = None

  def format(fmt: String): DataFrameReader =
    source = fmt
    this

  def option(key: String, value: String): DataFrameReader =
    opts = opts + (key -> value)
    this

  def option(key: String, value: Boolean): DataFrameReader = option(key, value.toString)
  def option(key: String, value: Long): DataFrameReader = option(key, value.toString)
  def option(key: String, value: Double): DataFrameReader = option(key, value.toString)

  def options(m: Map[String, String]): DataFrameReader =
    opts = opts ++ m
    this

  def schema(schemaString: String): DataFrameReader =
    userSchema = Some(schemaString)
    this

  def schema(schema: types.StructType): DataFrameReader =
    userSchema = Some(schema.toDDL)
    this

  def load(path: String): DataFrame = load(Seq(path))

  def load(): DataFrame = load(Seq.empty)

  def load(paths: Seq[String]): DataFrame =
    val dsBuilder = Read.DataSource.newBuilder()
      .setFormat(source)
    opts.foreach((k, v) => dsBuilder.putOptions(k, v))
    userSchema.foreach(dsBuilder.setSchema)
    paths.foreach(dsBuilder.addPaths)
    DataFrame(
      session,
      Relation.newBuilder()
        .setCommon(RelationCommon.newBuilder().setPlanId(session.nextPlanId()).build())
        .setRead(Read.newBuilder()
          .setDataSource(dsBuilder.build())
          .build())
        .build()
    )

  def table(tableName: String): DataFrame = session.table(tableName)

  def json(paths: String*): DataFrame = format("json").load(paths)
  def parquet(paths: String*): DataFrame = format("parquet").load(paths)
  def orc(paths: String*): DataFrame = format("orc").load(paths)
  def csv(paths: String*): DataFrame = format("csv").load(paths)
  def text(paths: String*): DataFrame = format("text").load(paths)
