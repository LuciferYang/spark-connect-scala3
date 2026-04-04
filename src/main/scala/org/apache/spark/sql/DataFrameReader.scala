package org.apache.spark.sql

import org.apache.spark.connect.proto.relations.*

/**
 * Reader for loading DataFrames from external storage.
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

  def options(m: Map[String, String]): DataFrameReader =
    opts = opts ++ m
    this

  def schema(schemaString: String): DataFrameReader =
    userSchema = Some(schemaString)
    this

  def load(path: String): DataFrame = load(Seq(path))

  def load(): DataFrame = load(Seq.empty)

  def load(paths: Seq[String]): DataFrame =
    val dataSource = Read.DataSource(
      format = Some(source),
      options = opts,
      schema = userSchema,
      paths = paths
    )
    DataFrame(session, Relation(
      common = Some(RelationCommon(planId = Some(session.nextPlanId()))),
      relType = Relation.RelType.Read(Read(
        readType = Read.ReadType.DataSource(dataSource)
      ))
    ))

  def table(tableName: String): DataFrame = session.table(tableName)

  def json(path: String): DataFrame = format("json").load(path)
  def parquet(path: String): DataFrame = format("parquet").load(path)
  def orc(path: String): DataFrame = format("orc").load(path)
  def csv(path: String): DataFrame = format("csv").load(path)
  def text(path: String): DataFrame = format("text").load(path)
