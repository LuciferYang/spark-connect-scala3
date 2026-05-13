package org.apache.spark.sql

import org.apache.spark.connect.proto.*
import org.apache.spark.sql.internal.OptionBuilder

/** Reader for loading DataFrames from external storage.
  *
  * {{{
  *   val df = spark.read.format("parquet").load("/path")
  *   val df = spark.read.json("/data.json")
  * }}}
  */
final class DataFrameReader private[sql] (private val session: SparkSession)
    extends OptionBuilder[DataFrameReader]:
  private var source: String = "parquet"
  private var opts: Map[String, String] = Map.empty
  private var userSchema: Option[String] = None

  def format(fmt: String): DataFrameReader =
    source = fmt
    this

  def option(key: String, value: String): DataFrameReader =
    opts = opts + (key -> value)
    this

  // Concrete overrides generate bridge methods with the concrete class as the return type,
  // so Java callers can chain reader.option("k", true).schema(...) without being stuck on the
  // OptionBuilder trait return type (which lacks schema() etc.).
  override def option(key: String, value: Boolean): DataFrameReader = super.option(key, value)
  override def option(key: String, value: Long): DataFrameReader = super.option(key, value)
  override def option(key: String, value: Double): DataFrameReader = super.option(key, value)

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

  /** Load JSON files. Equivalent to `format("json").load(paths)`. */
  def json(paths: String*): DataFrame = format("json").load(paths)

  /** Load Parquet files. Equivalent to `format("parquet").load(paths)`. */
  def parquet(paths: String*): DataFrame = format("parquet").load(paths)

  /** Load ORC files. Equivalent to `format("orc").load(paths)`. */
  def orc(paths: String*): DataFrame = format("orc").load(paths)

  /** Load CSV files. Equivalent to `format("csv").load(paths)`. */
  def csv(paths: String*): DataFrame = format("csv").load(paths)

  /** Load text files. Each line becomes a row with a single `value` column. Equivalent to
    * `format("text").load(paths)`.
    */
  def text(paths: String*): DataFrame = format("text").load(paths)

  /** Load XML files. Equivalent to `format("xml").load(paths)`. */
  def xml(paths: String*): DataFrame = format("xml").load(paths)

  /** Load a text file and return its `value` column as a single-column DataFrame — a convenience
    * for the common "treat file as list of strings" pattern.
    */
  def textFile(path: String): DataFrame = format("text").load(path).select(Column("value"))

  /** Varargs variant of `textFile(path)`. */
  def textFile(paths: String*)(using DummyImplicit): DataFrame =
    format("text").load(paths).select(Column("value"))

  /** Load a table from a JDBC data source.
    *
    * {{{
    *   val props = new java.util.Properties()
    *   props.put("user", "dbuser")
    *   val df = spark.read.jdbc("jdbc:h2:mem:test", "mytable", props)
    * }}}
    */
  def jdbc(url: String, table: String, properties: java.util.Properties): DataFrame =
    import scala.jdk.CollectionConverters.*
    val propsMap = properties.asScala.toMap
    format("jdbc")
      .option("url", url)
      .option("dbtable", table)
      .options(propsMap)
      .load()

  /** Load a table from a JDBC data source with range-based partitioning.
    *
    * {{{
    *   val df = spark.read.jdbc("jdbc:h2:mem:test", "mytable",
    *     "id", 0L, 100L, 4, new java.util.Properties())
    * }}}
    */
  def jdbc(
      url: String,
      table: String,
      columnName: String,
      lowerBound: Long,
      upperBound: Long,
      numPartitions: Int,
      connectionProperties: java.util.Properties
  ): DataFrame =
    option("partitionColumn", columnName)
      .option("lowerBound", lowerBound.toString)
      .option("upperBound", upperBound.toString)
      .option("numPartitions", numPartitions.toString)
      .jdbc(url, table, connectionProperties)

  /** Load a table from a JDBC data source with predicate-based partitioning.
    *
    * Each predicate string becomes a WHERE clause for one partition.
    * {{{
    *   val predicates = Array("id < 50", "id >= 50")
    *   val df = spark.read.jdbc("jdbc:h2:mem:test", "mytable", predicates, props)
    * }}}
    */
  def jdbc(
      url: String,
      table: String,
      predicates: Array[String],
      connectionProperties: java.util.Properties
  ): DataFrame =
    import scala.jdk.CollectionConverters.*
    val propsMap = connectionProperties.asScala.toMap
    val dsBuilder = Read.DataSource.newBuilder()
      .setFormat("jdbc")
    (opts ++ propsMap ++ Map("url" -> url, "dbtable" -> table)).foreach((k, v) =>
      dsBuilder.putOptions(k, v)
    )
    predicates.foreach(dsBuilder.addPredicates)
    userSchema.foreach(dsBuilder.setSchema)
    DataFrame(
      session,
      Relation.newBuilder()
        .setCommon(RelationCommon.newBuilder().setPlanId(session.nextPlanId()).build())
        .setRead(Read.newBuilder()
          .setDataSource(dsBuilder.build())
          .build())
        .build()
    )
