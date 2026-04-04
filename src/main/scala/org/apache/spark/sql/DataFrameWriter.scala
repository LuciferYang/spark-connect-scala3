package org.apache.spark.sql

import org.apache.spark.connect.proto.*

/**
 * Writer for saving DataFrames to external storage.
 *
 * {{{
 *   df.write.format("parquet").mode("overwrite").save("/path")
 *   df.write.saveAsTable("my_table")
 * }}}
 */
final class DataFrameWriter private[sql] (private val df: DataFrame):
  private var source: String = "parquet"
  private var saveMode: String = "error"
  private var opts: Map[String, String] = Map.empty
  private var partitionCols: Seq[String] = Seq.empty
  private var bucketColNames: Seq[String] = Seq.empty
  private var sortColNames: Seq[String] = Seq.empty
  private var numBuckets: Int = 0

  def format(fmt: String): DataFrameWriter =
    source = fmt
    this

  def mode(m: String): DataFrameWriter =
    saveMode = m
    this

  def option(key: String, value: String): DataFrameWriter =
    opts = opts + (key -> value)
    this

  def options(m: Map[String, String]): DataFrameWriter =
    opts = opts ++ m
    this

  def partitionBy(colNames: String*): DataFrameWriter =
    partitionCols = colNames.toSeq
    this

  def bucketBy(numBuckets: Int, colName: String, colNames: String*): DataFrameWriter =
    this.numBuckets = numBuckets
    bucketColNames = colName +: colNames.toSeq
    this

  def sortBy(colName: String, colNames: String*): DataFrameWriter =
    sortColNames = colName +: colNames.toSeq
    this

  def save(path: String): Unit =
    val writeBuilder = buildWriteOp()
    writeBuilder.setPath(path)
    executeCommand(Command.newBuilder()
      .setWriteOperation(writeBuilder.build())
      .build())

  def save(): Unit =
    executeCommand(Command.newBuilder()
      .setWriteOperation(buildWriteOp().build())
      .build())

  def saveAsTable(tableName: String): Unit =
    val writeBuilder = buildWriteOp()
    writeBuilder.setTable(
      WriteOperation.SaveTable.newBuilder()
        .setTableName(tableName)
        .build())
    executeCommand(Command.newBuilder()
      .setWriteOperation(writeBuilder.build())
      .build())

  def insertInto(tableName: String): Unit =
    mode("append").saveAsTable(tableName)

  def json(path: String): Unit = format("json").save(path)
  def parquet(path: String): Unit = format("parquet").save(path)
  def orc(path: String): Unit = format("orc").save(path)
  def csv(path: String): Unit = format("csv").save(path)
  def text(path: String): Unit = format("text").save(path)

  private def buildWriteOp(): WriteOperation.Builder =
    val builder = WriteOperation.newBuilder()
      .setInput(df.relation)
      .setSource(source)
      .setMode(toProtoMode(saveMode))
    opts.foreach { (k, v) => builder.putOptions(k, v) }
    sortColNames.foreach(builder.addSortColumnNames)
    partitionCols.foreach(builder.addPartitioningColumns)
    if numBuckets > 0 then
      val bucketBuilder = WriteOperation.BucketBy.newBuilder()
        .setNumBuckets(numBuckets)
      bucketColNames.foreach(bucketBuilder.addBucketColumnNames)
      builder.setBucketBy(bucketBuilder.build())
    builder

  private def executeCommand(command: Command): Unit =
    val plan = Plan.newBuilder().setCommand(command).build()
    val responses = df.session.client.execute(plan)
    responses.foreach(_ => ()) // drain iterator

  private def toProtoMode(mode: String): WriteOperation.SaveMode =
    mode.toLowerCase match
      case "overwrite"     => WriteOperation.SaveMode.SAVE_MODE_OVERWRITE
      case "append"        => WriteOperation.SaveMode.SAVE_MODE_APPEND
      case "ignore"        => WriteOperation.SaveMode.SAVE_MODE_IGNORE
      case "error" | "errorifexists" => WriteOperation.SaveMode.SAVE_MODE_ERROR_IF_EXISTS
      case _               => WriteOperation.SaveMode.SAVE_MODE_ERROR_IF_EXISTS
