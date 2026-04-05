package org.apache.spark.sql

import scala.jdk.CollectionConverters.*
import org.apache.spark.connect.proto.*

/** Writer for saving DataFrames using the V2 data source API.
  *
  * {{{
  *   df.writeTo("my_table").using("parquet").create()
  *   df.writeTo("my_table").append()
  *   df.writeTo("my_table").overwrite(col("date") === lit("2024-01-01"))
  * }}}
  */
final class DataFrameWriterV2 private[sql] (table: String, df: DataFrame):

  private val builder = WriteOperationV2.newBuilder()
    .setInput(df.relation)
    .setTableName(table)

  def using(provider: String): DataFrameWriterV2 =
    builder.setProvider(provider)
    this

  def option(key: String, value: String): DataFrameWriterV2 =
    builder.putOptions(key, value)
    this

  def option(key: String, value: Boolean): DataFrameWriterV2 =
    option(key, value.toString)

  def option(key: String, value: Long): DataFrameWriterV2 =
    option(key, value.toString)

  def option(key: String, value: Double): DataFrameWriterV2 =
    option(key, value.toString)

  def options(opts: Map[String, String]): DataFrameWriterV2 =
    builder.putAllOptions(opts.asJava)
    this

  def tableProperty(property: String, value: String): DataFrameWriterV2 =
    builder.putTableProperties(property, value)
    this

  def partitionedBy(column: Column, columns: Column*): DataFrameWriterV2 =
    builder.addAllPartitioningColumns((column +: columns).map(_.expr).asJava)
    this

  def clusterBy(colName: String, colNames: String*): DataFrameWriterV2 =
    (colName +: colNames).foreach(builder.addClusteringColumns)
    this

  def create(): Unit =
    executeWriteOperation(WriteOperationV2.Mode.MODE_CREATE)

  def replace(): Unit =
    executeWriteOperation(WriteOperationV2.Mode.MODE_REPLACE)

  def createOrReplace(): Unit =
    executeWriteOperation(WriteOperationV2.Mode.MODE_CREATE_OR_REPLACE)

  def append(): Unit =
    executeWriteOperation(WriteOperationV2.Mode.MODE_APPEND)

  def overwrite(condition: Column): Unit =
    builder.setOverwriteCondition(condition.expr)
    executeWriteOperation(WriteOperationV2.Mode.MODE_OVERWRITE)

  def overwritePartitions(): Unit =
    executeWriteOperation(WriteOperationV2.Mode.MODE_OVERWRITE_PARTITIONS)

  private def executeWriteOperation(mode: WriteOperationV2.Mode): Unit =
    val command = Command.newBuilder()
      .setWriteOperationV2(builder.setMode(mode).build())
      .build()
    val plan = Plan.newBuilder().setCommand(command).build()
    val responses = df.session.client.execute(plan)
    responses.foreach(_ => ()) // drain iterator
