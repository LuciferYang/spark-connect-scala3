package org.apache.spark.sql

import scala.jdk.CollectionConverters.*
import org.apache.spark.connect.proto.*
import org.apache.spark.sql.internal.OptionBuilder

/** Writer for saving DataFrames using the V2 data source API.
  *
  * The type parameter `T` carries the originating `Dataset[T]` element type so callers can keep
  * typed pipelines like `ds.as[Person].writeTo("t").append()` aligned with upstream Spark's
  * `DataFrameWriterV2[T]` contract. `T` is not used in the writer's runtime behavior — proto
  * construction is type-erased — but it preserves the signature shape so user code migrating from
  * upstream compiles unchanged.
  *
  * {{{
  *   df.writeTo("my_table").using("parquet").create()
  *   df.writeTo("my_table").append()
  *   df.writeTo("my_table").overwrite(col("date") === lit("2024-01-01"))
  * }}}
  */
final class DataFrameWriterV2[T] private[sql] (table: String, df: DataFrame)
    extends OptionBuilder[DataFrameWriterV2[T]]:

  private val builder = WriteOperationV2.newBuilder()
    .setInput(df.relation)
    .setTableName(table)

  def using(provider: String): DataFrameWriterV2[T] =
    builder.setProvider(provider)
    this

  def option(key: String, value: String): DataFrameWriterV2[T] =
    builder.putOptions(key, value)
    this

  // Concrete overrides for Java-interop-friendly return type.
  override def option(key: String, value: Boolean): DataFrameWriterV2[T] =
    super.option(key, value)
  override def option(key: String, value: Long): DataFrameWriterV2[T] =
    super.option(key, value)
  override def option(key: String, value: Double): DataFrameWriterV2[T] =
    super.option(key, value)

  def options(opts: Map[String, String]): DataFrameWriterV2[T] =
    builder.putAllOptions(opts.asJava)
    this

  def tableProperty(property: String, value: String): DataFrameWriterV2[T] =
    builder.putTableProperties(property, value)
    this

  def partitionedBy(column: Column, columns: Column*): DataFrameWriterV2[T] =
    builder.addAllPartitioningColumns((column +: columns).map(_.expr).asJava)
    this

  def clusterBy(colName: String, colNames: String*): DataFrameWriterV2[T] =
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
    df.session.client.executeCommand(command)
