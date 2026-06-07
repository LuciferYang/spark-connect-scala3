package org.apache.spark.sql

import java.util.Properties

import scala.jdk.CollectionConverters.*
import scala.reflect.ClassTag

import org.apache.spark.sql.types.StructType

/** Backward-compatible SQLContext facade backed by a Spark Connect SparkSession. */
final class SQLContext(val sparkSession: SparkSession) extends Serializable:

  def sparkContext: Nothing =
    throw UnsupportedOperationException("sparkContext is not supported in Spark Connect")

  def newSession(): SQLContext = SQLContext(sparkSession.newSession())

  def setConf(props: Properties): Unit =
    props.asScala.foreach { (key, value) =>
      sparkSession.conf.set(key, String.valueOf(value))
    }

  def setConf(key: String, value: String): Unit =
    sparkSession.conf.set(key, value)

  def getConf(key: String): String = sparkSession.conf.get(key)

  def getConf(key: String, defaultValue: String): String =
    sparkSession.conf.get(key, defaultValue)

  def getAllConfs: Map[String, String] = sparkSession.conf.getAll

  def emptyDataFrame: DataFrame = sparkSession.emptyDataFrame

  def udf: UDFRegistration = sparkSession.udf

  object implicits extends SQLImplicits:
    protected def session: SparkSession = SQLContext.this.sparkSession

  def isCached(tableName: String): Boolean = sparkSession.catalog.isCached(tableName)

  def cacheTable(tableName: String): Unit = sparkSession.catalog.cacheTable(tableName)

  def uncacheTable(tableName: String): Unit = sparkSession.catalog.uncacheTable(tableName)

  def clearCache(): Unit = sparkSession.catalog.clearCache()

  def createDataFrame(rows: Seq[Row], schema: StructType): DataFrame =
    sparkSession.createDataFrame(rows, schema)

  def createDataFrame(rows: java.util.List[Row], schema: StructType): DataFrame =
    sparkSession.createDataFrame(rows, schema)

  inline def createDataFrame[A <: Product](data: Seq[A])(using
      scala.deriving.Mirror.ProductOf[A],
      ClassTag[A]
  ): DataFrame =
    sparkSession.createDataFrame(data)

  def createDataset[T: Encoder: ClassTag](data: Seq[T]): Dataset[T] =
    sparkSession.createDataset(data)

  def createDataset[T: Encoder: ClassTag](data: java.util.List[T]): Dataset[T] =
    sparkSession.createDataset(data)

  def read: DataFrameReader = sparkSession.read

  def readStream: DataStreamReader = sparkSession.readStream

  def dropTempTable(tableName: String): Unit =
    sparkSession.catalog.dropTempView(tableName)

  def range(end: Long): DataFrame = sparkSession.range(end).toDF()

  def range(start: Long, end: Long): DataFrame =
    sparkSession.range(start, end).toDF()

  def range(start: Long, end: Long, step: Long): DataFrame =
    sparkSession.range(start, end, step).toDF()

  def range(start: Long, end: Long, step: Long, numPartitions: Int): DataFrame =
    sparkSession.range(start, end, step, numPartitions).toDF()

  def sql(sqlText: String): DataFrame = sparkSession.sql(sqlText)

  def table(tableName: String): DataFrame = sparkSession.table(tableName)

  def streams: StreamingQueryManager = sparkSession.streams

object SQLContext:
  def getOrCreate(sparkSession: SparkSession): SQLContext = SQLContext(sparkSession)
