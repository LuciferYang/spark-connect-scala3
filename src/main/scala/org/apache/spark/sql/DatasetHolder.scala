package org.apache.spark.sql

/** Container for a Dataset, used for Spark-compatible implicit conversions in Scala. */
abstract class DatasetHolder[T]:
  def toDS(): Dataset[T]

  def toDF(): DataFrame

  def toDF(colNames: String*): DataFrame
