package org.apache.spark.sql

/** Spark-compatible base type for session-bound SQL implicits. */
abstract class SQLImplicits extends Serializable:
  protected def session: SparkSession

  implicit def sparkSession: SparkSession = session

  export _root_.org.apache.spark.sql.implicits.*
