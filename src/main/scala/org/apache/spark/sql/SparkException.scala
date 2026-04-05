package org.apache.spark.sql

/** Lightweight Spark exception for the Scala 3 Connect client.
  *
  * Carries optional `errorClass` and `sqlState` extracted from gRPC error metadata, mirroring the
  * upstream SparkException contract without pulling in the full SparkThrowable hierarchy.
  */
class SparkException(
    message: String,
    cause: Throwable = null,
    val errorClass: Option[String] = None,
    val sqlState: Option[String] = None
) extends Exception(message, cause)
