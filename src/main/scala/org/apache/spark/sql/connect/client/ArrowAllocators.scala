package org.apache.spark.sql.connect.client

import scala.util.Try

/** Resolves the Arrow allocator reservation cap, in bytes.
  *
  * Defaults to 256 GB. Override with the `spark.connect.scala3.arrow.maxAllocatorBytes` system
  * property (a positive Long); invalid or non-positive values fall back to the default. The cap is
  * a ceiling on what the off-heap allocator may request, not a commitment.
  */
private[sql] object ArrowAllocators:

  val DefaultMaxBytes: Long = 256L * 1024 * 1024 * 1024

  val PropertyKey: String = "spark.connect.scala3.arrow.maxAllocatorBytes"

  /** Pure resolution from a raw property value (separated from system state for testability). */
  private[client] def resolve(raw: Option[String]): Long =
    raw.flatMap(s => Try(s.trim.toLong).toOption).filter(_ > 0).getOrElse(DefaultMaxBytes)

  /** The configured cap, read from the system property (falling back to the default). */
  def maxAllocatorBytes: Long = resolve(Option(System.getProperty(PropertyKey)))
