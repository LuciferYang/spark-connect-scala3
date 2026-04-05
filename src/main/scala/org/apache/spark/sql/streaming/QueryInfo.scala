package org.apache.spark.sql.streaming

import java.util.UUID

/** Information about the currently running streaming query, available to StatefulProcessor. */
trait QueryInfo extends Serializable:
  def getQueryId(): UUID
  def getRunId(): UUID
  def getBatchId(): Long
