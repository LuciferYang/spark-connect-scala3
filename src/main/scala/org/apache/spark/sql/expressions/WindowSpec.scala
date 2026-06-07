package org.apache.spark.sql.expressions

import org.apache.spark.sql.{Column, functions as sqlFunctions}

/** Spark-compatible package facade for a window specification. */
final class WindowSpec private[sql] (
    private[sql] val delegate: org.apache.spark.sql.WindowSpec
):
  @scala.annotation.varargs
  def partitionBy(colName: String, colNames: String*): WindowSpec =
    partitionBy(stringColumns(colName, colNames)*)

  @scala.annotation.varargs
  def partitionBy(cols: Column*): WindowSpec =
    WindowSpec(delegate.partitionBy(cols*))

  @scala.annotation.varargs
  def orderBy(colName: String, colNames: String*): WindowSpec =
    orderBy(stringColumns(colName, colNames)*)

  @scala.annotation.varargs
  def orderBy(cols: Column*): WindowSpec =
    WindowSpec(delegate.orderBy(cols*))

  def rowsBetween(start: Long, end: Long): WindowSpec =
    WindowSpec(delegate.rowsBetween(start, end))

  def rangeBetween(start: Long, end: Long): WindowSpec =
    WindowSpec(delegate.rangeBetween(start, end))

  private def stringColumns(colName: String, colNames: Seq[String]): Seq[Column] =
    (colName +: colNames).map(sqlFunctions.col)
