package org.apache.spark.sql.expressions

import org.apache.spark.sql.{Column, Window as SqlWindow, functions as sqlFunctions}

/** Spark-compatible package facade for defining DataFrame window specifications. */
object Window:
  @scala.annotation.varargs
  def partitionBy(colName: String, colNames: String*): WindowSpec =
    partitionBy(stringColumns(colName, colNames)*)

  @scala.annotation.varargs
  def partitionBy(cols: Column*): WindowSpec =
    WindowSpec(SqlWindow.partitionBy(cols*))

  @scala.annotation.varargs
  def orderBy(colName: String, colNames: String*): WindowSpec =
    orderBy(stringColumns(colName, colNames)*)

  @scala.annotation.varargs
  def orderBy(cols: Column*): WindowSpec =
    WindowSpec(SqlWindow.orderBy(cols*))

  def unboundedPreceding: Long = SqlWindow.unboundedPreceding

  def unboundedFollowing: Long = SqlWindow.unboundedFollowing

  def currentRow: Long = SqlWindow.currentRow

  def rowsBetween(start: Long, end: Long): WindowSpec =
    WindowSpec(SqlWindow.rowsBetween(start, end))

  def rangeBetween(start: Long, end: Long): WindowSpec =
    WindowSpec(SqlWindow.rangeBetween(start, end))

  private def stringColumns(colName: String, colNames: Seq[String]): Seq[Column] =
    (colName +: colNames).map(sqlFunctions.col)
