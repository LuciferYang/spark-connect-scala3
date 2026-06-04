package org.apache.spark.sql.avro

import scala.jdk.CollectionConverters.*

import org.apache.spark.connect.proto.Expression
import org.apache.spark.sql.{Column, functions as sqlFunctions}

/** Avro SQL functions exposed under the Spark-compatible package name. */
object functions:

  def from_avro(data: Column, jsonFormatSchema: String): Column =
    callFn("from_avro", data, sqlFunctions.lit(jsonFormatSchema))

  def from_avro(
      data: Column,
      jsonFormatSchema: String,
      options: java.util.Map[String, String]
  ): Column =
    callFn(
      "from_avro",
      data,
      sqlFunctions.lit(jsonFormatSchema),
      optionsMapColumn(options.asScala.toMap)
    )

  def to_avro(data: Column): Column =
    callFn("to_avro", data)

  def to_avro(data: Column, jsonFormatSchema: String): Column =
    callFn("to_avro", data, sqlFunctions.lit(jsonFormatSchema))

  def schema_of_avro(jsonFormatSchema: String): Column =
    callFn("schema_of_avro", sqlFunctions.lit(jsonFormatSchema))

  def schema_of_avro(jsonFormatSchema: String, options: java.util.Map[String, String]): Column =
    callFn(
      "schema_of_avro",
      sqlFunctions.lit(jsonFormatSchema),
      optionsMapColumn(options.asScala.toMap)
    )

  private def optionsMapColumn(options: Map[String, String]): Column =
    val entries =
      options.iterator.flatMap((k, v) => Seq(sqlFunctions.lit(k), sqlFunctions.lit(v))).toSeq
    if entries.isEmpty then callFn("map")
    else callFn("map", entries*)

  private def callFn(name: String, cols: Column*): Column =
    val builder = Expression.UnresolvedFunction.newBuilder()
      .setFunctionName(name)
      .setIsDistinct(false)
    cols.foreach(c => builder.addArguments(c.expr))
    Column(Expression.newBuilder()
      .setUnresolvedFunction(builder.build())
      .build())
