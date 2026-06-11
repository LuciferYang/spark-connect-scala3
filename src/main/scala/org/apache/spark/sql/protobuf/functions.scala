package org.apache.spark.sql.protobuf

import scala.jdk.CollectionConverters.*

import org.apache.spark.connect.proto.Expression
import org.apache.spark.sql.{Column, functions as sqlFunctions}
import org.apache.spark.sql.util.ProtobufUtils

/** Protobuf SQL functions exposed under the Spark-compatible package name. */
object functions:

  def from_protobuf(
      data: Column,
      messageName: String,
      descFilePath: String,
      options: java.util.Map[String, String]
  ): Column =
    from_protobuf(data, messageName, ProtobufUtils.readDescriptorFileContent(descFilePath), options)

  def from_protobuf(
      data: Column,
      messageName: String,
      descFilePath: String,
      options: Map[String, String]
  ): Column =
    from_protobuf(data, messageName, descFilePath, options.asJava)

  def from_protobuf(
      data: Column,
      messageName: String,
      binaryFileDescriptorSet: Array[Byte],
      options: java.util.Map[String, String]
  ): Column =
    callFn(
      "from_protobuf",
      data,
      sqlFunctions.lit(messageName),
      sqlFunctions.lit(binaryFileDescriptorSet),
      optionsMapColumn(options.asScala.toMap)
    )

  def from_protobuf(
      data: Column,
      messageName: String,
      binaryFileDescriptorSet: Array[Byte],
      options: Map[String, String]
  ): Column =
    from_protobuf(data, messageName, binaryFileDescriptorSet, options.asJava)

  def from_protobuf(data: Column, messageName: String, descFilePath: String): Column =
    from_protobuf(data, messageName, ProtobufUtils.readDescriptorFileContent(descFilePath))

  def from_protobuf(
      data: Column,
      messageName: String,
      binaryFileDescriptorSet: Array[Byte]
  ): Column =
    callFn(
      "from_protobuf",
      data,
      sqlFunctions.lit(messageName),
      sqlFunctions.lit(binaryFileDescriptorSet)
    )

  def from_protobuf(data: Column, messageClassName: String): Column =
    callFn("from_protobuf", data, sqlFunctions.lit(messageClassName))

  def from_protobuf(
      data: Column,
      messageClassName: String,
      options: java.util.Map[String, String]
  ): Column =
    callFn(
      "from_protobuf",
      data,
      sqlFunctions.lit(messageClassName),
      optionsMapColumn(options.asScala.toMap)
    )

  def from_protobuf(
      data: Column,
      messageClassName: String,
      options: Map[String, String]
  ): Column =
    from_protobuf(data, messageClassName, options.asJava)

  def to_protobuf(data: Column, messageName: String, descFilePath: String): Column =
    to_protobuf(data, messageName, descFilePath, Map.empty[String, String].asJava)

  def to_protobuf(data: Column, messageName: String, binaryFileDescriptorSet: Array[Byte]): Column =
    callFn(
      "to_protobuf",
      data,
      sqlFunctions.lit(messageName),
      sqlFunctions.lit(binaryFileDescriptorSet)
    )

  def to_protobuf(
      data: Column,
      messageName: String,
      descFilePath: String,
      options: java.util.Map[String, String]
  ): Column =
    to_protobuf(data, messageName, ProtobufUtils.readDescriptorFileContent(descFilePath), options)

  def to_protobuf(
      data: Column,
      messageName: String,
      descFilePath: String,
      options: Map[String, String]
  ): Column =
    to_protobuf(data, messageName, descFilePath, options.asJava)

  def to_protobuf(
      data: Column,
      messageName: String,
      binaryFileDescriptorSet: Array[Byte],
      options: java.util.Map[String, String]
  ): Column =
    callFn(
      "to_protobuf",
      data,
      sqlFunctions.lit(messageName),
      sqlFunctions.lit(binaryFileDescriptorSet),
      optionsMapColumn(options.asScala.toMap)
    )

  def to_protobuf(
      data: Column,
      messageName: String,
      binaryFileDescriptorSet: Array[Byte],
      options: Map[String, String]
  ): Column =
    to_protobuf(data, messageName, binaryFileDescriptorSet, options.asJava)

  def to_protobuf(data: Column, messageClassName: String): Column =
    callFn("to_protobuf", data, sqlFunctions.lit(messageClassName))

  def to_protobuf(
      data: Column,
      messageClassName: String,
      options: java.util.Map[String, String]
  ): Column =
    callFn(
      "to_protobuf",
      data,
      sqlFunctions.lit(messageClassName),
      optionsMapColumn(options.asScala.toMap)
    )

  def to_protobuf(
      data: Column,
      messageClassName: String,
      options: Map[String, String]
  ): Column =
    to_protobuf(data, messageClassName, options.asJava)

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
