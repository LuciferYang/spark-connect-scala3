package org.apache.spark.sql.util

import java.nio.file.{Files, Paths}

/** Small public utility surface needed by Spark-compatible protobuf SQL functions. */
object ProtobufUtils:
  def readDescriptorFileContent(descFilePath: String): Array[Byte] =
    Files.readAllBytes(Paths.get(descFilePath))
