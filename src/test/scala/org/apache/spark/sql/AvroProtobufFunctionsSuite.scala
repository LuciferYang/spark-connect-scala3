package org.apache.spark.sql

import java.nio.file.Files

import scala.jdk.CollectionConverters.*

import org.apache.spark.sql.avro.functions as avroFunctions
import org.apache.spark.sql.protobuf.functions as protobufFunctions
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class AvroProtobufFunctionsSuite extends AnyFunSuite with Matchers:

  private def unresolvedFunction(column: Column) =
    column.expr.hasUnresolvedFunction shouldBe true
    column.expr.getUnresolvedFunction

  private def assertFn(column: Column, expectedName: String, expectedArgCount: Int): Unit =
    val fn = unresolvedFunction(column)
    fn.getFunctionName shouldBe expectedName
    fn.getArgumentsCount shouldBe expectedArgCount

  private def optionsArgument(column: Column) =
    val fn = unresolvedFunction(column)
    fn.getArguments(fn.getArgumentsCount - 1).getUnresolvedFunction

  test("avro functions build expected unresolved functions") {
    val schema = """{"type":"int","name":"id"}"""

    assertFn(avroFunctions.from_avro(Column("bytes"), schema), "from_avro", 2)
    assertFn(avroFunctions.to_avro(Column("value")), "to_avro", 1)
    assertFn(avroFunctions.to_avro(Column("value"), schema), "to_avro", 2)
    assertFn(avroFunctions.schema_of_avro(schema), "schema_of_avro", 1)
  }

  test("avro functions encode java options as a map argument") {
    val schema = """{"type":"string","name":"name"}"""
    val options = Map("mode" -> "FAILFAST", "compression" -> "zstandard").asJava

    val fromAvro = avroFunctions.from_avro(Column("bytes"), schema, options)
    assertFn(fromAvro, "from_avro", 3)
    val fromOptions = optionsArgument(fromAvro)
    fromOptions.getFunctionName shouldBe "map"
    fromOptions.getArgumentsCount shouldBe 4

    val schemaOfAvro = avroFunctions.schema_of_avro(schema, options)
    assertFn(schemaOfAvro, "schema_of_avro", 2)
    optionsArgument(schemaOfAvro).getFunctionName shouldBe "map"
  }

  test("protobuf functions build expected unresolved functions") {
    val descriptor = Array[Byte](1, 2, 3)

    assertFn(
      protobufFunctions.from_protobuf(Column("bytes"), "StorageLevel", descriptor),
      "from_protobuf",
      3
    )
    assertFn(
      protobufFunctions.from_protobuf(Column("bytes"), "com.example.StorageLevel"),
      "from_protobuf",
      2
    )
    assertFn(
      protobufFunctions.to_protobuf(Column("value"), "StorageLevel", descriptor),
      "to_protobuf",
      3
    )
    assertFn(
      protobufFunctions.to_protobuf(Column("value"), "com.example.StorageLevel"),
      "to_protobuf",
      2
    )
  }

  test("protobuf functions encode options as a map argument") {
    val options = Map("recursive.fields.max.depth" -> "2").asJava
    val descriptor = Array[Byte](1, 2, 3)

    val fromDescriptor = protobufFunctions.from_protobuf(
      Column("bytes"),
      "StorageLevel",
      descriptor,
      options
    )
    assertFn(fromDescriptor, "from_protobuf", 4)
    optionsArgument(fromDescriptor).getFunctionName shouldBe "map"

    val fromClass = protobufFunctions.from_protobuf(
      Column("bytes"),
      "com.example.StorageLevel",
      options
    )
    assertFn(fromClass, "from_protobuf", 3)
    optionsArgument(fromClass).getFunctionName shouldBe "map"

    val toDescriptor = protobufFunctions.to_protobuf(
      Column("value"),
      "StorageLevel",
      descriptor,
      options
    )
    assertFn(toDescriptor, "to_protobuf", 4)
    optionsArgument(toDescriptor).getFunctionName shouldBe "map"

    val toClass = protobufFunctions.to_protobuf(
      Column("value"),
      "com.example.StorageLevel",
      options
    )
    assertFn(toClass, "to_protobuf", 3)
    optionsArgument(toClass).getFunctionName shouldBe "map"
  }

  test("protobuf descriptor path overloads read file content as binary literal") {
    val descriptor = Array[Byte](9, 8, 7)
    val path = Files.createTempFile("sc3-protobuf", ".desc")
    try
      Files.write(path, descriptor)

      val fromColumn =
        protobufFunctions.from_protobuf(Column("bytes"), "StorageLevel", path.toString)
      val fromDescriptorArg = unresolvedFunction(fromColumn).getArguments(2).getLiteral
      fromDescriptorArg.getBinary.toByteArray shouldBe descriptor

      val toColumn = protobufFunctions.to_protobuf(Column("value"), "StorageLevel", path.toString)
      val toDescriptorArg = unresolvedFunction(toColumn).getArguments(2).getLiteral
      toDescriptorArg.getBinary.toByteArray shouldBe descriptor
    finally Files.deleteIfExists(path)
  }

  test("Column.lit preserves byte arrays as binary literals") {
    val bytes = Array[Byte](1, 2, 3)
    val literal = Column.lit(bytes).expr.getLiteral

    literal.hasBinary shouldBe true
    literal.getBinary.toByteArray shouldBe bytes
  }
