package org.apache.spark.sql.connect.client

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.apache.arrow.vector.complex.*
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema as ArrowSchema}
import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import scala.jdk.CollectionConverters.*

class ArrowDeserializerSuite extends AnyFunSuite with Matchers:

  /** Encode a single-column Arrow IPC batch from a vector-writing callback. */
  private def encodeArrowBatch(field: Field)(
      writeFn: (FieldVector, VectorSchemaRoot) => Unit
  ): Array[Byte] =
    val allocator = RootAllocator(Long.MaxValue)
    val schema = ArrowSchema(java.util.Collections.singletonList(field))
    val root = VectorSchemaRoot.create(schema, allocator)
    try
      writeFn(root.getFieldVectors.get(0), root)
      val baos = ByteArrayOutputStream()
      val writer = ArrowStreamWriter(root, null, baos)
      writer.start()
      writer.writeBatch()
      writer.end()
      writer.close()
      baos.toByteArray
    finally
      root.close()
      allocator.close()

  // ---------------------------------------------------------------------------
  // Timestamp microsecond precision
  // ---------------------------------------------------------------------------

  test("TimeStampMicroVector preserves microsecond precision") {
    val field = Field(
      "ts",
      FieldType.nullable(
        new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, null)
      ),
      java.util.Collections.emptyList()
    )

    // 1_706_000_000_000_999L = a timestamp with 999 microseconds
    val inputMicros = 1_706_000_000_000_999L

    val bytes = encodeArrowBatch(field) { (vec, root) =>
      val tv = vec.asInstanceOf[TimeStampMicroVector]
      tv.setSafe(0, inputMicros)
      tv.setValueCount(1)
      root.setRowCount(1)
    }

    val (rows, _) = ArrowDeserializer.fromArrowBatchWithSchema(bytes)
    rows should have size 1
    val ts = rows.head.get(0).asInstanceOf[java.sql.Timestamp]
    // Convert back to micros
    val resultMicros = ts.getTime * 1000 + (ts.getNanos / 1000) % 1000
    resultMicros shouldBe inputMicros
  }

  test("TimeStampMicroTZVector preserves microsecond precision") {
    val field = Field(
      "ts",
      FieldType.nullable(
        new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "UTC")
      ),
      java.util.Collections.emptyList()
    )

    val inputMicros = 1_706_000_000_000_999L

    val bytes = encodeArrowBatch(field) { (vec, root) =>
      val tv = vec.asInstanceOf[TimeStampMicroTZVector]
      tv.setSafe(0, inputMicros)
      tv.setValueCount(1)
      root.setRowCount(1)
    }

    val (rows, _) = ArrowDeserializer.fromArrowBatchWithSchema(bytes)
    rows should have size 1
    val ts = rows.head.get(0).asInstanceOf[java.sql.Timestamp]
    val resultMicros = ts.getTime * 1000 + (ts.getNanos / 1000) % 1000
    resultMicros shouldBe inputMicros
  }

  // ---------------------------------------------------------------------------
  // LargeVarChar
  // ---------------------------------------------------------------------------

  test("LargeVarCharVector deserializes to String") {
    val field = Field(
      "s",
      FieldType.nullable(ArrowType.LargeUtf8.INSTANCE),
      java.util.Collections.emptyList()
    )

    val bytes = encodeArrowBatch(field) { (vec, root) =>
      val lv = vec.asInstanceOf[LargeVarCharVector]
      lv.setSafe(0, "hello large".getBytes("UTF-8"))
      lv.setValueCount(1)
      root.setRowCount(1)
    }

    val (rows, schema) = ArrowDeserializer.fromArrowBatchWithSchema(bytes)
    rows should have size 1
    rows.head.get(0) shouldBe "hello large"
    schema.get.fields.head.dataType shouldBe StringType
  }

  // ---------------------------------------------------------------------------
  // NullVector
  // ---------------------------------------------------------------------------

  test("NullVector deserializes to null") {
    val field = Field(
      "n",
      FieldType.nullable(ArrowType.Null.INSTANCE),
      java.util.Collections.emptyList()
    )

    val bytes = encodeArrowBatch(field) { (vec, root) =>
      vec.asInstanceOf[NullVector]
      // NullVector has no setSafe — all values are null by definition
      vec.setValueCount(1)
      root.setRowCount(1)
    }

    val (rows, schema) = ArrowDeserializer.fromArrowBatchWithSchema(bytes)
    rows should have size 1
    rows.head.get(0) shouldBe (null: Any)
    schema.get.fields.head.dataType shouldBe NullType
  }

  // ---------------------------------------------------------------------------
  // arrowTypeToSparkType mapping
  // ---------------------------------------------------------------------------

  test("arrowTypeToSparkType: Duration maps to DayTimeIntervalType") {
    val field = Field(
      "d",
      FieldType.nullable(
        new ArrowType.Duration(org.apache.arrow.vector.types.TimeUnit.MICROSECOND)
      ),
      java.util.Collections.emptyList()
    )

    val bytes = encodeArrowBatch(field) { (vec, root) =>
      val dv = vec.asInstanceOf[DurationVector]
      dv.setSafe(0, 1_000_000L) // 1 second in micros
      dv.setValueCount(1)
      root.setRowCount(1)
    }

    val (rows, schema) = ArrowDeserializer.fromArrowBatchWithSchema(bytes)
    rows should have size 1
    schema.get.fields.head.dataType shouldBe DayTimeIntervalType
  }

  test("arrowTypeToSparkType: LargeBinary maps to BinaryType") {
    val field = Field(
      "b",
      FieldType.nullable(ArrowType.LargeBinary.INSTANCE),
      java.util.Collections.emptyList()
    )

    val bytes = encodeArrowBatch(field) { (vec, root) =>
      val lv = vec.asInstanceOf[LargeVarBinaryVector]
      lv.setSafe(0, Array[Byte](1, 2, 3))
      lv.setValueCount(1)
      root.setRowCount(1)
    }

    val (rows, schema) = ArrowDeserializer.fromArrowBatchWithSchema(bytes)
    rows should have size 1
    schema.get.fields.head.dataType shouldBe BinaryType
  }

  // ---------------------------------------------------------------------------
  // Empty data
  // ---------------------------------------------------------------------------

  test("fromArrowBatch with empty data returns empty sequence") {
    val result = ArrowDeserializer.fromArrowBatch(Array.emptyByteArray)
    result shouldBe empty
  }

  // ---------------------------------------------------------------------------
  // Variant struct pattern detection
  // ---------------------------------------------------------------------------

  /** Create an Arrow batch with a Variant struct (value + metadata with variant=true flag). */
  private def encodeVariantBatch(value: Array[Byte], metadata: Array[Byte]): Array[Byte] =
    val allocator = RootAllocator(Long.MaxValue)
    val metadataMap = new java.util.HashMap[String, String]()
    metadataMap.put("variant", "true")
    val valueField = Field(
      "value",
      FieldType.nullable(ArrowType.Binary.INSTANCE),
      java.util.Collections.emptyList()
    )
    val metadataField = Field(
      "metadata",
      new FieldType(true, ArrowType.Binary.INSTANCE, null, metadataMap),
      java.util.Collections.emptyList()
    )
    val structField = Field(
      "v",
      FieldType.nullable(ArrowType.Struct.INSTANCE),
      java.util.Arrays.asList(valueField, metadataField)
    )
    val schema = ArrowSchema(java.util.Collections.singletonList(structField))
    val root = VectorSchemaRoot.create(schema, allocator)
    try
      val struct = root.getFieldVectors.get(0).asInstanceOf[StructVector]
      val valueVec = struct.getChild("value").asInstanceOf[VarBinaryVector]
      val metadataVec = struct.getChild("metadata").asInstanceOf[VarBinaryVector]
      valueVec.setSafe(0, value)
      metadataVec.setSafe(0, metadata)
      struct.setIndexDefined(0)
      valueVec.setValueCount(1)
      metadataVec.setValueCount(1)
      struct.setValueCount(1)
      root.setRowCount(1)
      val baos = ByteArrayOutputStream()
      val writer = ArrowStreamWriter(root, null, baos)
      writer.start()
      writer.writeBatch()
      writer.end()
      writer.close()
      baos.toByteArray
    finally
      root.close()
      allocator.close()

  test("Variant struct is detected and deserialized as VariantVal") {
    val value = Array[Byte](1, 2, 3, 4)
    val metadata = Array[Byte](10, 20)
    val bytes = encodeVariantBatch(value, metadata)

    val (rows, schema) = ArrowDeserializer.fromArrowBatchWithSchema(bytes)
    rows should have size 1
    schema.get.fields.head.dataType shouldBe VariantType

    val variant = rows.head.get(0).asInstanceOf[VariantVal]
    variant.getValue shouldBe value
    variant.getMetadata shouldBe metadata
  }

  test("Variant struct schema inferred as VariantType") {
    val bytes = encodeVariantBatch(Array[Byte](1), Array[Byte](2))
    val (_, schema) = ArrowDeserializer.fromArrowBatchWithSchema(bytes)
    schema.get.fields.head.dataType shouldBe VariantType
  }

  test("Variant struct with null value produces null") {
    val allocator = RootAllocator(Long.MaxValue)
    val metadataMap = new java.util.HashMap[String, String]()
    metadataMap.put("variant", "true")
    val valueField = Field(
      "value",
      FieldType.nullable(ArrowType.Binary.INSTANCE),
      java.util.Collections.emptyList()
    )
    val metadataField = Field(
      "metadata",
      new FieldType(true, ArrowType.Binary.INSTANCE, null, metadataMap),
      java.util.Collections.emptyList()
    )
    val structField = Field(
      "v",
      FieldType.nullable(ArrowType.Struct.INSTANCE),
      java.util.Arrays.asList(valueField, metadataField)
    )
    val schema = ArrowSchema(java.util.Collections.singletonList(structField))
    val root = VectorSchemaRoot.create(schema, allocator)
    try
      val struct = root.getFieldVectors.get(0).asInstanceOf[StructVector]
      // Leave the struct null (don't call setIndexDefined)
      struct.setValueCount(1)
      root.setRowCount(1)
      val baos = ByteArrayOutputStream()
      val writer = ArrowStreamWriter(root, null, baos)
      writer.start()
      writer.writeBatch()
      writer.end()
      writer.close()

      val (rows, _) = ArrowDeserializer.fromArrowBatchWithSchema(baos.toByteArray)
      rows should have size 1
      rows.head.get(0) shouldBe (null: Any)
    finally
      root.close()
      allocator.close()
  }

  test("Regular struct (no variant metadata) still produces Row") {
    val valueField = Field(
      "value",
      FieldType.nullable(ArrowType.Binary.INSTANCE),
      java.util.Collections.emptyList()
    )
    // No variant metadata on this field
    val metadataField = Field(
      "metadata",
      FieldType.nullable(ArrowType.Binary.INSTANCE),
      java.util.Collections.emptyList()
    )
    val structField = Field(
      "s",
      FieldType.nullable(ArrowType.Struct.INSTANCE),
      java.util.Arrays.asList(valueField, metadataField)
    )

    val allocator = RootAllocator(Long.MaxValue)
    val schema = ArrowSchema(java.util.Collections.singletonList(structField))
    val root = VectorSchemaRoot.create(schema, allocator)
    try
      val struct = root.getFieldVectors.get(0).asInstanceOf[StructVector]
      val valueVec = struct.getChild("value").asInstanceOf[VarBinaryVector]
      val metadataVec = struct.getChild("metadata").asInstanceOf[VarBinaryVector]
      valueVec.setSafe(0, Array[Byte](1, 2))
      metadataVec.setSafe(0, Array[Byte](3, 4))
      struct.setIndexDefined(0)
      valueVec.setValueCount(1)
      metadataVec.setValueCount(1)
      struct.setValueCount(1)
      root.setRowCount(1)
      val baos = ByteArrayOutputStream()
      val writer = ArrowStreamWriter(root, null, baos)
      writer.start()
      writer.writeBatch()
      writer.end()
      writer.close()

      val (rows, inferredSchema) = ArrowDeserializer.fromArrowBatchWithSchema(baos.toByteArray)
      rows should have size 1
      // Should be a regular Row, not a VariantVal
      rows.head.get(0) shouldBe a[org.apache.spark.sql.Row]
      inferredSchema.get.fields.head.dataType shouldBe a[StructType]
    finally
      root.close()
      allocator.close()
  }
