package org.apache.spark.sql

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, BitVector, FieldVector, Float4Vector, Float8Vector, IntVector,
  SmallIntVector, TinyIntVector, VarCharVector, VectorSchemaRoot}
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema as ArrowSchema}

import java.io.ByteArrayOutputStream
import scala.jdk.CollectionConverters.*

/** Encodes Row sequences into Arrow IPC byte arrays. */
private[sql] object ArrowSerializer:

  def encodeRows(rows: Seq[Row], schema: types.StructType): Array[Byte] =
    if rows.isEmpty then return Array.emptyByteArray

    val allocator = RootAllocator(Long.MaxValue)
    val baos = ByteArrayOutputStream()
    try
      val arrowFields = schema.fields.map { f =>
        val arrowType = sparkTypeToArrow(f.dataType)
        Field(f.name, FieldType(f.nullable, arrowType, null), java.util.Collections.emptyList())
      }.toList.asJava
      val arrowSchema = ArrowSchema(arrowFields)
      val root = VectorSchemaRoot.create(arrowSchema, allocator)
      try
        val writer = ArrowStreamWriter(root, null, baos)
        writer.start()
        root.setRowCount(rows.size)
        val vectors = root.getFieldVectors.asScala.toSeq
        rows.indices.foreach { rowIdx =>
          val row = rows(rowIdx)
          vectors.zipWithIndex.foreach { (vec, colIdx) =>
            setArrowValue(vec, rowIdx, row.get(colIdx), schema.fields(colIdx).dataType)
          }
        }
        vectors.foreach(_.setValueCount(rows.size))
        writer.writeBatch()
        writer.end()
      finally
        root.close()
      baos.toByteArray
    finally
      allocator.close()

  private def sparkTypeToArrow(dt: types.DataType): ArrowType = dt match
    case types.BooleanType  => ArrowType.Bool.INSTANCE
    case types.ByteType     => new ArrowType.Int(8, true)
    case types.ShortType    => new ArrowType.Int(16, true)
    case types.IntegerType  => new ArrowType.Int(32, true)
    case types.LongType     => new ArrowType.Int(64, true)
    case types.FloatType    => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    case types.DoubleType   => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    case types.StringType   => ArrowType.Utf8.INSTANCE
    case _                  => ArrowType.Utf8.INSTANCE // fallback

  private def setArrowValue(vec: FieldVector, idx: Int, value: Any, dt: types.DataType): Unit =
    if value == null then
      vec.setNull(idx)
    else
      vec match
        case v: BitVector     => v.setSafe(idx, if value.asInstanceOf[Boolean] then 1 else 0)
        case v: TinyIntVector => v.setSafe(idx, value.asInstanceOf[Byte].toInt)
        case v: SmallIntVector => v.setSafe(idx, value.asInstanceOf[Short].toInt)
        case v: IntVector     => v.setSafe(idx, value.asInstanceOf[Int])
        case v: BigIntVector  => v.setSafe(idx, value.asInstanceOf[Long])
        case v: Float4Vector  => v.setSafe(idx, value.asInstanceOf[Float])
        case v: Float8Vector  => v.setSafe(idx, value.asInstanceOf[Double])
        case v: VarCharVector => v.setSafe(idx, value.toString.getBytes("UTF-8"))
        case _ => () // unsupported type
