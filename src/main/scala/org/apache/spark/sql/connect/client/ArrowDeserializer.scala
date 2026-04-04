package org.apache.spark.sql.connect.client

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.apache.arrow.vector.complex.*
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.spark.sql.Row

import java.io.ByteArrayInputStream
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

/** Deserializes Arrow IPC batches into Row sequences. */
object ArrowDeserializer:

  def fromArrowBatch(data: Array[Byte]): Seq[Row] =
    if data.isEmpty then return Seq.empty
    val allocator = RootAllocator(Long.MaxValue)
    try
      val reader = ArrowStreamReader(ByteArrayInputStream(data), allocator)
      try
        val rows = mutable.ArrayBuffer.empty[Row]
        val root = reader.getVectorSchemaRoot
        while reader.loadNextBatch() do
          val numRows = root.getRowCount
          val vectors = root.getFieldVectors.asScala.toSeq
          var i = 0
          while i < numRows do
            rows += Row.fromSeq(vectors.map(v => extractValue(v, i)))
            i += 1
        rows.toSeq
      finally
        reader.close()
    finally
      allocator.close()

  private def extractValue(vector: FieldVector, index: Int): Any =
    if vector.isNull(index) then return null
    vector match
      case v: TinyIntVector      => v.get(index)
      case v: SmallIntVector     => v.get(index)
      case v: IntVector          => v.get(index)
      case v: BigIntVector       => v.get(index)
      case v: Float4Vector       => v.get(index)
      case v: Float8Vector       => v.get(index)
      case v: BitVector          => v.get(index) == 1
      case v: VarCharVector      => v.getObject(index).toString
      case v: VarBinaryVector    => v.getObject(index)
      case v: DecimalVector      => v.getObject(index)
      case v: Decimal256Vector   => v.getObject(index)

      case v: DateDayVector =>
        java.sql.Date(v.get(index).toLong * 86400000L)

      case v: TimeStampMicroVector =>
        java.sql.Timestamp(v.get(index) / 1000L)

      case v: TimeStampMilliVector =>
        java.sql.Timestamp(v.get(index))

      case v: TimeStampNanoVector =>
        java.sql.Timestamp(v.get(index) / 1000000L)

      case v: TimeStampSecVector =>
        java.sql.Timestamp(v.get(index) * 1000L)

      case v: ListVector =>
        val inner = v.getDataVector
        val start = v.getElementStartIndex(index)
        val end = v.getElementEndIndex(index)
        (start until end).map(i => extractValue(inner, i)).toSeq

      case v: StructVector =>
        val fields = v.getChildrenFromFields.asScala.toSeq
        Row.fromSeq(fields.map(f => extractValue(f, index)))

      case v: MapVector =>
        extractValue(v.getDataVector, index) match
          case seq: Seq[?] =>
            seq.collect { case row: Row if row.size == 2 => row.get(0) -> row.get(1) }.toMap
          case _ => Map.empty

      case v =>
        try v.getObject(index)
        catch case _: Exception => null
