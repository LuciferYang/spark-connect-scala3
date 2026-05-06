package org.apache.spark.sql.connect.client

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.apache.arrow.vector.complex.*
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.arrow.vector.types.pojo.{ArrowType, Field as ArrowField, Schema as ArrowSchema}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.*

import java.io.ByteArrayInputStream
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

/** Deserializes Arrow IPC batches into Row sequences. */
object ArrowDeserializer:

  def fromArrowBatch(data: Array[Byte]): Seq[Row] =
    fromArrowBatchWithSchema(data)._1

  /** Deserialize Arrow IPC bytes, returning rows and the inferred StructType schema. */
  def fromArrowBatchWithSchema(data: Array[Byte]): (Seq[Row], Option[StructType]) =
    if data.isEmpty then return (Seq.empty, None)
    val allocator = RootAllocator(Long.MaxValue)
    try
      val reader = ArrowStreamReader(ByteArrayInputStream(data), allocator)
      try
        val rows = mutable.ArrayBuffer.empty[Row]
        val root = reader.getVectorSchemaRoot
        val schema = arrowSchemaToStructType(root.getSchema)
        while reader.loadNextBatch() do
          val numRows = root.getRowCount
          val vectors = root.getFieldVectors.asScala.toSeq
          var i = 0
          while i < numRows do
            rows += Row.fromSeq(vectors.map(v => extractValue(v, i)))
            i += 1
        (rows.toSeq, Some(schema))
      finally
        reader.close()
    finally
      allocator.close()

  private def extractValue(vector: FieldVector, index: Int): Any =
    if vector.isNull(index) then return null
    vector match
      case v: TinyIntVector    => v.get(index)
      case v: SmallIntVector   => v.get(index)
      case v: IntVector        => v.get(index)
      case v: BigIntVector     => v.get(index)
      case v: Float4Vector     => v.get(index)
      case v: Float8Vector     => v.get(index)
      case v: BitVector        => v.get(index) == 1
      case v: VarCharVector    => v.getObject(index).toString
      case v: VarBinaryVector  => v.getObject(index)
      case v: DecimalVector    => v.getObject(index)
      case v: Decimal256Vector => v.getObject(index)

      case v: DateDayVector =>
        java.sql.Date(v.get(index).toLong * 86400000L)

      case v: TimeStampMicroVector =>
        val micros = v.get(index)
        val seconds = Math.floorDiv(micros, 1_000_000L)
        val nanos = Math.floorMod(micros, 1_000_000L) * 1000
        java.sql.Timestamp.from(java.time.Instant.ofEpochSecond(seconds, nanos))

      case v: TimeStampMicroTZVector =>
        val micros = v.get(index)
        val seconds = Math.floorDiv(micros, 1_000_000L)
        val nanos = Math.floorMod(micros, 1_000_000L) * 1000
        java.sql.Timestamp.from(java.time.Instant.ofEpochSecond(seconds, nanos))

      case v: TimeStampMilliVector =>
        java.sql.Timestamp(v.get(index))

      case v: TimeStampNanoVector =>
        java.sql.Timestamp(v.get(index) / 1000000L)

      case v: TimeStampSecVector =>
        java.sql.Timestamp(v.get(index) * 1000L)

      case v: MapVector =>
        val dataVec = v.getDataVector.asInstanceOf[StructVector]
        val start = v.getElementStartIndex(index)
        val end = v.getElementEndIndex(index)
        (start until end).map { i =>
          val keyVec = dataVec.getChildByOrdinal(0).asInstanceOf[FieldVector]
          val valVec = dataVec.getChildByOrdinal(1).asInstanceOf[FieldVector]
          extractValue(keyVec, i) -> extractValue(valVec, i)
        }.toMap

      case v: ListVector =>
        val inner = v.getDataVector
        val start = v.getElementStartIndex(index)
        val end = v.getElementEndIndex(index)
        (start until end).map(i => extractValue(inner, i)).toSeq

      case v: StructVector =>
        if isVariantStruct(v) then
          val valueVec = v.getChild("value").asInstanceOf[FieldVector]
          val metadataVec = v.getChild("metadata").asInstanceOf[FieldVector]
          VariantVal(
            extractValue(valueVec, index).asInstanceOf[Array[Byte]],
            extractValue(metadataVec, index).asInstanceOf[Array[Byte]]
          )
        else
          val fields = v.getChildrenFromFields.asScala.toSeq
          Row.fromSeq(fields.map(f => extractValue(f, index)))

      case v: DurationVector =>
        v.getObject(index)

      case v: IntervalYearVector =>
        v.get(index)

      case v: LargeVarCharVector =>
        v.getObject(index).toString

      case v: LargeVarBinaryVector =>
        v.getObject(index)

      case _: NullVector =>
        null

      case v =>
        try v.getObject(index)
        catch case _: Exception => null

  /** Convert an Arrow Schema to a Spark StructType. */
  private def arrowSchemaToStructType(arrowSchema: ArrowSchema): StructType =
    StructType(arrowSchema.getFields.asScala.map { field =>
      StructField(field.getName, arrowTypeToSparkType(field), field.isNullable)
    }.toSeq)

  private def arrowTypeToSparkType(field: ArrowField): DataType =
    field.getType match
      case _: ArrowType.Bool                       => BooleanType
      case i: ArrowType.Int if i.getBitWidth == 8  => ByteType
      case i: ArrowType.Int if i.getBitWidth == 16 => ShortType
      case i: ArrowType.Int if i.getBitWidth == 32 => IntegerType
      case i: ArrowType.Int if i.getBitWidth == 64 => LongType
      case f: ArrowType.FloatingPoint              =>
        import org.apache.arrow.vector.types.FloatingPointPrecision
        f.getPrecision match
          case FloatingPointPrecision.SINGLE => FloatType
          case FloatingPointPrecision.DOUBLE => DoubleType
          case _                             => DoubleType
      case _: ArrowType.Utf8      => StringType
      case _: ArrowType.Binary    => BinaryType
      case _: ArrowType.Date      => DateType
      case t: ArrowType.Timestamp =>
        if t.getTimezone == null then TimestampNTZType else TimestampType
      case d: ArrowType.Decimal => DecimalType(d.getPrecision, d.getScale)
      case _: ArrowType.Null    => NullType
      case _: ArrowType.List    =>
        val children = field.getChildren.asScala
        if children.nonEmpty then
          ArrayType(arrowTypeToSparkType(children.head), containsNull = true)
        else ArrayType(NullType, containsNull = true)
      case _: ArrowType.Struct =>
        // Detect Variant struct pattern: children "value" (binary) + "metadata" (binary with
        // Arrow field metadata "variant"="true")
        val children = field.getChildren.asScala
        if isVariantStructSchema(children) then VariantType
        else
          StructType(children.map { child =>
            StructField(child.getName, arrowTypeToSparkType(child), child.isNullable)
          }.toSeq)
      case _: ArrowType.Map =>
        val entriesField = field.getChildren.asScala.head
        val children = entriesField.getChildren.asScala
        if children.size == 2 then
          MapType(
            arrowTypeToSparkType(children(0)),
            arrowTypeToSparkType(children(1)),
            valueContainsNull = true
          )
        else MapType(NullType, NullType, valueContainsNull = true)
      case _: ArrowType.Duration    => DayTimeIntervalType
      case _: ArrowType.Interval    => YearMonthIntervalType
      case _: ArrowType.LargeUtf8   => StringType
      case _: ArrowType.LargeBinary => BinaryType
      case _                        => StringType

  /** Check if Arrow struct field children match the Variant pattern:
    * two children named "value" and "metadata", both binary, with "metadata" having
    * Arrow field metadata key "variant" = "true".
    */
  private def isVariantStructSchema(children: mutable.Buffer[ArrowField]): Boolean =
    if children.size != 2 then return false
    val hasValue = children.exists(c =>
      c.getName == "value" && c.getType.isInstanceOf[ArrowType.Binary]
    )
    val hasMetadataWithFlag = children.exists { c =>
      c.getName == "metadata" &&
      c.getType.isInstanceOf[ArrowType.Binary] &&
      c.getMetadata != null &&
      c.getMetadata.containsKey("variant") &&
      c.getMetadata.get("variant") == "true"
    }
    hasValue && hasMetadataWithFlag

  /** Check if a StructVector at runtime matches the Variant encoding pattern. */
  private def isVariantStruct(v: StructVector): Boolean =
    val children = v.getChildrenFromFields.asScala
    if children.size != 2 then return false
    val valueChild = children.find(_.getName == "value")
    val metadataChild = children.find(_.getName == "metadata")
    (valueChild, metadataChild) match
      case (Some(_: VarBinaryVector), Some(mc: VarBinaryVector)) =>
        val meta = mc.getField.getMetadata
        meta != null && meta.containsKey("variant") && meta.get("variant") == "true"
      case _ => false
