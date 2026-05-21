package org.apache.spark.sql.connect.client

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.apache.arrow.vector.complex.*
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.arrow.vector.types.pojo.{ArrowType, Field as ArrowField, Schema as ArrowSchema}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.*

import java.io.ByteArrayInputStream
import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

/** Deserializes Arrow IPC batches into Row sequences. */
object ArrowDeserializer:

  /** Cached metadata for a StructVector: whether it's a Variant struct, and its child field
    * vectors.
    */
  private case class StructVectorMeta(isVariant: Boolean, children: Seq[FieldVector])

  /** Maximum bytes the Arrow allocator may reserve (256 GB). */
  private val MaxAllocatorBytes: Long = 256L * 1024 * 1024 * 1024

  /** Shared RootAllocator — thread-safe (uses AtomicLong internally). */
  private val rootAllocator = RootAllocator(MaxAllocatorBytes)

  def fromArrowBatch(data: Array[Byte]): Seq[Row] =
    fromArrowBatchWithSchema(data)._1

  /** Deserialize Arrow IPC bytes, returning rows and the inferred StructType schema. */
  def fromArrowBatchWithSchema(data: Array[Byte]): (Seq[Row], Option[StructType]) =
    if data.isEmpty then return (Seq.empty, None)
    val childAllocator = rootAllocator.newChildAllocator("deser", 0, MaxAllocatorBytes)
    try
      val reader = ArrowStreamReader(ByteArrayInputStream(data), childAllocator)
      try
        val rows = mutable.ArrayBuffer.empty[Row]
        val root = reader.getVectorSchemaRoot
        val schema = arrowSchemaToStructType(root.getSchema)
        val vectors = root.getFieldVectors.asScala.toArray
        val numCols = vectors.length
        val structMetaCache = mutable.HashMap.empty[StructVector, StructVectorMeta]
        while reader.loadNextBatch() do
          val numRows = root.getRowCount
          var i = 0
          while i < numRows do
            val values = new Array[Any](numCols)
            var col = 0
            while col < numCols do
              values(col) = extractValue(vectors(col), i, structMetaCache)
              col += 1
            rows += Row.fromSeqDirectWithSchema(ArraySeq.unsafeWrapArray(values), schema)
            i += 1
        // Return an indexed view over the mutable array so callers get O(1) random access
        // without a List-rebuild (ArrayBuffer.toSeq goes through an immutable.List factory).
        (ArraySeq.unsafeWrapArray(rows.toArray), Some(schema))
      finally
        reader.close()
    finally
      childAllocator.close()

  private def microsToTimestamp(micros: Long): java.sql.Timestamp =
    val seconds = Math.floorDiv(micros, 1_000_000L)
    val nanos = Math.floorMod(micros, 1_000_000L) * 1000
    java.sql.Timestamp.from(java.time.Instant.ofEpochSecond(seconds, nanos))

  private def extractValue(
      vector: FieldVector,
      index: Int,
      structMetaCache: mutable.HashMap[StructVector, StructVectorMeta]
  ): Any =
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
        java.sql.Date.valueOf(java.time.LocalDate.ofEpochDay(v.get(index).toLong))

      case v: TimeStampMicroVector =>
        microsToTimestamp(v.get(index))

      case v: TimeStampMicroTZVector =>
        microsToTimestamp(v.get(index))

      case v: TimeStampMilliVector =>
        java.sql.Timestamp(v.get(index))

      case v: TimeStampNanoVector =>
        java.sql.Timestamp(v.get(index) / 1000000L)

      case v: TimeStampSecVector =>
        java.sql.Timestamp(v.get(index) * 1000L)

      case v: MapVector =>
        val dataVec = v.getDataVector.asInstanceOf[StructVector]
        val keyVec = dataVec.getChildByOrdinal(0).asInstanceOf[FieldVector]
        val valVec = dataVec.getChildByOrdinal(1).asInstanceOf[FieldVector]
        val start = v.getElementStartIndex(index)
        val end = v.getElementEndIndex(index)
        (start until end).map(i =>
          extractValue(keyVec, i, structMetaCache) -> extractValue(valVec, i, structMetaCache)
        ).toMap

      case v: ListVector =>
        val inner = v.getDataVector
        val start = v.getElementStartIndex(index)
        val end = v.getElementEndIndex(index)
        (start until end).map(i => extractValue(inner, i, structMetaCache)).toSeq

      case v: StructVector =>
        val meta = structMetaCache.getOrElseUpdate(
          v, {
            val children = v.getChildrenFromFields.asScala.toSeq
            StructVectorMeta(isVariantStruct(v), children)
          }
        )
        if meta.isVariant then
          val valueVec = v.getChild("value").asInstanceOf[FieldVector]
          val metadataVec = v.getChild("metadata").asInstanceOf[FieldVector]
          VariantVal(
            extractValue(valueVec, index, structMetaCache).asInstanceOf[Array[Byte]],
            extractValue(metadataVec, index, structMetaCache).asInstanceOf[Array[Byte]]
          )
        else
          Row.fromSeq(meta.children.map(f => extractValue(f, index, structMetaCache)))

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
        // Fallback for vector types not covered above. `getObject` may throw on malformed
        // values (corrupt buffer, schema/data mismatch); swallow and return null so a
        // single bad value doesn't fail the whole batch — matches upstream Spark behavior.
        try v.getObject(index)
        catch case _: Exception => null

  /** Convert an Arrow Schema to a Spark StructType. */
  private def arrowSchemaToStructType(arrowSchema: ArrowSchema): StructType =
    // Iterate directly over the Java list — `.asScala.map(...).toSeq` goes through the Scala
    // Iterator adapter and then an immutable copy; a single Array pass is cheaper and produces
    // the same `Seq[StructField]` result.
    val fieldsList = arrowSchema.getFields
    val n = fieldsList.size
    val arr = new Array[StructField](n)
    var i = 0
    while i < n do
      val field = fieldsList.get(i)
      arr(i) = StructField(field.getName, arrowTypeToSparkType(field), field.isNullable)
      i += 1
    StructType(ArraySeq.unsafeWrapArray(arr))

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
        val mapChildren = field.getChildren.asScala
        if mapChildren.isEmpty then MapType(NullType, NullType, valueContainsNull = true)
        else
          val entriesField = mapChildren.head
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

  /** Check if Arrow struct field children match the Variant pattern: two children named "value" and
    * "metadata", both binary, with "metadata" having Arrow field metadata key "variant" = "true".
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
