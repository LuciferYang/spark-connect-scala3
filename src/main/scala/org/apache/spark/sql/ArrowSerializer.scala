package org.apache.spark.sql

import org.apache.arrow.memory.RootAllocator
import org.apache.spark.sql.connect.client.ArrowAllocators
import org.apache.arrow.vector.{
  BigIntVector,
  BitVector,
  DateDayVector,
  DecimalVector,
  DurationVector,
  FieldVector,
  Float4Vector,
  Float8Vector,
  IntervalYearVector,
  IntVector,
  SmallIntVector,
  TimeMicroVector,
  TimeStampMicroTZVector,
  TimeStampMicroVector,
  TinyIntVector,
  VarBinaryVector,
  VarCharVector,
  VectorSchemaRoot
}
import org.apache.arrow.vector.complex.{ListVector, MapVector, StructVector}
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema as ArrowSchema}

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters.*

/** Encodes Row sequences into Arrow IPC byte arrays. */
private[sql] object ArrowSerializer:

  /** Maximum bytes the Arrow allocator may reserve (default 256 GB; configurable — see
    * [[org.apache.spark.sql.connect.client.ArrowAllocators]]).
    */
  private val MaxAllocatorBytes: Long = ArrowAllocators.maxAllocatorBytes

  /** Shared RootAllocator — thread-safe (uses AtomicLong internally). */
  private val rootAllocator = RootAllocator(MaxAllocatorBytes)

  def encodeRows(rows: Seq[Row], schema: types.StructType): Array[Byte] =
    if rows.isEmpty then return Array.emptyByteArray

    val allocator = rootAllocator.newChildAllocator("ser", 0, MaxAllocatorBytes)
    val baos = ByteArrayOutputStream()
    try
      val arrowFields =
        schema.fields.map(f => sparkTypeToArrowField(f.name, f.dataType, f.nullable)).toList.asJava
      val arrowSchema = ArrowSchema(arrowFields)
      val root = VectorSchemaRoot.create(arrowSchema, allocator)
      try
        val writer = ArrowStreamWriter(root, null, baos)
        try
          writer.start()
          root.setRowCount(rows.size)
          val vectors = root.getFieldVectors.asScala.toArray
          val numCols = vectors.length
          val numRows = rows.length
          // Resolve the vector-type dispatch and the column DataType once per column rather than
          // once per (row × column) cell — the vector type is fixed for the whole column.
          val fields = schema.fields
          val setters = new Array[(Int, Any) => Unit](numCols)
          var setupCol = 0
          while setupCol < numCols do
            setters(setupCol) = buildColumnSetter(vectors(setupCol), fields(setupCol).dataType)
            setupCol += 1
          var rowIdx = 0
          while rowIdx < numRows do
            val row = rows(rowIdx)
            var colIdx = 0
            while colIdx < numCols do
              setters(colIdx)(rowIdx, row.get(colIdx))
              colIdx += 1
            rowIdx += 1
          vectors.foreach(_.setValueCount(rows.size))
          writer.writeBatch()
          writer.end()
        finally
          writer.close()
      finally
        root.close()
      baos.toByteArray
    finally
      allocator.close()

  private def sparkTypeToArrow(dt: types.DataType): ArrowType = dt match
    case types.BooleanType    => ArrowType.Bool.INSTANCE
    case types.ByteType       => new ArrowType.Int(8, true)
    case types.ShortType      => new ArrowType.Int(16, true)
    case types.IntegerType    => new ArrowType.Int(32, true)
    case types.LongType       => new ArrowType.Int(64, true)
    case types.FloatType      => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    case types.DoubleType     => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    case types.StringType     => ArrowType.Utf8.INSTANCE
    case _: types.CharType    => ArrowType.Utf8.INSTANCE
    case _: types.VarcharType => ArrowType.Utf8.INSTANCE
    case types.DateType       => new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)
    case types.TimestampType  =>
      new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "UTC")
    case types.TimestampNTZType =>
      new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, null)
    case d: types.DecimalType      => new ArrowType.Decimal(d.precision, d.scale, 128)
    case types.BinaryType          => ArrowType.Binary.INSTANCE
    case types.VariantType         => ArrowType.Struct.INSTANCE
    case _: types.ArrayType        => ArrowType.List.INSTANCE
    case _: types.StructType       => ArrowType.Struct.INSTANCE
    case _: types.MapType          => new ArrowType.Map(false)
    case types.DayTimeIntervalType =>
      new ArrowType.Duration(org.apache.arrow.vector.types.TimeUnit.MICROSECOND)
    case types.YearMonthIntervalType =>
      new ArrowType.Interval(org.apache.arrow.vector.types.IntervalUnit.YEAR_MONTH)
    case _: types.TimeType =>
      new ArrowType.Time(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, 64)
    case _: types.GeometryType  => ArrowType.Binary.INSTANCE
    case _: types.GeographyType => ArrowType.Binary.INSTANCE
    case types.NullType         => ArrowType.Null.INSTANCE

  /** Convert a Spark DataType to an Arrow Field with child fields for complex types. */
  private def sparkTypeToArrowField(
      name: String,
      dt: types.DataType,
      nullable: Boolean
  ): Field =
    dt match
      case at: types.ArrayType =>
        val childField = sparkTypeToArrowField("element", at.elementType, at.containsNull)
        Field(
          name,
          FieldType(nullable, ArrowType.List.INSTANCE, null),
          java.util.Collections.singletonList(childField)
        )
      case st: types.StructType =>
        val children = st.fields.map(f =>
          sparkTypeToArrowField(f.name, f.dataType, f.nullable)
        ).toList.asJava
        Field(name, FieldType(nullable, ArrowType.Struct.INSTANCE, null), children)
      case mt: types.MapType =>
        val keyField = sparkTypeToArrowField("key", mt.keyType, false)
        val valueField = sparkTypeToArrowField("value", mt.valueType, mt.valueContainsNull)
        val entriesField = Field(
          "entries",
          FieldType(false, ArrowType.Struct.INSTANCE, null),
          java.util.List.of(keyField, valueField)
        )
        Field(
          name,
          FieldType(nullable, new ArrowType.Map(false), null),
          java.util.Collections.singletonList(entriesField)
        )
      case types.VariantType =>
        // Variant is wire-encoded as a Struct with two binary children, mirroring upstream
        // ArrowWriter.VariantWriter. The "metadata" child carries an Arrow field-level metadata
        // entry "variant" -> "true" so the deserializer (and server) can recognise the shape.
        val valueChild = Field(
          "value",
          FieldType(false, ArrowType.Binary.INSTANCE, null),
          java.util.Collections.emptyList()
        )
        val metadataChild = Field(
          "metadata",
          FieldType(
            false,
            ArrowType.Binary.INSTANCE,
            null,
            java.util.Map.of("variant", "true")
          ),
          java.util.Collections.emptyList()
        )
        Field(
          name,
          FieldType(nullable, ArrowType.Struct.INSTANCE, null),
          java.util.List.of(valueChild, metadataChild)
        )
      case _ =>
        val arrowType = sparkTypeToArrow(dt)
        Field(
          name,
          FieldType(nullable, arrowType, null),
          java.util.Collections.emptyList()
        )

  private def toMicros(value: Any): Long = value match
    case ts: java.sql.Timestamp =>
      ts.getTime * 1000 + (ts.getNanos / 1000) % 1000
    case inst: java.time.Instant =>
      inst.getEpochSecond * 1_000_000 + inst.getNano / 1000
    case n: Number => n.longValue()
    case _         => value.toString.toLong

  /** Build a reusable value setter for one column.
    *
    * The vector-type dispatch happens once here instead of once per (row × column) cell. The
    * returned closure handles nulls uniformly; for the fixed vector type it writes directly.
    * Complex/nested vectors (Map/List/Struct) delegate to [[setArrowValue]], which recurses
    * per element — those columns are uncommon and inherently per-element.
    */
  private def buildColumnSetter(vec: FieldVector, dt: types.DataType): (Int, Any) => Unit =
    val raw: (Int, Any) => Unit = vec match
      case v: BitVector =>
        (idx, value) => v.setSafe(idx, if value.asInstanceOf[Boolean] then 1 else 0)
      case v: TinyIntVector =>
        (idx, value) => v.setSafe(idx, value.asInstanceOf[Byte].toInt)
      case v: SmallIntVector =>
        (idx, value) => v.setSafe(idx, value.asInstanceOf[Short].toInt)
      case v: IntVector =>
        (idx, value) => v.setSafe(idx, value.asInstanceOf[Int])
      case v: BigIntVector =>
        (idx, value) => v.setSafe(idx, value.asInstanceOf[Long])
      case v: Float4Vector =>
        (idx, value) => v.setSafe(idx, value.asInstanceOf[Float])
      case v: Float8Vector =>
        (idx, value) => v.setSafe(idx, value.asInstanceOf[Double])
      case v: VarCharVector =>
        (idx, value) => v.setSafe(idx, value.toString.getBytes(StandardCharsets.UTF_8))
      case v: DateDayVector =>
        (idx, value) =>
          val epochDay = value match
            case d: java.sql.Date        => d.toLocalDate.toEpochDay.toInt
            case ld: java.time.LocalDate => ld.toEpochDay.toInt
            case n: Number               => n.intValue()
            case _                       => value.toString.toInt
          v.setSafe(idx, epochDay)
      case v: TimeStampMicroTZVector =>
        (idx, value) => v.setSafe(idx, toMicros(value))
      case v: TimeStampMicroVector =>
        (idx, value) => v.setSafe(idx, toMicros(value))
      case v: DecimalVector =>
        (idx, value) =>
          val bd0 = value match
            case d: BigDecimal           => d.underlying()
            case d: java.math.BigDecimal => d
            case _                       => java.math.BigDecimal(value.toString)
          // Match upstream ArrowWriter.DecimalWriter: rescale to the vector's declared scale
          // (HALF_UP), then null out values that overflow the declared precision.
          val rescaled = bd0.setScale(v.getScale, java.math.RoundingMode.HALF_UP)
          if rescaled.precision <= v.getPrecision then v.setSafe(idx, rescaled)
          else v.setNull(idx)
      case v: DurationVector =>
        (idx, value) =>
          value match
            case d: java.time.Duration =>
              v.setSafe(idx, d.getSeconds * 1_000_000L + d.getNano / 1_000L)
            case n: Number =>
              v.setSafe(idx, n.longValue())
            case _ =>
              v.setSafe(idx, value.toString.toLong)
      case v: IntervalYearVector =>
        (idx, value) =>
          value match
            case p: java.time.Period =>
              v.setSafe(idx, p.toTotalMonths.toInt)
            case n: Number =>
              v.setSafe(idx, n.intValue())
            case _ =>
              v.setSafe(idx, value.toString.toInt)
      case v: TimeMicroVector =>
        (idx, value) =>
          value match
            case t: java.time.LocalTime =>
              v.setSafe(idx, t.toNanoOfDay / 1_000L)
            case n: Number =>
              v.setSafe(idx, n.longValue())
            case _ =>
              v.setSafe(idx, value.toString.toLong)
      case v: VarBinaryVector =>
        (idx, value) =>
          val bytes = value.asInstanceOf[Array[Byte]]
          v.setSafe(idx, bytes, 0, bytes.length)
      // Complex/nested vectors recurse per element via setArrowValue.
      case _: MapVector =>
        (idx, value) => setArrowValue(vec, idx, value, dt)
      case _: ListVector =>
        (idx, value) => setArrowValue(vec, idx, value, dt)
      case _: StructVector =>
        (idx, value) => setArrowValue(vec, idx, value, dt)
      case other =>
        (_, _) =>
          throw UnsupportedOperationException(
            s"Unsupported Arrow vector type: ${other.getClass.getName}"
          )
    (idx, value) =>
      if value == null then vec.setNull(idx)
      else raw(idx, value)

  private def setArrowValue(vec: FieldVector, idx: Int, value: Any, dt: types.DataType): Unit =
    if value == null then
      vec.setNull(idx)
    else
      vec match
        case v: BitVector      => v.setSafe(idx, if value.asInstanceOf[Boolean] then 1 else 0)
        case v: TinyIntVector  => v.setSafe(idx, value.asInstanceOf[Byte].toInt)
        case v: SmallIntVector => v.setSafe(idx, value.asInstanceOf[Short].toInt)
        case v: IntVector      => v.setSafe(idx, value.asInstanceOf[Int])
        case v: BigIntVector   => v.setSafe(idx, value.asInstanceOf[Long])
        case v: Float4Vector   => v.setSafe(idx, value.asInstanceOf[Float])
        case v: Float8Vector   => v.setSafe(idx, value.asInstanceOf[Double])
        case v: VarCharVector  => v.setSafe(idx, value.toString.getBytes(StandardCharsets.UTF_8))
        case v: DateDayVector  =>
          val epochDay = value match
            case d: java.sql.Date        => d.toLocalDate.toEpochDay.toInt
            case ld: java.time.LocalDate => ld.toEpochDay.toInt
            case n: Number               => n.intValue()
            case _                       => value.toString.toInt
          v.setSafe(idx, epochDay)
        case v: TimeStampMicroTZVector =>
          v.setSafe(idx, toMicros(value))
        case v: TimeStampMicroVector =>
          v.setSafe(idx, toMicros(value))
        case v: DecimalVector =>
          val bd0 = value match
            case d: BigDecimal           => d.underlying()
            case d: java.math.BigDecimal => d
            case _                       => java.math.BigDecimal(value.toString)
          // Match upstream ArrowWriter.DecimalWriter: rescale to the vector's declared scale
          // (HALF_UP), then null out values that overflow the declared precision. Arrow's
          // DecimalVector.setSafe would otherwise raise UnsupportedOperationException on any
          // mismatched scale (e.g. BigDecimal("99") against DecimalType(10,2)).
          val rescaled = bd0.setScale(v.getScale, java.math.RoundingMode.HALF_UP)
          if rescaled.precision <= v.getPrecision then v.setSafe(idx, rescaled)
          else v.setNull(idx)
        case v: DurationVector =>
          // sparkTypeToArrow maps DayTimeIntervalType -> Duration(MICROSECOND), so the value is
          // serialized as microseconds (not raw nanoseconds). Number values are passed through.
          value match
            case d: java.time.Duration =>
              v.setSafe(idx, d.getSeconds * 1_000_000L + d.getNano / 1_000L)
            case n: Number =>
              v.setSafe(idx, n.longValue())
            case _ =>
              v.setSafe(idx, value.toString.toLong)
        case v: IntervalYearVector =>
          value match
            case p: java.time.Period =>
              v.setSafe(idx, p.toTotalMonths.toInt)
            case n: Number =>
              v.setSafe(idx, n.intValue())
            case _ =>
              v.setSafe(idx, value.toString.toInt)
        case v: TimeMicroVector =>
          value match
            case t: java.time.LocalTime =>
              v.setSafe(idx, t.toNanoOfDay / 1_000L)
            case n: Number =>
              v.setSafe(idx, n.longValue())
            case _ =>
              v.setSafe(idx, value.toString.toLong)
        case v: VarBinaryVector =>
          val bytes = value.asInstanceOf[Array[Byte]]
          v.setSafe(idx, bytes, 0, bytes.length)
        case v: MapVector =>
          val mt = dt.asInstanceOf[types.MapType]
          val dataVec = v.getDataVector.asInstanceOf[StructVector]
          val keyVec = dataVec.getChildByOrdinal(0).asInstanceOf[FieldVector]
          val valVec = dataVec.getChildByOrdinal(1).asInstanceOf[FieldVector]
          val offset = v.startNewValue(idx)
          value match
            case m: Map[?, ?] =>
              var i = 0
              m.foreach { (key, valu) =>
                // Arrow Map keys must not be null — a null key violates the Arrow IPC invariant
                // (the key child vector is declared non-nullable). Fail fast here to match
                // upstream ArrowWriter.extractKey(Objects.requireNonNull(key)).
                if key == null then
                  throw NullPointerException("Map keys must not be null in Arrow serialization")
                setArrowValue(keyVec, offset + i, key, mt.keyType)
                setArrowValue(valVec, offset + i, valu, mt.valueType)
                i += 1
              }
              v.endValue(idx, i)
            case m: java.util.Map[?, ?] =>
              var i = 0
              m.forEach { (key, valu) =>
                if key == null then
                  throw NullPointerException("Map keys must not be null in Arrow serialization")
                setArrowValue(keyVec, offset + i, key, mt.keyType)
                setArrowValue(valVec, offset + i, valu, mt.valueType)
                i += 1
              }
              v.endValue(idx, i)
            case _ =>
              throw IllegalArgumentException(s"Cannot convert ${value.getClass} to Map")
        case v: ListVector =>
          val elemType = dt.asInstanceOf[types.ArrayType].elementType
          val dataVec = v.getDataVector.asInstanceOf[FieldVector]
          val offset = v.startNewValue(idx)
          val items: Seq[Any] = value match
            case a: Array[?]          => a.toSeq
            case s: Iterable[?]       => s.toSeq
            case j: java.util.List[?] =>
              val buf = scala.collection.mutable.ArrayBuffer[Any]()
              j.forEach(item => buf += item)
              buf.toSeq
            case other => Seq(other)
          var i = 0
          while i < items.size do
            setArrowValue(dataVec, offset + i, items(i), elemType)
            i += 1
          v.endValue(idx, items.size)
        case v: StructVector =>
          dt match
            case types.VariantType =>
              val variant = value.asInstanceOf[org.apache.spark.sql.types.VariantVal]
              val valueVec = v.getChild("value", classOf[VarBinaryVector])
              val metadataVec = v.getChild("metadata", classOf[VarBinaryVector])
              val vBytes = variant.value
              val mBytes = variant.metadata
              valueVec.setSafe(idx, vBytes, 0, vBytes.length)
              metadataVec.setSafe(idx, mBytes, 0, mBytes.length)
              v.setIndexDefined(idx)
            case st: types.StructType =>
              val row = value.asInstanceOf[Row]
              val structWriter = v.getWriter
              structWriter.setPosition(idx)
              structWriter.start()
              for i <- 0 until row.size do
                val childVec = v.getChildByOrdinal(i).asInstanceOf[FieldVector]
                setArrowValue(childVec, idx, row.get(i), st.fields(i).dataType)
              structWriter.end()
            case other =>
              throw UnsupportedOperationException(
                s"Cannot write StructVector for non-Struct/non-Variant DataType: $other"
              )
        case other =>
          throw UnsupportedOperationException(
            s"Unsupported Arrow vector type: ${other.getClass.getName}"
          )
