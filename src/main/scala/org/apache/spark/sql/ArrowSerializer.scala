package org.apache.spark.sql

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{
  BigIntVector,
  BitVector,
  DateDayVector,
  DecimalVector,
  FieldVector,
  Float4Vector,
  Float8Vector,
  IntVector,
  SmallIntVector,
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

  /** Maximum bytes the Arrow allocator may reserve (256 GB). */
  private val MaxAllocatorBytes: Long = 256L * 1024 * 1024 * 1024

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
          var rowIdx = 0
          while rowIdx < numRows do
            val row = rows(rowIdx)
            var colIdx = 0
            while colIdx < numCols do
              setArrowValue(
                vectors(colIdx),
                rowIdx,
                row.get(colIdx),
                schema.fields(colIdx).dataType
              )
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
    case types.VariantType         => ArrowType.Binary.INSTANCE
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
          val bd = value match
            case d: BigDecimal           => d.underlying()
            case d: java.math.BigDecimal => d
            case _                       => java.math.BigDecimal(value.toString)
          v.setSafe(idx, bd)
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
                setArrowValue(keyVec, offset + i, key, mt.keyType)
                setArrowValue(valVec, offset + i, valu, mt.valueType)
                i += 1
              }
              v.endValue(idx, i)
            case m: java.util.Map[?, ?] =>
              var i = 0
              m.forEach { (key, valu) =>
                setArrowValue(keyVec, offset + i, key, mt.keyType)
                setArrowValue(valVec, offset + i, valu, mt.valueType)
                i += 1
              }
              v.endValue(idx, i)
            case _ =>
              throw IllegalArgumentException(s"Cannot convert ${value.getClass} to Map")
        case v: ListVector =>
          val listWriter = v.getWriter
          listWriter.setPosition(idx)
          listWriter.startList()
          val writeItem = (item: Any) =>
            if item == null then listWriter.writeNull()
            else
              item match
                case i: Int     => listWriter.integer().writeInt(i)
                case l: Long    => listWriter.bigInt().writeBigInt(l)
                case d: Double  => listWriter.float8().writeFloat8(d)
                case f: Float   => listWriter.float4().writeFloat4(f)
                case s: Short   => listWriter.smallInt().writeSmallInt(s)
                case b: Byte    => listWriter.tinyInt().writeTinyInt(b)
                case b: Boolean => listWriter.bit().writeBit(if b then 1 else 0)
                case s: String  =>
                  val bytes = s.getBytes(StandardCharsets.UTF_8)
                  val buf = v.getAllocator.buffer(bytes.length)
                  try
                    buf.writeBytes(bytes)
                    listWriter.varChar().writeVarChar(0, bytes.length, buf)
                  finally buf.close()
                case _ => listWriter.writeNull()
          value match
            case a: Array[?]          => a.foreach(writeItem)
            case s: Iterable[?]       => s.foreach(writeItem)
            case j: java.util.List[?] => j.forEach(item => writeItem(item))
            case _                    => writeItem(value)
          listWriter.endList()
        case v: StructVector =>
          val row = value.asInstanceOf[Row]
          val st = dt.asInstanceOf[types.StructType]
          val structWriter = v.getWriter
          structWriter.setPosition(idx)
          structWriter.start()
          for i <- 0 until row.size do
            val childVec = v.getChildByOrdinal(i).asInstanceOf[FieldVector]
            setArrowValue(childVec, idx, row.get(i), st.fields(i).dataType)
          structWriter.end()
        case other =>
          throw UnsupportedOperationException(
            s"Unsupported Arrow vector type: ${other.getClass.getName}"
          )
