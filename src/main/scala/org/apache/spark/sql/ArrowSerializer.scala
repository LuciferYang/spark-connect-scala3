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
  TinyIntVector,
  VarBinaryVector,
  VarCharVector,
  VectorSchemaRoot
}
import org.apache.arrow.vector.complex.{ListVector, StructVector}
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
      val arrowFields =
        schema.fields.map(f => sparkTypeToArrowField(f.name, f.dataType, f.nullable)).toList.asJava
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
    case types.BooleanType   => ArrowType.Bool.INSTANCE
    case types.ByteType      => new ArrowType.Int(8, true)
    case types.ShortType     => new ArrowType.Int(16, true)
    case types.IntegerType   => new ArrowType.Int(32, true)
    case types.LongType      => new ArrowType.Int(64, true)
    case types.FloatType     => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    case types.DoubleType    => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    case types.StringType    => ArrowType.Utf8.INSTANCE
    case types.DateType      => new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)
    case types.TimestampType =>
      new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "UTC")
    case d: types.DecimalType => new ArrowType.Decimal(d.precision, d.scale, 128)
    case types.BinaryType     => ArrowType.Binary.INSTANCE
    case _: types.ArrayType   => ArrowType.List.INSTANCE
    case _: types.StructType  => ArrowType.Struct.INSTANCE
    case _: types.MapType     => new ArrowType.Map(false)
    case _                    => ArrowType.Utf8.INSTANCE // fallback

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

  @scala.annotation.nowarn("msg=unused explicit parameter")
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
        case v: VarCharVector  => v.setSafe(idx, value.toString.getBytes("UTF-8"))
        case v: DateDayVector  =>
          val epochDay = value match
            case d: java.sql.Date        => d.toLocalDate.toEpochDay.toInt
            case ld: java.time.LocalDate => ld.toEpochDay.toInt
            case n: Number               => n.intValue()
            case _                       => value.toString.toInt
          v.setSafe(idx, epochDay)
        case v: TimeStampMicroTZVector =>
          val micros = value match
            case ts: java.sql.Timestamp =>
              ts.getTime * 1000 + (ts.getNanos / 1000) % 1000
            case inst: java.time.Instant =>
              inst.getEpochSecond * 1_000_000 + inst.getNano / 1000
            case n: Number => n.longValue()
            case _         => value.toString.toLong
          v.setSafe(idx, micros)
        case v: DecimalVector =>
          val bd = value match
            case d: BigDecimal           => d.underlying()
            case d: java.math.BigDecimal => d
            case _                       => java.math.BigDecimal(value.toString)
          v.setSafe(idx, bd)
        case v: VarBinaryVector =>
          val bytes = value.asInstanceOf[Array[Byte]]
          v.setSafe(idx, bytes, 0, bytes.length)
        case v: ListVector =>
          val listWriter = v.getWriter
          val items = value match
            case a: Array[?]    => a.toSeq
            case s: Iterable[?] => s.toSeq
            case _              => Seq(value)
          listWriter.setPosition(idx)
          listWriter.startList()
          for item <- items do
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
                  val bytes = s.getBytes("UTF-8")
                  val buf = v.getAllocator.buffer(bytes.length)
                  buf.writeBytes(bytes)
                  listWriter.varChar().writeVarChar(0, bytes.length, buf)
                  buf.close()
                case _ => listWriter.writeNull()
          listWriter.endList()
        case v: StructVector =>
          val row = value.asInstanceOf[Row]
          val structWriter = v.getWriter
          structWriter.setPosition(idx)
          structWriter.start()
          for i <- 0 until row.size do
            val childVec = v.getChildByOrdinal(i).asInstanceOf[FieldVector]
            setArrowValue(childVec, idx, row.get(i), types.StringType)
          structWriter.end()
        case _ => () // unsupported type
