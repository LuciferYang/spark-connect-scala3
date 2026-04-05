package org.apache.spark.sql.connect.common

import org.apache.spark.connect.proto.{DataType as ProtoDataType, Expression}
import org.apache.spark.sql.Row
import org.apache.spark.sql.connect.client.DataTypeProtoConverter
import org.apache.spark.sql.types.*

import java.time.{Instant, LocalDate, LocalDateTime}
import scala.jdk.CollectionConverters.*

/** Converts proto Expression.Literal values to Scala values and DataTypes.
  *
  * Simplified version for SC3 — supports the types commonly seen in ObservedMetrics responses.
  */
object LiteralValueProtoConverter:

  /** Convert a proto Literal to a Scala value. */
  def toScalaValue(literal: Expression.Literal): Any =
    import Expression.Literal.LiteralTypeCase
    if literal.hasNull then return null

    literal.getLiteralTypeCase match
      case LiteralTypeCase.BOOLEAN   => literal.getBoolean
      case LiteralTypeCase.BYTE      => literal.getByte.toByte
      case LiteralTypeCase.SHORT     => literal.getShort.toShort
      case LiteralTypeCase.INTEGER   => literal.getInteger
      case LiteralTypeCase.LONG      => literal.getLong
      case LiteralTypeCase.FLOAT     => literal.getFloat
      case LiteralTypeCase.DOUBLE    => literal.getDouble
      case LiteralTypeCase.STRING    => literal.getString
      case LiteralTypeCase.BINARY    => literal.getBinary.toByteArray
      case LiteralTypeCase.DATE      => LocalDate.ofEpochDay(literal.getDate.toLong)
      case LiteralTypeCase.TIMESTAMP => Instant.ofEpochSecond(0, literal.getTimestamp * 1000)
      case LiteralTypeCase.TIMESTAMP_NTZ =>
        val micros = literal.getTimestampNtz
        val secs = Math.floorDiv(micros, 1000000L)
        val nos = Math.floorMod(micros, 1000000L) * 1000
        LocalDateTime.ofEpochSecond(secs, nos.toInt, java.time.ZoneOffset.UTC)
      case LiteralTypeCase.DECIMAL =>
        val d = literal.getDecimal
        BigDecimal(d.getValue)
      case LiteralTypeCase.ARRAY =>
        val arr = literal.getArray
        arr.getElementsList.asScala.map(toScalaValue).toArray
      case LiteralTypeCase.MAP =>
        val m = literal.getMap
        val keys = m.getKeysList.asScala.map(toScalaValue)
        val vals = m.getValuesList.asScala.map(toScalaValue)
        keys.zip(vals).toMap
      case LiteralTypeCase.STRUCT =>
        val s = literal.getStruct
        val values = s.getElementsList.asScala.map(toScalaValue).toSeq
        Row.fromSeq(values)
      case LiteralTypeCase.LITERALTYPE_NOT_SET =>
        null
      case other =>
        throw UnsupportedOperationException(
          s"Unsupported literal type: $other"
        )

  /** Infer the DataType of a proto Literal. */
  def toDataType(literal: Expression.Literal): DataType =
    import Expression.Literal.LiteralTypeCase
    if literal.hasDataType then return DataTypeProtoConverter.fromProto(literal.getDataType)
    if literal.hasNull then
      return if literal.getNull.getKindCase != ProtoDataType.KindCase.KIND_NOT_SET then
        DataTypeProtoConverter.fromProto(literal.getNull)
      else NullType

    literal.getLiteralTypeCase match
      case LiteralTypeCase.BOOLEAN       => BooleanType
      case LiteralTypeCase.BYTE          => ByteType
      case LiteralTypeCase.SHORT         => ShortType
      case LiteralTypeCase.INTEGER       => IntegerType
      case LiteralTypeCase.LONG          => LongType
      case LiteralTypeCase.FLOAT         => FloatType
      case LiteralTypeCase.DOUBLE        => DoubleType
      case LiteralTypeCase.STRING        => StringType
      case LiteralTypeCase.BINARY        => BinaryType
      case LiteralTypeCase.DATE          => DateType
      case LiteralTypeCase.TIMESTAMP     => TimestampType
      case LiteralTypeCase.TIMESTAMP_NTZ => TimestampNTZType
      case LiteralTypeCase.DECIMAL =>
        val d = literal.getDecimal
        DecimalType(
          if d.hasPrecision then d.getPrecision else 10,
          if d.hasScale then d.getScale else 0
        )
      case LiteralTypeCase.ARRAY =>
        val arr = literal.getArray
        if arr.hasElementType then
          ArrayType(DataTypeProtoConverter.fromProto(arr.getElementType), containsNull = true)
        else if arr.getElementsCount > 0 then
          ArrayType(toDataType(arr.getElements(0)), containsNull = true)
        else ArrayType(NullType, containsNull = true)
      case LiteralTypeCase.MAP =>
        val m = literal.getMap
        if m.hasKeyType && m.hasValueType then
          MapType(
            DataTypeProtoConverter.fromProto(m.getKeyType),
            DataTypeProtoConverter.fromProto(m.getValueType),
            valueContainsNull = true
          )
        else if m.getKeysCount > 0 then
          MapType(
            toDataType(m.getKeys(0)),
            toDataType(m.getValues(0)),
            valueContainsNull = true
          )
        else MapType(NullType, NullType, valueContainsNull = true)
      case LiteralTypeCase.STRUCT =>
        val s = literal.getStruct
        if s.hasStructType then DataTypeProtoConverter.fromProto(s.getStructType)
        else
          StructType(s.getElementsList.asScala.zipWithIndex.map { (elem, i) =>
            StructField(s"col$i", toDataType(elem))
          }.toSeq)
      case _ => NullType
