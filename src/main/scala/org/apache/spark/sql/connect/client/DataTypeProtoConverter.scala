package org.apache.spark.sql.connect.client

import org.apache.spark.connect.proto.DataType as ProtoDataType
import org.apache.spark.sql.types.*

import scala.jdk.CollectionConverters.*

/** Converts between protobuf DataType and Spark SQL DataType. */
object DataTypeProtoConverter:

  def fromProto(proto: ProtoDataType): DataType =
    import ProtoDataType.KindCase
    proto.getKindCase match
      case KindCase.BOOLEAN       => BooleanType
      case KindCase.BYTE          => ByteType
      case KindCase.SHORT         => ShortType
      case KindCase.INTEGER       => IntegerType
      case KindCase.LONG          => LongType
      case KindCase.FLOAT         => FloatType
      case KindCase.DOUBLE        => DoubleType
      case KindCase.STRING        => StringType
      case KindCase.BINARY        => BinaryType
      case KindCase.DATE          => DateType
      case KindCase.TIMESTAMP     => TimestampType
      case KindCase.TIMESTAMP_NTZ => TimestampNTZType
      case KindCase.NULL          => NullType

      case KindCase.DECIMAL =>
        val d = proto.getDecimal
        DecimalType(
          if d.hasPrecision then d.getPrecision else 10,
          if d.hasScale then d.getScale else 0
        )

      case KindCase.ARRAY =>
        val a = proto.getArray
        ArrayType(
          fromProto(a.getElementType),
          a.getContainsNull
        )

      case KindCase.MAP =>
        val m = proto.getMap
        MapType(
          fromProto(m.getKeyType),
          fromProto(m.getValueType),
          m.getValueContainsNull
        )

      case KindCase.STRUCT =>
        val s = proto.getStruct
        StructType(s.getFieldsList.asScala.map { f =>
          StructField(f.getName, fromProto(f.getDataType), f.getNullable)
        }.toSeq)

      case _ =>
        StringType // fallback for unsupported types

  /** Convert a Spark SQL DataType to its protobuf representation. */
  def toProto(dt: DataType): ProtoDataType =
    dt match
      case BooleanType =>
        ProtoDataType.newBuilder().setBoolean(ProtoDataType.Boolean.getDefaultInstance).build()
      case ByteType =>
        ProtoDataType.newBuilder().setByte(ProtoDataType.Byte.getDefaultInstance).build()
      case ShortType =>
        ProtoDataType.newBuilder().setShort(ProtoDataType.Short.getDefaultInstance).build()
      case IntegerType =>
        ProtoDataType.newBuilder().setInteger(ProtoDataType.Integer.getDefaultInstance).build()
      case LongType =>
        ProtoDataType.newBuilder().setLong(ProtoDataType.Long.getDefaultInstance).build()
      case FloatType =>
        ProtoDataType.newBuilder().setFloat(ProtoDataType.Float.getDefaultInstance).build()
      case DoubleType =>
        ProtoDataType.newBuilder().setDouble(ProtoDataType.Double.getDefaultInstance).build()
      case StringType =>
        ProtoDataType.newBuilder().setString(ProtoDataType.String.getDefaultInstance).build()
      case BinaryType =>
        ProtoDataType.newBuilder().setBinary(ProtoDataType.Binary.getDefaultInstance).build()
      case DateType =>
        ProtoDataType.newBuilder().setDate(ProtoDataType.Date.getDefaultInstance).build()
      case TimestampType =>
        ProtoDataType.newBuilder().setTimestamp(ProtoDataType.Timestamp.getDefaultInstance).build()
      case TimestampNTZType => ProtoDataType.newBuilder().setTimestampNtz(
          ProtoDataType.TimestampNTZ.getDefaultInstance
        ).build()
      case NullType =>
        ProtoDataType.newBuilder().setNull(ProtoDataType.NULL.getDefaultInstance).build()

      case DecimalType(p, s) =>
        ProtoDataType.newBuilder().setDecimal(
          ProtoDataType.Decimal.newBuilder().setPrecision(p).setScale(s).build()
        ).build()

      case ArrayType(elementType, containsNull) =>
        ProtoDataType.newBuilder().setArray(
          ProtoDataType.Array.newBuilder()
            .setElementType(toProto(elementType))
            .setContainsNull(containsNull)
            .build()
        ).build()

      case MapType(keyType, valueType, valueContainsNull) =>
        ProtoDataType.newBuilder().setMap(
          ProtoDataType.Map.newBuilder()
            .setKeyType(toProto(keyType))
            .setValueType(toProto(valueType))
            .setValueContainsNull(valueContainsNull)
            .build()
        ).build()

      case st: StructType =>
        val structBuilder = ProtoDataType.Struct.newBuilder()
        st.fields.foreach { f =>
          structBuilder.addFields(
            ProtoDataType.StructField.newBuilder()
              .setName(f.name)
              .setDataType(toProto(f.dataType))
              .setNullable(f.nullable)
              .build()
          )
        }
        ProtoDataType.newBuilder().setStruct(structBuilder.build()).build()

      case _ =>
        ProtoDataType.newBuilder().setString(
          ProtoDataType.String.getDefaultInstance
        ).build() // fallback
