package org.apache.spark.sql.connect.client

import org.apache.spark.connect.proto.types.{DataType as ProtoDataType}
import org.apache.spark.sql.types.*

/** Converts between protobuf DataType and Spark SQL DataType. */
object DataTypeProtoConverter:

  def fromProto(proto: ProtoDataType): DataType =
    import ProtoDataType.Kind
    proto.kind match
      case Kind.Boolean(_)      => BooleanType
      case Kind.Byte(_)         => ByteType
      case Kind.Short(_)        => ShortType
      case Kind.Integer(_)      => IntegerType
      case Kind.Long(_)         => LongType
      case Kind.Float(_)        => FloatType
      case Kind.Double(_)       => DoubleType
      case Kind.String(_)       => StringType
      case Kind.Binary(_)       => BinaryType
      case Kind.Date(_)         => DateType
      case Kind.Timestamp(_)    => TimestampType
      case Kind.TimestampNtz(_) => TimestampNTZType
      case Kind.Null(_)         => NullType

      case Kind.Decimal(d) =>
        DecimalType(
          d.precision.getOrElse(10),
          d.scale.getOrElse(0)
        )

      case Kind.Array(a) =>
        ArrayType(
          fromProto(a.elementType.get),
          a.containsNull
        )

      case Kind.Map(m) =>
        MapType(
          fromProto(m.keyType.get),
          fromProto(m.valueType.get),
          m.valueContainsNull
        )

      case Kind.Struct(s) =>
        StructType(s.fields.map { f =>
          StructField(f.name, fromProto(f.dataType.get), f.nullable)
        })

      case Kind.Null(_)         => NullType

      case _ =>
        StringType // fallback for unsupported types

  /** Convert a Spark SQL DataType to its protobuf representation. */
  def toProto(dt: DataType): ProtoDataType =
    import ProtoDataType.Kind
    dt match
      case BooleanType   => ProtoDataType(kind = Kind.Boolean(ProtoDataType.Boolean()))
      case ByteType      => ProtoDataType(kind = Kind.Byte(ProtoDataType.Byte()))
      case ShortType     => ProtoDataType(kind = Kind.Short(ProtoDataType.Short()))
      case IntegerType   => ProtoDataType(kind = Kind.Integer(ProtoDataType.Integer()))
      case LongType      => ProtoDataType(kind = Kind.Long(ProtoDataType.Long()))
      case FloatType     => ProtoDataType(kind = Kind.Float(ProtoDataType.Float()))
      case DoubleType    => ProtoDataType(kind = Kind.Double(ProtoDataType.Double()))
      case StringType    => ProtoDataType(kind = Kind.String(ProtoDataType.String()))
      case BinaryType    => ProtoDataType(kind = Kind.Binary(ProtoDataType.Binary()))
      case DateType      => ProtoDataType(kind = Kind.Date(ProtoDataType.Date()))
      case TimestampType => ProtoDataType(kind = Kind.Timestamp(ProtoDataType.Timestamp()))
      case TimestampNTZType => ProtoDataType(kind = Kind.TimestampNtz(ProtoDataType.TimestampNTZ()))
      case NullType      => ProtoDataType(kind = Kind.Null(ProtoDataType.NULL()))

      case DecimalType(p, s) =>
        ProtoDataType(kind = Kind.Decimal(
          ProtoDataType.Decimal(precision = Some(p), scale = Some(s))
        ))

      case ArrayType(elementType, containsNull) =>
        ProtoDataType(kind = Kind.Array(
          ProtoDataType.Array(elementType = Some(toProto(elementType)), containsNull = containsNull)
        ))

      case MapType(keyType, valueType, valueContainsNull) =>
        ProtoDataType(kind = Kind.Map(
          ProtoDataType.Map(
            keyType = Some(toProto(keyType)),
            valueType = Some(toProto(valueType)),
            valueContainsNull = valueContainsNull
          )
        ))

      case st: StructType =>
        val protoFields = st.fields.map { f =>
          ProtoDataType.StructField(
            name = f.name,
            dataType = Some(toProto(f.dataType)),
            nullable = f.nullable
          )
        }
        ProtoDataType(kind = Kind.Struct(ProtoDataType.Struct(fields = protoFields.toSeq)))

      case _ =>
        ProtoDataType(kind = Kind.String(ProtoDataType.String())) // fallback
