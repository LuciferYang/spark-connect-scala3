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

      case _ =>
        StringType // fallback for unsupported types
