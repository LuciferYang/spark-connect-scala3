package org.apache.spark.sql.catalyst.encoders

import scala.collection.immutable.ArraySeq
import scala.reflect.ClassTag

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.*
import org.apache.spark.sql.types.*

/** Companion to [[AgnosticEncoders.RowEncoder]] providing the schema-driven factory used by
  * `Encoders.row(schema)`.
  *
  * Mirrors upstream Spark's
  * `org.apache.spark.sql.catalyst.encoders.RowEncoder.encoderFor(StructType)`. The mapping from
  * [[DataType]] to [[AgnosticEncoder]] is intentionally aligned with upstream so that a schema
  * round-tripped from the server produces the same encoder graph the server itself would build.
  */
object RowEncoder:

  /** Build an [[AgnosticEncoder]][Row] for the given [[StructType]] schema. */
  def encoderFor(schema: StructType): AgnosticEncoder[Row] =
    encoderFor(schema, lenient = false)

  /** Build an [[AgnosticEncoder]][Row] for the given schema. When `lenient = true`, primitive Date
    * / Timestamp / Instant / LocalDate encoders accept slightly looser input shapes (matching
    * upstream `RowEncoder.encoderFor(schema, lenient = true)`).
    */
  def encoderFor(schema: StructType, lenient: Boolean): AgnosticEncoder[Row] =
    encoderForDataType(schema, lenient).asInstanceOf[AgnosticEncoder[Row]]

  private[sql] def encoderForDataType(
      dataType: DataType,
      lenient: Boolean
  ): AgnosticEncoder[?] = dataType match
    case NullType                             => NullEncoder
    case BooleanType                          => BoxedBooleanEncoder
    case ByteType                             => BoxedByteEncoder
    case ShortType                            => BoxedShortEncoder
    case IntegerType                          => BoxedIntEncoder
    case LongType                             => BoxedLongEncoder
    case FloatType                            => BoxedFloatEncoder
    case DoubleType                           => BoxedDoubleEncoder
    case dt: DecimalType                      => JavaDecimalEncoder(dt, lenient = true)
    case BinaryType                           => BinaryEncoder
    case CharType(length)                     => CharEncoder(length)
    case VarcharType(length)                  => VarcharEncoder(length)
    case StringType                           => StringEncoder
    case TimestampType                        => TimestampEncoder(lenient)
    case TimestampNTZType                     => LocalDateTimeEncoder
    case DateType                             => DateEncoder(lenient)
    case _: TimeType                          => LocalTimeEncoder
    case DayTimeIntervalType                  => DayTimeIntervalEncoder
    case YearMonthIntervalType                => YearMonthIntervalEncoder
    case VariantType                          => VariantEncoder
    case udt: UserDefinedType[?]              => UDTEncoder(udt.asInstanceOf[UserDefinedType[Null]])
    case ArrayType(elementType, containsNull) =>
      IterableEncoder[ArraySeq[Any], Any](
        ClassTag(classOf[ArraySeq[?]]),
        encoderForDataType(elementType, lenient).asInstanceOf[AgnosticEncoder[Any]],
        containsNull
      )
    case MapType(keyType, valueType, valueContainsNull) =>
      MapEncoder[scala.collection.Map[Any, Any], Any, Any](
        ClassTag(classOf[scala.collection.Map[?, ?]]),
        encoderForDataType(keyType, lenient).asInstanceOf[AgnosticEncoder[Any]],
        encoderForDataType(valueType, lenient).asInstanceOf[AgnosticEncoder[Any]],
        valueContainsNull
      )
    case StructType(fields) =>
      AgnosticEncoders.RowEncoder(
        fields.map { field =>
          EncoderField(
            field.name,
            encoderForDataType(field.dataType, lenient),
            field.nullable,
            Metadata.empty
          )
        }.toIndexedSeq
      )
    case g: GeographyType => GeographyEncoder(g)
    case g: GeometryType  => GeometryEncoder(g)
    case other            =>
      throw UnsupportedOperationException(
        s"Cannot create encoder for unsupported data type: ${other.simpleString}"
      )
