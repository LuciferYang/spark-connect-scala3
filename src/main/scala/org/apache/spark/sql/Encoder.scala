package org.apache.spark.sql

import org.apache.spark.sql.types.*
import scala.compiletime.*
import scala.deriving.Mirror

/**
 * An Encoder provides serialization and deserialization between Scala types and Spark Rows.
 *
 * In Scala 3, Encoders are derived at compile time using `Mirror.ProductOf`,
 * eliminating the need for runtime reflection (`scala.reflect.runtime.universe`).
 */
trait Encoder[T]:
  /** The Spark SQL schema for type T. */
  def schema: StructType

  /** Convert a Row to T. */
  def fromRow(row: Row): T

  /** Convert T to a Row. */
  def toRow(value: T): Row

object Encoder:

  // ---------------------------------------------------------------------------
  // Primitive Encoders
  // ---------------------------------------------------------------------------

  given Encoder[Int] with
    def schema = StructType(Seq(StructField("value", IntegerType)))
    def fromRow(row: Row) = row.getInt(0)
    def toRow(value: Int) = Row(value)

  given Encoder[Long] with
    def schema = StructType(Seq(StructField("value", LongType)))
    def fromRow(row: Row) = row.getLong(0)
    def toRow(value: Long) = Row(value)

  given Encoder[Double] with
    def schema = StructType(Seq(StructField("value", DoubleType)))
    def fromRow(row: Row) = row.getDouble(0)
    def toRow(value: Double) = Row(value)

  given Encoder[Float] with
    def schema = StructType(Seq(StructField("value", FloatType)))
    def fromRow(row: Row) = row.getFloat(0)
    def toRow(value: Float) = Row(value)

  given Encoder[Short] with
    def schema = StructType(Seq(StructField("value", ShortType)))
    def fromRow(row: Row) = row.getShort(0)
    def toRow(value: Short) = Row(value)

  given Encoder[Byte] with
    def schema = StructType(Seq(StructField("value", ByteType)))
    def fromRow(row: Row) = row.getByte(0)
    def toRow(value: Byte) = Row(value)

  given Encoder[Boolean] with
    def schema = StructType(Seq(StructField("value", BooleanType)))
    def fromRow(row: Row) = row.getBoolean(0)
    def toRow(value: Boolean) = Row(value)

  given Encoder[String] with
    def schema = StructType(Seq(StructField("value", StringType)))
    def fromRow(row: Row) = row.getString(0)
    def toRow(value: String) = Row(value)

  // ---------------------------------------------------------------------------
  // Scala type -> Spark DataType mapping (compile time)
  // ---------------------------------------------------------------------------

  /** Map a Scala type to its corresponding Spark DataType. */
  inline def sparkTypeOf[T]: DataType =
    inline erasedValue[T] match
      case _: Boolean => BooleanType
      case _: Byte    => ByteType
      case _: Short   => ShortType
      case _: Int     => IntegerType
      case _: Long    => LongType
      case _: Float   => FloatType
      case _: Double  => DoubleType
      case _: String  => StringType
      case _: BigDecimal => DecimalType.DEFAULT
      case _: java.sql.Date => DateType
      case _: java.sql.Timestamp => TimestampType
      case _: Array[Byte] => BinaryType
      case _: Option[t] => sparkTypeOf[t] // nullable wrapper
      case _: Seq[t] => ArrayType(sparkTypeOf[t], containsNull = true)
      case _: Map[k, v] => MapType(sparkTypeOf[k], sparkTypeOf[v], valueContainsNull = true)

  /** Check if a type is Option[_] at compile time. */
  inline def isOption[T]: Boolean =
    inline erasedValue[T] match
      case _: Option[_] => true
      case _            => false

  // ---------------------------------------------------------------------------
  // Product (case class) Encoder derivation via Mirror
  // ---------------------------------------------------------------------------

  /** Build field list from a product's element types and labels at compile time. */
  inline def fieldsOf[Types <: Tuple, Labels <: Tuple]: List[StructField] =
    inline (erasedValue[Types], erasedValue[Labels]) match
      case (_: EmptyTuple, _: EmptyTuple) => Nil
      case (_: (t *: ts), _: (l *: ls)) =>
        val name = constValue[l].toString
        val dt = sparkTypeOf[t]
        val nullable = isOption[t]
        StructField(name, dt, nullable = nullable || !dt.isInstanceOf[DataType]) ::
          fieldsOf[ts, ls]

  /** Extract value from Row at index, handling Option fields. */
  private def extractField(row: Row, idx: Int, isOpt: Boolean): Any =
    if isOpt then
      if row.isNullAt(idx) then None else Some(row.get(idx))
    else
      row.get(idx)

  /** Unwrap Option values for Row storage. */
  private def unwrapForRow(value: Any): Any = value match
    case None    => null
    case Some(v) => v
    case other   => other

  /** Derive an Encoder for any case class T using Scala 3 Mirror. */
  inline given derived[T](using m: Mirror.ProductOf[T]): Encoder[T] =
    val fields = fieldsOf[m.MirroredElemTypes, m.MirroredElemLabels]
    val _schema = StructType(fields)

    new Encoder[T]:
      def schema: StructType = _schema

      def fromRow(row: Row): T =
        val values = (0 until fields.size).map { i =>
          val isOpt = fields(i).nullable && fields(i).dataType != NullType
          extractField(row, i, isOpt)
        }
        val tuple = Tuple.fromArray(values.toArray)
        m.fromTuple(tuple.asInstanceOf[m.MirroredElemTypes])

      def toRow(value: T): Row =
        val product = value.asInstanceOf[Product]
        val rowValues = (0 until product.productArity).map { i =>
          unwrapForRow(product.productElement(i))
        }
        Row.fromSeq(rowValues)
