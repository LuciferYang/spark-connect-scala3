package org.apache.spark.sql

import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, AgnosticEncoders}
import org.apache.spark.sql.types.*
import scala.compiletime.*
import scala.deriving.Mirror

/** An Encoder provides serialization and deserialization between Scala types and Spark Rows.
  *
  * In Scala 3, Encoders are derived at compile time using `Mirror.ProductOf`, eliminating the need
  * for runtime reflection (`scala.reflect.runtime.universe`).
  */
trait Encoder[T]:
  /** The Spark SQL schema for type T. */
  def schema: StructType

  /** Convert a Row to T. */
  def fromRow(row: Row): T

  /** Convert T to a Row. */
  def toRow(value: T): Row

  /** The corresponding AgnosticEncoder for server-side typed operations.
    *
    * Returns `null` when no AgnosticEncoder is available (e.g. for derived case class encoders
    * before ProductEncoder is implemented). Typed operations fall back to client-side
    * implementation in this case.
    */
  def agnosticEncoder: AgnosticEncoder[?] = null

object Encoder:

  // ---------------------------------------------------------------------------
  // Primitive Encoders
  // ---------------------------------------------------------------------------

  given Encoder[Int] with
    def schema = StructType(Seq(StructField("value", IntegerType)))
    def fromRow(row: Row) = row.getInt(0)
    def toRow(value: Int) = Row(value)
    override def agnosticEncoder = AgnosticEncoders.PrimitiveIntEncoder

  given Encoder[Long] with
    def schema = StructType(Seq(StructField("value", LongType)))
    def fromRow(row: Row) = row.getLong(0)
    def toRow(value: Long) = Row(value)
    override def agnosticEncoder = AgnosticEncoders.PrimitiveLongEncoder

  given Encoder[Double] with
    def schema = StructType(Seq(StructField("value", DoubleType)))
    def fromRow(row: Row) = row.getDouble(0)
    def toRow(value: Double) = Row(value)
    override def agnosticEncoder = AgnosticEncoders.PrimitiveDoubleEncoder

  given Encoder[Float] with
    def schema = StructType(Seq(StructField("value", FloatType)))
    def fromRow(row: Row) = row.getFloat(0)
    def toRow(value: Float) = Row(value)
    override def agnosticEncoder = AgnosticEncoders.PrimitiveFloatEncoder

  given Encoder[Short] with
    def schema = StructType(Seq(StructField("value", ShortType)))
    def fromRow(row: Row) = row.getShort(0)
    def toRow(value: Short) = Row(value)
    override def agnosticEncoder = AgnosticEncoders.PrimitiveShortEncoder

  given Encoder[Byte] with
    def schema = StructType(Seq(StructField("value", ByteType)))
    def fromRow(row: Row) = row.getByte(0)
    def toRow(value: Byte) = Row(value)
    override def agnosticEncoder = AgnosticEncoders.PrimitiveByteEncoder

  given Encoder[Boolean] with
    def schema = StructType(Seq(StructField("value", BooleanType)))
    def fromRow(row: Row) = row.getBoolean(0)
    def toRow(value: Boolean) = Row(value)
    override def agnosticEncoder = AgnosticEncoders.PrimitiveBooleanEncoder

  given Encoder[String] with
    def schema = StructType(Seq(StructField("value", StringType)))
    def fromRow(row: Row) = row.getString(0)
    def toRow(value: String) = Row(value)
    override def agnosticEncoder = AgnosticEncoders.StringEncoder

  // ---------------------------------------------------------------------------
  // Extended Type Encoders (Date, Timestamp, Decimal, Binary, LocalDate, Instant)
  // ---------------------------------------------------------------------------

  given Encoder[java.sql.Date] with
    def schema = StructType(Seq(StructField("value", DateType)))
    def fromRow(row: Row) = row.get(0).asInstanceOf[java.sql.Date]
    def toRow(value: java.sql.Date) = Row(value)
    override def agnosticEncoder = AgnosticEncoders.STRICT_DATE_ENCODER

  given Encoder[java.sql.Timestamp] with
    def schema = StructType(Seq(StructField("value", TimestampType)))
    def fromRow(row: Row) = row.get(0).asInstanceOf[java.sql.Timestamp]
    def toRow(value: java.sql.Timestamp) = Row(value)
    override def agnosticEncoder = AgnosticEncoders.STRICT_TIMESTAMP_ENCODER

  given Encoder[java.time.LocalDate] with
    def schema = StructType(Seq(StructField("value", DateType)))
    def fromRow(row: Row) = row.get(0).asInstanceOf[java.time.LocalDate]
    def toRow(value: java.time.LocalDate) = Row(value)
    override def agnosticEncoder = AgnosticEncoders.STRICT_LOCAL_DATE_ENCODER

  given Encoder[java.time.Instant] with
    def schema = StructType(Seq(StructField("value", TimestampType)))
    def fromRow(row: Row) = row.get(0).asInstanceOf[java.time.Instant]
    def toRow(value: java.time.Instant) = Row(value)
    override def agnosticEncoder = AgnosticEncoders.STRICT_INSTANT_ENCODER

  given Encoder[BigDecimal] with
    def schema = StructType(Seq(StructField("value", DecimalType.DEFAULT)))
    def fromRow(row: Row) = row.get(0) match
      case d: BigDecimal           => d
      case d: java.math.BigDecimal => BigDecimal(d)
      case n: Number               => BigDecimal(n.doubleValue())
      case other                   => BigDecimal(other.toString)
    def toRow(value: BigDecimal) = Row(value)
    override def agnosticEncoder = AgnosticEncoders.DEFAULT_SCALA_DECIMAL_ENCODER

  given Encoder[Array[Byte]] with
    def schema = StructType(Seq(StructField("value", BinaryType)))
    def fromRow(row: Row) = row.get(0).asInstanceOf[Array[Byte]]
    def toRow(value: Array[Byte]) = Row(value)
    override def agnosticEncoder = AgnosticEncoders.BinaryEncoder

  // ---------------------------------------------------------------------------
  // Scala type -> Spark DataType mapping (compile time)
  // ---------------------------------------------------------------------------

  /** Map a Scala type to its corresponding Spark DataType. */
  inline def sparkTypeOf[T]: DataType =
    inline erasedValue[T] match
      case _: Boolean             => BooleanType
      case _: Byte                => ByteType
      case _: Short               => ShortType
      case _: Int                 => IntegerType
      case _: Long                => LongType
      case _: Float               => FloatType
      case _: Double              => DoubleType
      case _: String              => StringType
      case _: BigDecimal          => DecimalType.DEFAULT
      case _: java.sql.Date       => DateType
      case _: java.sql.Timestamp  => TimestampType
      case _: java.time.LocalDate => DateType
      case _: java.time.Instant   => TimestampType
      case _: Array[Byte]         => BinaryType
      case _: Option[t]           => sparkTypeOf[t] // nullable wrapper
      case _: Seq[t]              => ArrayType(sparkTypeOf[t], containsNull = true)
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
      case (_: (t *: ts), _: (l *: ls))   =>
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

  /** Concrete Encoder for derived case classes (avoids anonymous class duplication at inline
    * sites).
    */
  private[sql] class DerivedEncoder[T](
      _schema: StructType,
      mirror: Mirror.ProductOf[T]
  ) extends Encoder[T]:
    def schema: StructType = _schema

    def fromRow(row: Row): T =
      val values = (0 until _schema.fields.size).map { i =>
        val isOpt = _schema.fields(i).nullable && _schema.fields(i).dataType != NullType
        extractField(row, i, isOpt)
      }
      val tuple = Tuple.fromArray(values.toArray)
      mirror.fromTuple(tuple.asInstanceOf[mirror.MirroredElemTypes])

    def toRow(value: T): Row =
      val product = value.asInstanceOf[Product]
      val rowValues = (0 until product.productArity).map { i =>
        unwrapForRow(product.productElement(i))
      }
      Row.fromSeq(rowValues)

  /** Derive an Encoder for any case class T using Scala 3 Mirror. */
  inline given derived[T](using m: Mirror.ProductOf[T]): Encoder[T] =
    val fields = fieldsOf[m.MirroredElemTypes, m.MirroredElemLabels]
    DerivedEncoder[T](StructType(fields), m)
