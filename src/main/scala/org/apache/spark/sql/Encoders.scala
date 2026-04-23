package org.apache.spark.sql

import scala.deriving.Mirror
import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.*
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.UserDefinedType

/** Factory methods for creating [[Encoder]] instances.
  *
  * Users reference these methods inside `Aggregator.bufferEncoder` / `outputEncoder`:
  * {{{
  *   def bufferEncoder: Encoder[Long] = Encoders.scalaLong
  *   def outputEncoder: Encoder[Long] = Encoders.scalaLong
  * }}}
  *
  * Every method returns an `Encoder[T]` that wraps an `AgnosticEncoder` instance with a correct
  * `writeReplace` proxy, enabling cross-Scala-3/2.13 serialization.
  */
object Encoders:

  /** Wraps an `AgnosticEncoder` in the `Encoder[T]` trait.
    *
    * The `schema`/`fromRow`/`toRow` methods delegate to the wrapped encoder's `dataType`. The
    * critical method is `agnosticEncoder` which returns the underlying `AgnosticEncoder` used for
    * UDF/UDAF serialization.
    */
  private class AgnosticEncoderWrapper[T](val underlying: AgnosticEncoder[T]) extends Encoder[T]:
    def schema: StructType = StructType(Seq(StructField("value", underlying.dataType)))
    def fromRow(row: Row): T =
      throw UnsupportedOperationException("AgnosticEncoderWrapper.fromRow is not supported")
    def toRow(value: T): Row =
      throw UnsupportedOperationException("AgnosticEncoderWrapper.toRow is not supported")
    override def agnosticEncoder: AgnosticEncoder[?] = underlying

  private def wrap[T](ae: AgnosticEncoder[T]): Encoder[T] = AgnosticEncoderWrapper(ae)

  /** Tuple2 encoder that properly implements fromRow/toRow for joinWith results.
    *
    * The server returns rows with two struct columns (`_1`, `_2`). Each struct is deserialized as a
    * nested Row, then passed through the element encoder's `fromRow`.
    */
  private class TupleEncoder2[T1, T2](
      e1: Encoder[T1],
      e2: Encoder[T2],
      ae: AgnosticEncoder[(T1, T2)]
  ) extends Encoder[(T1, T2)]:
    def schema: StructType = StructType(
      Seq(
        StructField("_1", e1.schema),
        StructField("_2", e2.schema)
      )
    )
    def fromRow(row: Row): (T1, T2) =
      val v1 = row.get(0) match
        case r: Row => e1.fromRow(r)
        case other  => other.asInstanceOf[T1]
      val v2 = row.get(1) match
        case r: Row => e2.fromRow(r)
        case other  => other.asInstanceOf[T2]
      (v1, v2)
    def toRow(value: (T1, T2)): Row =
      Row(e1.toRow(value._1), e2.toRow(value._2))
    override def agnosticEncoder: AgnosticEncoder[?] = ae

  // -- Scala primitive types ------------------------------------------------

  def scalaBoolean: Encoder[Boolean] = wrap(PrimitiveBooleanEncoder)
  def scalaByte: Encoder[Byte] = wrap(PrimitiveByteEncoder)
  def scalaShort: Encoder[Short] = wrap(PrimitiveShortEncoder)
  def scalaInt: Encoder[Int] = wrap(PrimitiveIntEncoder)
  def scalaLong: Encoder[Long] = wrap(PrimitiveLongEncoder)
  def scalaFloat: Encoder[Float] = wrap(PrimitiveFloatEncoder)
  def scalaDouble: Encoder[Double] = wrap(PrimitiveDoubleEncoder)

  // -- Java boxed types -----------------------------------------------------

  def BOOLEAN: Encoder[java.lang.Boolean] = wrap(BoxedBooleanEncoder)
  def BYTE: Encoder[java.lang.Byte] = wrap(BoxedByteEncoder)
  def SHORT: Encoder[java.lang.Short] = wrap(BoxedShortEncoder)
  def INT: Encoder[java.lang.Integer] = wrap(BoxedIntEncoder)
  def LONG: Encoder[java.lang.Long] = wrap(BoxedLongEncoder)
  def FLOAT: Encoder[java.lang.Float] = wrap(BoxedFloatEncoder)
  def DOUBLE: Encoder[java.lang.Double] = wrap(BoxedDoubleEncoder)

  // -- String / Binary ------------------------------------------------------

  def STRING: Encoder[String] = wrap(StringEncoder)
  def BINARY: Encoder[Array[Byte]] = wrap(BinaryEncoder)

  // -- Date / Time / Decimal ------------------------------------------------

  def DATE: Encoder[java.sql.Date] = wrap(AgnosticEncoders.STRICT_DATE_ENCODER)
  def LOCALDATE: Encoder[java.time.LocalDate] = wrap(AgnosticEncoders.STRICT_LOCAL_DATE_ENCODER)
  def TIMESTAMP: Encoder[java.sql.Timestamp] = wrap(AgnosticEncoders.STRICT_TIMESTAMP_ENCODER)
  def INSTANT: Encoder[java.time.Instant] = wrap(AgnosticEncoders.STRICT_INSTANT_ENCODER)
  def LOCALDATETIME: Encoder[java.time.LocalDateTime] = wrap(LocalDateTimeEncoder)
  def DECIMAL: Encoder[java.math.BigDecimal] = wrap(AgnosticEncoders.DEFAULT_JAVA_DECIMAL_ENCODER)

  // -- Interval types ---------------------------------------------------------

  def DURATION: Encoder[java.time.Duration] = wrap(AgnosticEncoders.DayTimeIntervalEncoder)
  def PERIOD: Encoder[java.time.Period] = wrap(AgnosticEncoders.YearMonthIntervalEncoder)

  // -- Char / Varchar / Time types -------------------------------------------

  def CHAR(length: Int): Encoder[String] = wrap(AgnosticEncoders.CharEncoder(length))
  def VARCHAR(length: Int): Encoder[String] = wrap(AgnosticEncoders.VarcharEncoder(length))
  def LOCALTIME: Encoder[java.time.LocalTime] = wrap(AgnosticEncoders.LocalTimeEncoder)

  // -- Spatial types -----------------------------------------------------------

  def GEOMETRY: Encoder[org.apache.spark.sql.types.Geometry] =
    wrap(AgnosticEncoders.GeometryEncoder(org.apache.spark.sql.types.GeometryType()))
  def GEOGRAPHY: Encoder[org.apache.spark.sql.types.Geography] =
    wrap(AgnosticEncoders.GeographyEncoder(org.apache.spark.sql.types.GeographyType()))

  // -- User-defined type encoder -----------------------------------------------

  def udt[T >: Null](tpe: UserDefinedType[T]): Encoder[T] =
    wrap(AgnosticEncoders.UDTEncoder(tpe))

  // -- Row encoder ------------------------------------------------------------

  def row: Encoder[Row] = wrap(AgnosticEncoders.UnboundRowEncoder)

  // -- Tuple encoders -------------------------------------------------------

  def tuple[T1](e1: Encoder[T1]): Encoder[Tuple1[T1]] =
    wrap(tupleProductEncoder[Tuple1[T1]](asAgnostic(e1)))

  def tuple[T1, T2](
      e1: Encoder[T1],
      e2: Encoder[T2]
  ): Encoder[(T1, T2)] =
    val ae = tupleProductEncoder[(T1, T2)](asAgnostic(e1), asAgnostic(e2))
    new TupleEncoder2(e1, e2, ae)

  def tuple[T1, T2, T3](
      e1: Encoder[T1],
      e2: Encoder[T2],
      e3: Encoder[T3]
  ): Encoder[(T1, T2, T3)] =
    wrap(tupleProductEncoder[(T1, T2, T3)](asAgnostic(e1), asAgnostic(e2), asAgnostic(e3)))

  def tuple[T1, T2, T3, T4](
      e1: Encoder[T1],
      e2: Encoder[T2],
      e3: Encoder[T3],
      e4: Encoder[T4]
  ): Encoder[(T1, T2, T3, T4)] =
    wrap(
      tupleProductEncoder[(T1, T2, T3, T4)](
        asAgnostic(e1),
        asAgnostic(e2),
        asAgnostic(e3),
        asAgnostic(e4)
      )
    )

  def tuple[T1, T2, T3, T4, T5](
      e1: Encoder[T1],
      e2: Encoder[T2],
      e3: Encoder[T3],
      e4: Encoder[T4],
      e5: Encoder[T5]
  ): Encoder[(T1, T2, T3, T4, T5)] =
    wrap(
      tupleProductEncoder[(T1, T2, T3, T4, T5)](
        asAgnostic(e1),
        asAgnostic(e2),
        asAgnostic(e3),
        asAgnostic(e4),
        asAgnostic(e5)
      )
    )

  // -- Case class encoder ---------------------------------------------------

  /** Derive an encoder for a case class T.
    *
    * Delegates to `Encoder.derived` which uses Scala 3 `Mirror.ProductOf`.
    */
  inline def product[T <: Product](using Mirror.ProductOf[T], ClassTag[T]): Encoder[T] =
    Encoder.derived[T]

  // -- Internal helpers -----------------------------------------------------

  private[sql] def asAgnostic[T](enc: Encoder[T]): AgnosticEncoder[?] =
    enc match
      case w: AgnosticEncoderWrapper[?] => w.underlying
      case ae: AgnosticEncoder[?]       => ae
      case other                        =>
        val ae = other.agnosticEncoder
        require(ae != null, s"Encoder does not provide AgnosticEncoder: $other")
        ae

  private def tupleProductEncoder[T: ClassTag](encoders: AgnosticEncoder[?]*): ProductEncoder[T] =
    val fields = encoders.zipWithIndex.map { (enc, i) =>
      EncoderField(s"_${i + 1}", enc, enc.nullable, Metadata.empty)
    }.toSeq
    ProductEncoder[T](summon[ClassTag[T]], fields)
