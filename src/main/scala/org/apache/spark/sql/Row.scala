package org.apache.spark.sql

import org.apache.spark.sql.types.StructType

/** A single row of output from a relational operator. */
final class Row private (
    private val values: IndexedSeq[Any],
    val schema: Option[StructType] = None
):

  def size: Int = values.size

  def length: Int = size

  def get(i: Int): Any = values(i)

  def isNullAt(i: Int): Boolean = get(i) == null

  def getBoolean(i: Int): Boolean = get(i).asInstanceOf[Boolean]

  def getByte(i: Int): Byte = get(i).asInstanceOf[Number].byteValue()

  def getShort(i: Int): Short = get(i).asInstanceOf[Number].shortValue()

  def getInt(i: Int): Int = get(i).asInstanceOf[Number].intValue()

  def getLong(i: Int): Long = get(i).asInstanceOf[Number].longValue()

  def getFloat(i: Int): Float = get(i).asInstanceOf[Number].floatValue()

  def getDouble(i: Int): Double = get(i).asInstanceOf[Number].doubleValue()

  def getString(i: Int): String = get(i).asInstanceOf[String]

  def getAs[T](i: Int): T = get(i).asInstanceOf[T]

  def getAs[T](fieldName: String): T =
    schema match
      case Some(s) => get(s.fieldIndex(fieldName)).asInstanceOf[T]
      case None    =>
        throw UnsupportedOperationException("getAs by field name requires a Row with schema")

  def getDecimal(i: Int): java.math.BigDecimal = get(i) match
    case d: java.math.BigDecimal => d
    case d: BigDecimal           => d.underlying
    case n: Number               => java.math.BigDecimal.valueOf(n.doubleValue())

  def getDate(i: Int): java.sql.Date = get(i).asInstanceOf[java.sql.Date]

  def getTimestamp(i: Int): java.sql.Timestamp = get(i).asInstanceOf[java.sql.Timestamp]

  def getInstant(i: Int): java.time.Instant = get(i).asInstanceOf[java.time.Instant]

  def getLocalDate(i: Int): java.time.LocalDate = get(i).asInstanceOf[java.time.LocalDate]

  def getSeq[T](i: Int): Seq[T] = get(i).asInstanceOf[Seq[T]]

  def getList[T](i: Int): java.util.List[T] =
    import scala.jdk.CollectionConverters.*
    getSeq[T](i).asJava

  def getMap[K, V](i: Int): Map[K, V] = get(i).asInstanceOf[Map[K, V]]

  def getJavaMap[K, V](i: Int): java.util.Map[K, V] =
    import scala.jdk.CollectionConverters.*
    getMap[K, V](i).asJava

  def getStruct(i: Int): Row = get(i).asInstanceOf[Row]

  def fieldIndex(name: String): Int =
    schema match
      case Some(s) => s.fieldIndex(name)
      case None    =>
        throw UnsupportedOperationException("fieldIndex requires a Row with schema")

  def anyNull: Boolean = (0 until size).exists(isNullAt)

  def json: String =
    schema match
      case Some(s) =>
        val fields = s.fields.zipWithIndex.map { (f, i) =>
          val v =
            if isNullAt(i) then "null"
            else
              get(i) match
                case s: String => s"\"$s\""
                case other     => other.toString
          s"\"${f.name}\":$v"
        }
        s"{${fields.mkString(",")}}"
      case None =>
        throw UnsupportedOperationException("json requires a Row with schema")

  /** Alias for `json`. */
  def toJson: String = json

  def prettyJson: String =
    schema match
      case Some(s) =>
        val fields = s.fields.zipWithIndex.map { (f, i) =>
          val v =
            if isNullAt(i) then "null"
            else
              get(i) match
                case s: String => s"\"$s\""
                case other     => other.toString
          s"  \"${f.name}\" : $v"
        }
        s"{\n${fields.mkString(",\n")}\n}"
      case None =>
        throw UnsupportedOperationException("prettyJson requires a Row with schema")

  def copy(): Row = Row.fromSeq(toSeq)

  /** Get values for the given field names as a Map.
    *
    * Requires that this Row was created with a schema.
    */
  def getValuesMap[T](fieldNames: Seq[String]): Map[String, T] =
    schema match
      case Some(s) =>
        fieldNames.map { name =>
          name -> get(s.fieldIndex(name)).asInstanceOf[T]
        }.toMap
      case None =>
        throw UnsupportedOperationException(
          "getValuesMap requires a Row with schema"
        )

  def toSeq: Seq[Any] = values.toSeq

  def mkString(sep: String): String =
    values.map(v => if v == null then "null" else v.toString).mkString(sep)

  override def toString: String =
    values.map(v => if v == null then "null" else v.toString).mkString("[", ",", "]")

  override def equals(other: Any): Boolean = other match
    case that: Row => this.values == that.values
    case _         => false

  override def hashCode(): Int = values.hashCode()

object Row:
  def fromSeq(values: Seq[Any]): Row = new Row(values.toIndexedSeq)

  def fromSeqWithSchema(values: Seq[Any], schema: StructType): Row =
    new Row(values.toIndexedSeq, Some(schema))

  def apply(values: Any*): Row = fromSeq(values)

  def fromTuple(t: Product): Row = fromSeq(t.productIterator.toSeq)

  val empty: Row = new Row(IndexedSeq.empty)

  /** Pattern-matching extractor for Row values.
    *
    * {{{
    *   row match { case Row(x, y, z) => ... }
    * }}}
    */
  def unapplySeq(row: Row): Some[Seq[Any]] = Some(row.toSeq)

  /** Merge multiple rows into one by concatenating their fields.
    *
    * The resulting Row has no schema.
    */
  @deprecated("This method is deprecated and will be removed in future versions.", "3.0.0")
  def merge(rows: Row*): Row =
    fromSeq(rows.flatMap(_.toSeq))
