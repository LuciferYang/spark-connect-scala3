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

  val empty: Row = new Row(IndexedSeq.empty)
