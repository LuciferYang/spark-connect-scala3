package org.apache.spark.sql.types

import com.fasterxml.jackson.databind.ObjectMapper

import org.apache.spark.sql.internal.JsonEscaping

import scala.collection.mutable
import scala.jdk.CollectionConverters.*

/** User metadata attached to schema fields.
  *
  * Matches Spark's public value surface: Boolean, Long, Double, String, nested Metadata, null, and
  * arrays of those scalar/nested metadata types.
  */
final class Metadata private[types] (private[types] val map: Map[String, Any])
    extends Serializable:

  def contains(key: String): Boolean = map.contains(key)
  def isEmpty: Boolean = map.isEmpty

  def getLong(key: String): Long = get(key)
  def getDouble(key: String): Double = get(key)
  def getBoolean(key: String): Boolean = get(key)
  def getString(key: String): String = get(key)
  def getMetadata(key: String): Metadata = get(key)
  def getLongArray(key: String): Array[Long] = get(key)
  def getDoubleArray(key: String): Array[Double] = get(key)
  def getBooleanArray(key: String): Array[Boolean] = get(key)
  def getStringArray(key: String): Array[String] = get(key)
  def getMetadataArray(key: String): Array[Metadata] = get(key)

  def withKeyRemoved(keyToRemove: String): Metadata =
    if map.contains(keyToRemove) then Metadata(map - keyToRemove) else this

  def withKeysRemoved(keysToRemove: Seq[String]): Metadata =
    if keysToRemove.isEmpty then this else Metadata(map -- keysToRemove)

  def json: String = Metadata.toJson(map)

  override def toString: String = json

  override def equals(other: Any): Boolean = other match
    case that: Metadata =>
      map.size == that.map.size &&
        map.keysIterator.forall(key => that.map.get(key).exists(Metadata.valueEquals(map(key), _)))
    case _ => false

  override def hashCode(): Int = Metadata.valueHash(map)

  private def get[T](key: String): T = map(key).asInstanceOf[T]

object Metadata:
  private val mapper = ObjectMapper()
  private val emptyMetadata = new Metadata(Map.empty)

  def empty: Metadata = emptyMetadata

  def fromJson(json: String): Metadata =
    val raw = mapper.readValue(json, classOf[java.util.Map[String, Object]])
    Metadata(convertMap(raw))

  private[types] def apply(map: Map[String, Any]): Metadata =
    if map.isEmpty then emptyMetadata else new Metadata(map)

  private[types] def toJson(value: Any): String =
    val sb = StringBuilder()
    appendJson(sb, value)
    sb.toString

  private def appendJson(sb: StringBuilder, value: Any): Unit = value match
    case null =>
      sb.append("null")
    case metadata: Metadata =>
      appendJson(sb, metadata.map)
    case map: Map[?, ?] =>
      sb.append('{')
      var first = true
      map.toSeq.sortBy((k, _) => String.valueOf(k)).foreach { (k, v) =>
        if first then first = false else sb.append(',')
        appendJson(sb, String.valueOf(k))
        sb.append(':')
        appendJson(sb, v)
      }
      sb.append('}')
    case arr: Array[?] =>
      sb.append('[')
      var i = 0
      while i < arr.length do
        if i > 0 then sb.append(',')
        appendJson(sb, arr(i))
        i += 1
      sb.append(']')
    case s: String =>
      sb.append('"').append(JsonEscaping.escape(s)).append('"')
    case b: Boolean =>
      sb.append(b)
    case n: Byte =>
      sb.append(n.toLong)
    case n: Short =>
      sb.append(n.toLong)
    case n: Int =>
      sb.append(n.toLong)
    case n: Long =>
      sb.append(n)
    case n: Float =>
      sb.append(n.toDouble)
    case n: Double =>
      sb.append(n)
    case other =>
      throw IllegalArgumentException(s"Unsupported metadata value type: ${other.getClass.getName}")

  private def convertObject(value: Any): Any = value match
    case null => null
    case m: java.util.Map[?, ?] =>
      Metadata(convertMap(m))
    case l: java.util.List[?] =>
      convertArray(l.asScala.toSeq)
    case b: java.lang.Boolean => b.booleanValue()
    case n: java.lang.Byte    => n.longValue()
    case n: java.lang.Short   => n.longValue()
    case n: java.lang.Integer => n.longValue()
    case n: java.lang.Long    => n.longValue()
    case n: java.lang.Float   => n.doubleValue()
    case n: java.lang.Double  => n.doubleValue()
    case s: String            => s
    case other =>
      throw IllegalArgumentException(s"Unsupported metadata JSON value: ${other.getClass.getName}")

  private def convertArray(values: Seq[Any]): Any =
    if values.isEmpty then Array.empty[Long]
    else
      convertObject(values.head) match
        case _: Long =>
          values.map(v => convertObject(v).asInstanceOf[Long]).toArray
        case _: Double =>
          values.map(v => convertObject(v).asInstanceOf[Double]).toArray
        case _: Boolean =>
          values.map(v => convertObject(v).asInstanceOf[Boolean]).toArray
        case _: String =>
          values.map(v => convertObject(v).asInstanceOf[String]).toArray
        case _: Metadata =>
          values.map(v => convertObject(v).asInstanceOf[Metadata]).toArray
        case other =>
          throw IllegalArgumentException(
            "Unsupported metadata JSON array value: " +
              Option(other).map(_.getClass.getName).orNull
          )

  private def convertMap(value: java.util.Map[?, ?]): Map[String, Any] =
    value.asScala.iterator.map { (k, v) =>
      String.valueOf(k) -> convertObject(v)
    }.toMap

  private[types] def normalizeValue(value: Any): Any = value match
    case null                 => null
    case v: Boolean          => v
    case v: Byte             => v.toLong
    case v: Short            => v.toLong
    case v: Int              => v.toLong
    case v: Long             => v
    case v: Float            => v.toDouble
    case v: Double           => v
    case v: String           => v
    case v: Metadata         => v
    case v: Array[Long]      => v
    case v: Array[Double]    => v
    case v: Array[Boolean]   => v
    case v: Array[String]    => v
    case v: Array[Metadata]  => v
    case v: Array[Int]       => v.map(_.toLong)
    case v: Array[java.lang.Long] =>
      v.map(_.longValue())
    case v: Array[java.lang.Double] =>
      v.map(_.doubleValue())
    case v: Array[java.lang.Boolean] =>
      v.map(_.booleanValue())
    case other =>
      throw IllegalArgumentException(s"Unsupported metadata value type: ${other.getClass.getName}")

  private def valueEquals(left: Any, right: Any): Boolean = (left, right) match
    case (a: Array[Long], b: Array[Long])         => java.util.Arrays.equals(a, b)
    case (a: Array[Double], b: Array[Double])     => java.util.Arrays.equals(a, b)
    case (a: Array[Boolean], b: Array[Boolean])   => java.util.Arrays.equals(a, b)
    case (a: Array[String], b: Array[String]) =>
      java.util.Arrays.equals(a.asInstanceOf[Array[Object]], b.asInstanceOf[Array[Object]])
    case (a: Array[Metadata], b: Array[Metadata]) =>
      java.util.Arrays.equals(a.asInstanceOf[Array[Object]], b.asInstanceOf[Array[Object]])
    case _                                        => left == right

  private def valueHash(value: Any): Int = value match
    case null               => 0
    case m: Map[?, ?]       => m.iterator.map((k, v) => k.hashCode() ^ valueHash(v)).sum
    case a: Array[Long]     => java.util.Arrays.hashCode(a)
    case a: Array[Double]   => java.util.Arrays.hashCode(a)
    case a: Array[Boolean]  => java.util.Arrays.hashCode(a)
    case a: Array[String]   => java.util.Arrays.hashCode(a.asInstanceOf[Array[Object]])
    case a: Array[Metadata] => java.util.Arrays.hashCode(a.asInstanceOf[Array[Object]])
    case other              => other.hashCode()

final class MetadataBuilder:
  private val map = mutable.LinkedHashMap.empty[String, Any]

  def withMetadata(metadata: Metadata): this.type =
    map ++= metadata.map
    this

  def putNull(key: String): this.type = put(key, null)
  def putLong(key: String, value: Long): this.type = put(key, value)
  def putDouble(key: String, value: Double): this.type = put(key, value)
  def putBoolean(key: String, value: Boolean): this.type = put(key, value)
  def putString(key: String, value: String): this.type = put(key, value)
  def putMetadata(key: String, value: Metadata): this.type = put(key, value)
  def putLongArray(key: String, value: Array[Long]): this.type = put(key, value)
  def putDoubleArray(key: String, value: Array[Double]): this.type = put(key, value)
  def putBooleanArray(key: String, value: Array[Boolean]): this.type = put(key, value)
  def putStringArray(key: String, value: Array[String]): this.type = put(key, value)
  def putMetadataArray(key: String, value: Array[Metadata]): this.type = put(key, value)

  def remove(key: String): this.type =
    map.remove(key)
    this

  def build(): Metadata = Metadata(map.toMap)

  private def put(key: String, value: Any): this.type =
    map.put(key, Metadata.normalizeValue(value))
    this
