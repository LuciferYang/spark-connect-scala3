package org.apache.spark.sql

import org.apache.spark.sql.types.StructType

/** A single row of output from a relational operator.
  *
  * Rows produced by `collect()` carry an inferred [[StructType]] schema; rows constructed via
  * [[Row.apply]] / [[Row.fromSeq]] do not. Methods that look up fields by name (`getAs(fieldName)`,
  * `fieldIndex`, `getValuesMap`, `json`, `prettyJson`) require a schema and throw
  * [[UnsupportedOperationException]] otherwise.
  */
final class Row private (
    private val values: IndexedSeq[Any],
    val schema: Option[StructType] = None
) extends Serializable:

  /** Number of fields in this row. */
  def size: Int = values.size

  /** Alias for [[size]]. */
  def length: Int = size

  /** Return the raw value at position `i`. May be `null`. */
  def get(i: Int): Any = values(i)

  /** True iff the value at position `i` is `null`. */
  def isNullAt(i: Int): Boolean = get(i) == null

  /** Primitive accessors all reject `null` with a `NullPointerException` carrying the field index,
    * matching upstream Spark's `getAnyValAs` contract. Without the guard, `getBoolean` silently
    * unboxes `null` to `false` (Scala default for primitive cast) — indistinguishable from a real
    * `false` — and the numeric accessors would NPE inside the `Number` cast with no useful field
    * context. Use [[isNullAt]] or [[get]] for nullable fields.
    */
  private def requireNotNull(i: Int): Unit =
    if isNullAt(i) then throw NullPointerException(s"Value at index $i is null")

  def getBoolean(i: Int): Boolean =
    requireNotNull(i)
    get(i).asInstanceOf[Boolean]

  def getByte(i: Int): Byte =
    requireNotNull(i)
    get(i).asInstanceOf[Number].byteValue()

  def getShort(i: Int): Short =
    requireNotNull(i)
    get(i).asInstanceOf[Number].shortValue()

  def getInt(i: Int): Int =
    requireNotNull(i)
    get(i).asInstanceOf[Number].intValue()

  def getLong(i: Int): Long =
    requireNotNull(i)
    get(i).asInstanceOf[Number].longValue()

  def getFloat(i: Int): Float =
    requireNotNull(i)
    get(i).asInstanceOf[Number].floatValue()

  def getDouble(i: Int): Double =
    requireNotNull(i)
    get(i).asInstanceOf[Number].doubleValue()

  def getString(i: Int): String = get(i).asInstanceOf[String]

  /** Get the value at position `i` as type `T` (unchecked cast). */
  def getAs[T](i: Int): T = get(i).asInstanceOf[T]

  /** Get the value of the field named `fieldName` as type `T`. Requires a schema. */
  def getAs[T](fieldName: String): T =
    schema match
      case Some(s) => get(s.fieldIndex(fieldName)).asInstanceOf[T]
      case None    =>
        throw UnsupportedOperationException("getAs by field name requires a Row with schema")

  /** Get the value at position `i` as a `java.math.BigDecimal`. Accepts either Java/Scala
    * `BigDecimal` or any `Number` (lossy via `doubleValue` for non-decimal numerics).
    *
    * Unlike the other `getXxx` accessors which use `asInstanceOf` and surface a
    * `ClassCastException` on a type mismatch, this method dispatches via pattern match with no
    * fallback case — non-`Number` / non-`BigDecimal` values raise `MatchError`. Use [[get]] if you
    * need to inspect the raw value first.
    *
    * @throws scala.MatchError
    *   if the value is neither `Number` nor `BigDecimal`.
    */
  def getDecimal(i: Int): java.math.BigDecimal = get(i) match
    case d: java.math.BigDecimal => d
    case d: BigDecimal           => d.underlying
    case n: Number               => java.math.BigDecimal.valueOf(n.doubleValue())

  /** Get a `DateType` field as `java.sql.Date`. Use [[getLocalDate]] for `java.time.LocalDate`. */
  def getDate(i: Int): java.sql.Date = get(i).asInstanceOf[java.sql.Date]

  /** Get a `TimestampType` field as `java.sql.Timestamp`. Use [[getInstant]] for
    * `java.time.Instant`.
    */
  def getTimestamp(i: Int): java.sql.Timestamp = get(i).asInstanceOf[java.sql.Timestamp]

  /** Get a `TimestampType` field as `java.time.Instant`. */
  def getInstant(i: Int): java.time.Instant = get(i).asInstanceOf[java.time.Instant]

  /** Get a `DateType` field as `java.time.LocalDate`. */
  def getLocalDate(i: Int): java.time.LocalDate = get(i).asInstanceOf[java.time.LocalDate]

  /** Get an `ArrayType` field as a Scala `Seq`. Use [[getList]] for `java.util.List`. */
  def getSeq[T](i: Int): Seq[T] = get(i).asInstanceOf[Seq[T]]

  /** Get an `ArrayType` field as a `java.util.List`. */
  def getList[T](i: Int): java.util.List[T] =
    import scala.jdk.CollectionConverters.*
    getSeq[T](i).asJava

  /** Get a `MapType` field as a Scala `Map`. Use [[getJavaMap]] for `java.util.Map`. */
  def getMap[K, V](i: Int): Map[K, V] = get(i).asInstanceOf[Map[K, V]]

  /** Get a `MapType` field as a `java.util.Map`. */
  def getJavaMap[K, V](i: Int): java.util.Map[K, V] =
    import scala.jdk.CollectionConverters.*
    getMap[K, V](i).asJava

  /** Get a nested `StructType` field as a [[Row]]. */
  def getStruct(i: Int): Row = get(i).asInstanceOf[Row]

  def getGeometry(i: Int): org.apache.spark.sql.types.Geometry =
    get(i).asInstanceOf[org.apache.spark.sql.types.Geometry]

  def getGeography(i: Int): org.apache.spark.sql.types.Geography =
    get(i).asInstanceOf[org.apache.spark.sql.types.Geography]

  /** Position of the field named `name`. Requires a schema. */
  def fieldIndex(name: String): Int =
    schema match
      case Some(s) => s.fieldIndex(name)
      case None    =>
        throw UnsupportedOperationException("fieldIndex requires a Row with schema")

  /** True iff any field is `null`. */
  def anyNull: Boolean =
    // while loop avoids the Int boxing that `(0 until size).exists(isNullAt)` incurs
    // (Range.exists lifts the index into a boxed Integer via the Function1 adapter).
    val n = size
    var i = 0
    while i < n do
      if isNullAt(i) then return true
      i += 1
    false

  /** Render this row as a single-line JSON object. Requires a schema. */
  def json: String =
    schema match
      case Some(s) =>
        val fields = s.fields.zipWithIndex.map { (f, i) =>
          val v = if isNullAt(i) then "null" else Row.encodeJsonValue(get(i))
          s"\"${Row.escapeJson(f.name)}\":$v"
        }
        s"{${fields.mkString(",")}}"
      case None =>
        throw UnsupportedOperationException("json requires a Row with schema")

  /** Alias for [[json]]. */
  def toJson: String = json

  /** Render this row as a multi-line indented JSON object. Requires a schema. */
  def prettyJson: String =
    schema match
      case Some(s) =>
        val fields = s.fields.zipWithIndex.map { (f, i) =>
          val v = if isNullAt(i) then "null" else Row.encodeJsonValue(get(i))
          s"  \"${Row.escapeJson(f.name)}\" : $v"
        }
        s"{\n${fields.mkString(",\n")}\n}"
      case None =>
        throw UnsupportedOperationException("prettyJson requires a Row with schema")

  /** Shallow copy of this row.
    *
    * `Row` is immutable (the `values` and `schema` are stored once at construction and never
    * mutated), so the original instance is returned — matching upstream Spark's
    * `GenericRowWithSchema.copy()` identity semantics. This preserves any attached schema, so
    * follow-up calls like `getAs(name)`, `fieldIndex`, `json`, `prettyJson`, and `getValuesMap`
    * keep working on the copy.
    */
  def copy(): Row = this

  /** Map from each given field name to its value. Requires a schema. */
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

  /** All field values as a `Seq`, in positional order. */
  def toSeq: Seq[Any] = values.toSeq

  /** Concatenate field values as strings, separated by `sep`. `null` is rendered as `"null"`. */
  def mkString(sep: String): String =
    values.map(v => if v == null then "null" else v.toString).mkString(sep)

  override def toString: String =
    values.map(v => if v == null then "null" else v.toString).mkString("[", ",", "]")

  /** Element-wise equality that handles JVM `Array[_]` reference semantics — two rows with
    * content-equal binary or array fields must compare equal. Without this, `Set[Row]` /
    * `Map[Row, _]` silently fail to deduplicate any row containing a `BinaryType` column.
    */
  override def equals(other: Any): Boolean = other match
    case that: Row =>
      val n = values.size
      if n != that.values.size then false
      else
        var i = 0
        while i < n do
          if !Row.elementEquals(values(i), that.values(i)) then return false
          i += 1
        true
    case _ => false

  override def hashCode(): Int =
    val n = values.size
    var h = 1
    var i = 0
    while i < n do
      h = 31 * h + Row.elementHash(values(i))
      i += 1
    h

object Row:
  /** Escape a string for safe inclusion in JSON output. */
  private def escapeJson(s: String): String =
    org.apache.spark.sql.internal.JsonEscaping.escape(s)

  /** Type-aware JSON encoding for a single field value. Produces parseable JSON for the common
    * Spark/Java types — quoted strings for textual values, ISO-8601 for dates / timestamps, base64
    * for binary, recursive arrays / objects for collections, and bare JSON numbers for numerics /
    * booleans. The previous `other.toString` fallback emitted invalid JSON for `Date` / `Timestamp`
    * (no quotes), `Array[Byte]` (`[B@...`), nested rows, `Seq` / `Map`.
    *
    * `null` is handled by the call site so it is not part of this dispatch.
    */
  private def encodeJsonValue(v: Any): String = v match
    case null                         => "null"
    case b: Boolean                   => b.toString
    case n: Byte                      => n.toString
    case n: Short                     => n.toString
    case n: Int                       => n.toString
    case n: Long                      => n.toString
    case n: Float                     => n.toString
    case n: Double                    => n.toString
    case bd: java.math.BigDecimal     => bd.toPlainString
    case bd: BigDecimal               => bd.bigDecimal.toPlainString
    case n: Number                    => n.toString
    case s: String                    => s"\"${escapeJson(s)}\""
    case d: java.sql.Date             => s"\"${d.toLocalDate.toString}\""
    case ld: java.time.LocalDate      => s"\"${ld.toString}\""
    case ts: java.sql.Timestamp       => s"\"${ts.toInstant.toString}\""
    case inst: java.time.Instant      => s"\"${inst.toString}\""
    case lt: java.time.LocalTime      => s"\"${lt.toString}\""
    case ldt: java.time.LocalDateTime => s"\"${ldt.toString}\""
    case d: java.time.Duration        => s"\"${d.toString}\""
    case p: java.time.Period          => s"\"${p.toString}\""
    case u: java.util.UUID            => s"\"${u.toString}\""
    case bytes: Array[Byte]           =>
      s"\"${java.util.Base64.getEncoder.encodeToString(bytes)}\""
    case row: Row    => row.json
    case seq: Seq[?] =>
      seq.map(elem => if elem == null then "null" else encodeJsonValue(elem))
        .mkString("[", ",", "]")
    case arr: Array[?] =>
      arr.iterator
        .map(elem => if elem == null then "null" else encodeJsonValue(elem))
        .mkString("[", ",", "]")
    case m: scala.collection.Map[?, ?] =>
      m.iterator.map { case (k, vv) =>
        val key = k match
          case s: String => escapeJson(s)
          case other     => escapeJson(String.valueOf(other))
        val value = if vv == null then "null" else encodeJsonValue(vv)
        s"\"$key\":$value"
      }.mkString("{", ",", "}")
    case jl: java.util.List[?] =>
      import scala.jdk.CollectionConverters.*
      encodeJsonValue(jl.asScala.toSeq)
    case jm: java.util.Map[?, ?] =>
      import scala.jdk.CollectionConverters.*
      encodeJsonValue(jm.asScala.toMap)
    case other =>
      // Last-resort fallback: stringify and quote, so we still emit *parseable* JSON.
      s"\"${escapeJson(String.valueOf(other))}\""

  /** Element-wise equality used by [[Row.equals]]. Handles `Array[_]` reference semantics
    * (delegating to `java.util.Arrays.equals`) and recurses into nested `Row` values.
    */
  private def elementEquals(a: Any, b: Any): Boolean = (a, b) match
    case (null, null)                     => true
    case (null, _) | (_, null)            => false
    case (x: Array[Byte], y: Array[Byte]) => java.util.Arrays.equals(x, y)
    case (x: Array[?], y: Array[?])       =>
      x.length == y.length &&
      x.zip(y).forall((xi, yi) => elementEquals(xi, yi))
    case (x: Row, y: Row) => x == y
    case _                => a == b

  /** Element-wise hash used by [[Row.hashCode]]. Mirrors [[elementEquals]]'s `Array` handling via
    * `java.util.Arrays.hashCode` so the equals/hashCode contract holds.
    */
  private def elementHash(a: Any): Int = a match
    case null           => 0
    case x: Array[Byte] => java.util.Arrays.hashCode(x)
    case x: Array[?]    =>
      var h = 1
      val n = x.length
      var i = 0
      while i < n do
        h = 31 * h + elementHash(x(i))
        i += 1
      h
    case other => other.hashCode()

  def fromSeq(values: Seq[Any]): Row = values match
    case idx: IndexedSeq[Any @unchecked] => new Row(idx)
    case _                               => new Row(values.toIndexedSeq)

  def fromArray(values: Array[Object]): Row = fromSeq(values.toIndexedSeq)

  def fromSeqWithSchema(values: Seq[Any], schema: StructType): Row =
    require(
      values.size == schema.fields.size,
      s"values.size (${values.size}) must equal schema.fields.size (${schema.fields.size})"
    )
    values match
      case idx: IndexedSeq[Any @unchecked] => new Row(idx, Some(schema))
      case _                               => new Row(values.toIndexedSeq, Some(schema))

  def apply(values: Any*): Row = fromSeq(values)

  def fromTuple(t: Product): Row = fromSeq(t.productIterator.toSeq)

  /** Fast-path constructor that avoids the `toIndexedSeq` copy when the caller already provides an
    * `IndexedSeq`. Package-private to keep the public API unchanged.
    */
  private[sql] def fromSeqDirectWithSchema(values: IndexedSeq[Any], schema: StructType): Row =
    new Row(values, Some(schema))

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
