package org.apache.spark.sql.types

/** The base type of all Spark SQL data types. */
trait DataType:
  def typeName: String
  def simpleString: String = typeName

  /** SQL representation used in DDL strings. */
  def sql: String = typeName.toUpperCase

case object BooleanType extends DataType:
  def typeName = "boolean"

case object ByteType extends DataType:
  def typeName = "byte"
  override def sql = "TINYINT"

case object ShortType extends DataType:
  def typeName = "short"
  override def sql = "SMALLINT"

case object IntegerType extends DataType:
  def typeName = "integer"
  override def sql = "INT"

case object LongType extends DataType:
  def typeName = "long"
  override def sql = "BIGINT"

case object FloatType extends DataType:
  def typeName = "float"
  override def sql = "FLOAT"

case object DoubleType extends DataType:
  def typeName = "double"
  override def sql = "DOUBLE"

case object StringType extends DataType:
  def typeName = "string"
  override def sql = "STRING"

final case class CharType(length: Int) extends DataType:
  require(length >= 0, "The length of char type cannot be negative.")
  def typeName = s"char($length)"
  override def sql = s"CHAR($length)"

final case class VarcharType(length: Int) extends DataType:
  require(length >= 0, "The length of varchar type cannot be negative.")
  def typeName = s"varchar($length)"
  override def sql = s"VARCHAR($length)"

case object BinaryType extends DataType:
  def typeName = "binary"
  override def sql = "BINARY"

case object DateType extends DataType:
  def typeName = "date"
  override def sql = "DATE"

case object TimestampType extends DataType:
  def typeName = "timestamp"
  override def sql = "TIMESTAMP"

case object TimestampNTZType extends DataType:
  def typeName = "timestamp_ntz"
  override def sql = "TIMESTAMP_NTZ"

case object NullType extends DataType:
  def typeName = "null"
  override def sql = "VOID"

case object VariantType extends DataType:
  def typeName = "variant"
  override def sql = "VARIANT"

case object DayTimeIntervalType extends DataType:
  def typeName = "day_time_interval"
  override def sql = "INTERVAL DAY TO SECOND"

case object YearMonthIntervalType extends DataType:
  def typeName = "year_month_interval"
  override def sql = "INTERVAL YEAR TO MONTH"

case object CalendarIntervalType extends DataType:
  def typeName = "calendar_interval"
  override def sql = "INTERVAL"

final case class TimeType(precision: Int = TimeType.DEFAULT_PRECISION) extends DataType:
  def typeName = s"time($precision)"
  override def sql = s"TIME($precision)"

object TimeType:
  val DEFAULT_PRECISION: Int = 6

final case class GeometryType(srid: Int = GeometryType.DEFAULT_SRID) extends DataType:
  def typeName = s"geometry($srid)"
  override def sql = s"GEOMETRY($srid)"

object GeometryType:
  val DEFAULT_SRID: Int = 0

final case class GeographyType(srid: Int = GeographyType.DEFAULT_SRID) extends DataType:
  def typeName = s"geography($srid)"
  override def sql = s"GEOGRAPHY($srid)"

object GeographyType:
  val DEFAULT_SRID: Int = 4326

final case class DecimalType(precision: Int, scale: Int) extends DataType:
  def typeName = "decimal"
  override def simpleString = s"decimal($precision,$scale)"
  override def sql = s"DECIMAL($precision,$scale)"

object DecimalType:
  val DEFAULT: DecimalType = DecimalType(10, 0)

final case class ArrayType(elementType: DataType, containsNull: Boolean) extends DataType:
  def typeName = "array"
  override def simpleString = s"array<${elementType.simpleString}>"
  override def sql = s"ARRAY<${elementType.sql}>"

final case class MapType(keyType: DataType, valueType: DataType, valueContainsNull: Boolean)
    extends DataType:
  def typeName = "map"
  override def simpleString = s"map<${keyType.simpleString},${valueType.simpleString}>"
  override def sql = s"MAP<${keyType.sql},${valueType.sql}>"

final case class StructField(name: String, dataType: DataType, nullable: Boolean = true)

final case class StructType(fields: Seq[StructField]) extends DataType:
  def typeName = "struct"

  def apply(name: String): StructField =
    // Use the lazy name→index map instead of a linear `fields.find` — cheaper on repeated lookups
    // such as groupBy/agg chains that call apply(name) once per field per transformation.
    fieldNameIndex.get(name) match
      case Some(i) => fields(i)
      case None    =>
        throw java.util.NoSuchElementException(s"Field '$name' not found in $this")

  def fieldNames: Array[String] = fields.map(_.name).toArray

  /** Name → index map; for schemas that contain duplicate field names (rare but permitted by
    * `StructType.apply`), the FIRST occurrence wins, matching `fields.find(_.name == name)`
    * semantics and upstream Spark behavior. `Map.apply` on a sequence of pairs retains the LAST
    * occurrence, so we fold manually and skip duplicates.
    */
  private lazy val fieldNameIndex: Map[String, Int] =
    val m = scala.collection.mutable.LinkedHashMap.empty[String, Int]
    var i = 0
    val n = fields.size
    while i < n do
      val name = fields(i).name
      if !m.contains(name) then m(name) = i
      i += 1
    m.toMap

  def fieldIndex(name: String): Int =
    fieldNameIndex.getOrElse(
      name,
      throw IllegalArgumentException(s"Field '$name' not found in $this")
    )

  override def simpleString: String =
    s"struct<${fields.map(f => s"${f.name}:${f.dataType.simpleString}").mkString(",")}>"

  /** Return the DDL string representation (e.g. "id BIGINT, name STRING"). */
  def toDDL: String =
    fields.map { f =>
      val nullStr = if f.nullable then "" else " NOT NULL"
      val quotedName = s"`${f.name.replace("`", "``")}`"
      s"$quotedName ${f.dataType.sql}$nullStr"
    }.mkString(", ")

  def treeString: String = treeString(Int.MaxValue)

  /** Print schema tree up to the given nesting depth. */
  def treeString(maxLevel: Int): String =
    val sb = StringBuilder()
    sb.append("root\n")
    def buildTree(fields: Seq[StructField], indent: Int): Unit =
      if indent < maxLevel then
        fields.foreach { f =>
          val prefix = " |" * indent + "-- "
          val nullStr = if f.nullable then "nullable = true" else "nullable = false"
          sb.append(s"$prefix${f.name}: ${f.dataType.simpleString} ($nullStr)\n")
          f.dataType match
            case st: StructType => buildTree(st.fields, indent + 1)
            case _              => ()
        }
    buildTree(fields, 1)
    sb.toString

object StructType:
  val empty: StructType = StructType(Seq.empty)

/** Represents an unparsed data type returned by the server as a raw type string.
  *
  * This is used when the server sends a DataType.Unparsed proto that this client cannot resolve to
  * a concrete type.
  */
final case class UnparsedDataType(typeString: String) extends DataType:
  def typeName = "unparsed"
  override def simpleString = s"unparsed($typeString)"
  override def sql = typeString
