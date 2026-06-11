package org.apache.spark.sql.types

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}

/** The base type of all Spark SQL data types. */
trait DataType:
  def typeName: String
  def simpleString: String = typeName
  def catalogString: String = DataType.catalogString(this)
  def defaultSize: Int = DataType.defaultSize(this)
  def json: String = DataType.toJson(this)
  def prettyJson: String = DataType.toPrettyJson(this)

  /** SQL representation used in DDL strings. */
  def sql: String = typeName.toUpperCase

  def sameType(other: DataType): Boolean =
    DataType.equalsIgnoreNullability(this, other)

object DataType:
  private val mapper = ObjectMapper()
  private val decimalType = """decimal\(\s*(\d+)\s*,\s*(-?\d+)\s*\)""".r
  private val charType = """char\(\s*(\d+)\s*\)""".r
  private val varcharType = """varchar\(\s*(\d+)\s*\)""".r
  private val timeType = """time\(\s*(\d+)\s*\)""".r
  private val geometryType = """geometry\(\s*(\d+)\s*\)""".r
  private val geographyType = """geography\(\s*(\d+)\s*\)""".r

  def fromDDL(ddl: String): DataType =
    try DDLParser(ddl).parseDataTypeOnly()
    catch
      case first: IllegalArgumentException =>
        try StructType.fromDDL(ddl)
        catch
          case second: IllegalArgumentException =>
            first.addSuppressed(second)
            throw first

  def fromJson(json: String): DataType =
    fromJsonNode(mapper.readTree(json))

  def equalsStructurally(
      from: DataType,
      to: DataType,
      ignoreNullability: Boolean = false
  ): Boolean =
    (from, to) match
      case (ArrayType(leftElement, leftNull), ArrayType(rightElement, rightNull)) =>
        equalsStructurally(leftElement, rightElement, ignoreNullability) &&
        (ignoreNullability || leftNull == rightNull)
      case (
            MapType(leftKey, leftValue, leftNull),
            MapType(rightKey, rightValue, rightNull)
          ) =>
        equalsStructurally(leftKey, rightKey, ignoreNullability) &&
        equalsStructurally(leftValue, rightValue, ignoreNullability) &&
        (ignoreNullability || leftNull == rightNull)
      case (StructType(leftFields), StructType(rightFields)) =>
        leftFields.length == rightFields.length &&
        leftFields.zip(rightFields).forall { case (left, right) =>
          equalsStructurally(left.dataType, right.dataType, ignoreNullability) &&
          (ignoreNullability || left.nullable == right.nullable)
        }
      case (left, right) => left == right

  def equalsStructurallyByName(
      from: DataType,
      to: DataType,
      resolver: (String, String) => Boolean
  ): Boolean =
    (from, to) match
      case (ArrayType(leftElement, _), ArrayType(rightElement, _)) =>
        equalsStructurallyByName(leftElement, rightElement, resolver)
      case (MapType(leftKey, leftValue, _), MapType(rightKey, rightValue, _)) =>
        equalsStructurallyByName(leftKey, rightKey, resolver) &&
        equalsStructurallyByName(leftValue, rightValue, resolver)
      case (StructType(leftFields), StructType(rightFields)) =>
        leftFields.length == rightFields.length &&
        leftFields.zip(rightFields).forall { case (left, right) =>
          resolver(left.name, right.name) &&
          equalsStructurallyByName(left.dataType, right.dataType, resolver)
        }
      case _ => true

  def equalsIgnoreNullability(left: DataType, right: DataType): Boolean =
    (left, right) match
      case (ArrayType(leftElement, _), ArrayType(rightElement, _)) =>
        equalsIgnoreNullability(leftElement, rightElement)
      case (MapType(leftKey, leftValue, _), MapType(rightKey, rightValue, _)) =>
        equalsIgnoreNullability(leftKey, rightKey) &&
        equalsIgnoreNullability(leftValue, rightValue)
      case (StructType(leftFields), StructType(rightFields)) =>
        leftFields.length == rightFields.length &&
        leftFields.zip(rightFields).forall { case (leftField, rightField) =>
          leftField.name == rightField.name &&
          equalsIgnoreNullability(leftField.dataType, rightField.dataType)
        }
      case (leftType, rightType) => leftType == rightType

  def equalsIgnoreCaseAndNullability(from: DataType, to: DataType): Boolean =
    (from, to) match
      case (ArrayType(fromElement, _), ArrayType(toElement, _)) =>
        equalsIgnoreCaseAndNullability(fromElement, toElement)
      case (MapType(fromKey, fromValue, _), MapType(toKey, toValue, _)) =>
        equalsIgnoreCaseAndNullability(fromKey, toKey) &&
        equalsIgnoreCaseAndNullability(fromValue, toValue)
      case (StructType(fromFields), StructType(toFields)) =>
        fromFields.length == toFields.length &&
        fromFields.zip(toFields).forall { case (fromField, toField) =>
          fromField.name.equalsIgnoreCase(toField.name) &&
          equalsIgnoreCaseAndNullability(fromField.dataType, toField.dataType)
        }
      case (fromType, toType) => fromType == toType

  private[types] def catalogString(dataType: DataType): String =
    dataType match
      case ByteType                       => "tinyint"
      case ShortType                      => "smallint"
      case IntegerType                    => "int"
      case LongType                       => "bigint"
      case NullType                       => "void"
      case DecimalType(precision, scale)  => s"decimal($precision,$scale)"
      case ArrayType(elementType, _)      => s"array<${elementType.catalogString}>"
      case MapType(keyType, valueType, _) =>
        s"map<${keyType.catalogString},${valueType.catalogString}>"
      case StructType(fields) =>
        fields.map(f => s"${f.name}:${f.dataType.catalogString}").mkString("struct<", ",", ">")
      case udt: UserDefinedType[?]      => udt.sqlType.catalogString
      case UnparsedDataType(typeString) => typeString
      case other                        => other.simpleString

  private[types] def defaultSize(dataType: DataType): Int =
    dataType match
      case BooleanType | ByteType | NullType                          => 1
      case ShortType                                                  => 2
      case IntegerType | FloatType | DateType | YearMonthIntervalType => 4
      case LongType | DoubleType | TimestampType | TimestampNTZType | TimeType(_) |
          DayTimeIntervalType =>
        8
      case StringType                                       => 20
      case BinaryType                                       => 100
      case CalendarIntervalType                             => 16
      case VariantType | GeometryType(_) | GeographyType(_) => 2048
      case CharType(length)                                 => length
      case VarcharType(length)                              => length
      case DecimalType(precision, _)                        => if precision <= 18 then 8 else 16
      case ArrayType(elementType, _)                        => elementType.defaultSize
      case MapType(keyType, valueType, _) => keyType.defaultSize + valueType.defaultSize
      case StructType(fields)             => fields.map(_.dataType.defaultSize).sum
      case udt: UserDefinedType[?]        => udt.sqlType.defaultSize
      case UnparsedDataType(_)            => 0

  private[types] def toJson(dataType: DataType): String =
    mapper.writeValueAsString(toJsonNode(dataType))

  private[types] def toPrettyJson(dataType: DataType): String =
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(toJsonNode(dataType))

  private[types] def toJsonNode(dataType: DataType): JsonNode =
    dataType match
      case ArrayType(elementType, containsNull) =>
        val node = mapper.createObjectNode()
        node.put("type", "array")
        node.set[JsonNode]("elementType", toJsonNode(elementType))
        node.put("containsNull", containsNull)
        node
      case MapType(keyType, valueType, valueContainsNull) =>
        val node = mapper.createObjectNode()
        node.put("type", "map")
        node.set[JsonNode]("keyType", toJsonNode(keyType))
        node.set[JsonNode]("valueType", toJsonNode(valueType))
        node.put("valueContainsNull", valueContainsNull)
        node
      case StructType(fields) =>
        val node = mapper.createObjectNode()
        val fieldNodes = mapper.createArrayNode()
        fields.foreach(field => fieldNodes.add(toJsonNode(field)))
        node.put("type", "struct")
        node.set[ArrayNode]("fields", fieldNodes)
        node
      case pyUdt: PythonUserDefinedType =>
        val node = mapper.createObjectNode()
        node.put("type", "udt")
        node.put("pyClass", pyUdt.pyUDT)
        node.put("serializedClass", pyUdt.serializedPyClass)
        node.set[JsonNode]("sqlType", toJsonNode(pyUdt.sqlType))
        node
      case udt: UserDefinedType[?] =>
        val node = mapper.createObjectNode()
        node.put("type", "udt")
        node.put("class", udt.getClass.getName)
        if udt.pyUDT == null then node.set[JsonNode]("pyClass", mapper.nullNode())
        else node.put("pyClass", udt.pyUDT)
        node.set[JsonNode]("sqlType", toJsonNode(udt.sqlType))
        node
      case DecimalType(precision, scale) =>
        mapper.getNodeFactory.textNode(s"decimal($precision,$scale)")
      case other => mapper.getNodeFactory.textNode(jsonTypeName(other))

  private def toJsonNode(field: StructField): ObjectNode =
    val node = mapper.createObjectNode()
    node.put("name", field.name)
    node.set[JsonNode]("type", toJsonNode(field.dataType))
    node.put("nullable", field.nullable)
    node.set[JsonNode]("metadata", mapper.readTree(field.metadata.json))
    node

  private def fromJsonNode(node: JsonNode): DataType =
    if node.isTextual then parseTypeName(node.asText())
    else if node.isObject then
      node.path("type").asText() match
        case "array" =>
          ArrayType(
            fromJsonNode(node.get("elementType")),
            node.path("containsNull").asBoolean(true)
          )
        case "map" =>
          MapType(
            fromJsonNode(node.get("keyType")),
            fromJsonNode(node.get("valueType")),
            node.path("valueContainsNull").asBoolean(true)
          )
        case "struct" =>
          val fields = node.path("fields").elements()
          val builder = Vector.newBuilder[StructField]
          while fields.hasNext do builder += parseStructField(fields.next())
          StructType(builder.result())
        case "udt" =>
          val classNode = node.get("class")
          if classNode != null && !classNode.isNull then
            instantiateUDT(classNode.asText())
          else
            new PythonUserDefinedType(
              fromJsonNode(node.get("sqlType")),
              textOrNull(node.get("pyClass")),
              textOrNull(node.get("serializedClass"))
            )
        case other =>
          throw IllegalArgumentException(s"Unsupported JSON data type: $other")
    else throw IllegalArgumentException(s"Unsupported JSON data type: $node")

  private def parseStructField(node: JsonNode): StructField =
    val metadataNode = node.get("metadata")
    val metadata =
      if metadataNode == null || metadataNode.isNull then Metadata.empty
      else Metadata.fromJson(metadataNode.toString)
    StructField(
      node.path("name").asText(),
      fromJsonNode(node.get("type")),
      node.path("nullable").asBoolean(true),
      metadata
    )

  private def instantiateUDT(className: String): UserDefinedType[?] =
    val udtClass = Class.forName(className)
    if !classOf[UserDefinedType[?]].isAssignableFrom(udtClass) then
      throw IllegalArgumentException(s"Class '$className' is not a UserDefinedType subclass")
    udtClass.getConstructor().newInstance().asInstanceOf[UserDefinedType[?]]

  private def textOrNull(node: JsonNode): String =
    if node == null || node.isNull then null else node.asText()

  private def jsonTypeName(dataType: DataType): String =
    dataType match
      case DecimalType(precision, scale) => s"decimal($precision,$scale)"
      case other                         => other.typeName

  private[types] def parseTypeName(raw: String): DataType =
    raw.trim.toLowerCase match
      case decimalType(precision, scale) => DecimalType(precision.toInt, scale.toInt)
      case charType(length)              => CharType(length.toInt)
      case varcharType(length)           => VarcharType(length.toInt)
      case timeType(precision)           => TimeType(precision.toInt)
      case geometryType(srid)            => GeometryType(srid.toInt)
      case geographyType(srid)           => GeographyType(srid.toInt)
      case "decimal"                     => DecimalType.DEFAULT
      case "boolean" | "bool"            => BooleanType
      case "byte" | "tinyint"            => ByteType
      case "short" | "smallint"          => ShortType
      case "integer" | "int"             => IntegerType
      case "long" | "bigint"             => LongType
      case "float" | "real"              => FloatType
      case "double"                      => DoubleType
      case "string"                      => StringType
      case "binary"                      => BinaryType
      case "date"                        => DateType
      case "timestamp" | "timestamp_ltz" => TimestampType
      case "timestamp_ntz"               => TimestampNTZType
      case "null" | "void"               => NullType
      case "variant"                     => VariantType
      case "day_time_interval"           => DayTimeIntervalType
      case "year_month_interval"         => YearMonthIntervalType
      case "calendar_interval"           => CalendarIntervalType
      case "time"                        => TimeType()
      case "geometry"                    => GeometryType()
      case "geography"                   => GeographyType()
      case other => throw IllegalArgumentException(s"Unsupported data type: $other")

  final private[types] class DDLParser(input: String):
    private var pos = 0

    def parseDataTypeOnly(): DataType =
      val dataType = parseDataType()
      skipWhitespace()
      requireEnd()
      dataType

    def parseTableSchema(): StructType =
      skipWhitespace()
      if atEnd then StructType.empty
      else
        val fields = Vector.newBuilder[StructField]
        fields += parseStructField(requireColon = false)
        skipWhitespace()
        while consumeIf(',') do
          fields += parseStructField(requireColon = false)
          skipWhitespace()
        requireEnd()
        StructType(fields.result())

    private def parseDataType(): DataType =
      skipWhitespace()
      val word = parseWord()
      val lower = word.toLowerCase
      lower match
        case "array" =>
          expect('<')
          val elementType = parseDataType()
          expect('>')
          ArrayType(elementType, containsNull = true)
        case "map" =>
          expect('<')
          val keyType = parseDataType()
          expect(',')
          val valueType = parseDataType()
          expect('>')
          MapType(keyType, valueType, valueContainsNull = true)
        case "struct" =>
          expect('<')
          val fields = Vector.newBuilder[StructField]
          skipWhitespace()
          if !consumeIf('>') then
            fields += parseStructField(requireColon = true)
            skipWhitespace()
            while consumeIf(',') do
              fields += parseStructField(requireColon = true)
              skipWhitespace()
            expect('>')
          StructType(fields.result())
        case "decimal" =>
          if consumeIf('(') then
            val precision = parseInt()
            expect(',')
            val scale = parseInt()
            expect(')')
            DecimalType(precision, scale)
          else DecimalType.DEFAULT
        case "char" =>
          CharType(parseParenthesizedInt("char"))
        case "varchar" =>
          VarcharType(parseParenthesizedInt("varchar"))
        case "time" =>
          if peekContains('(') then TimeType(parseParenthesizedInt("time")) else TimeType()
        case "geometry" =>
          if peekContains('(') then GeometryType(parseParenthesizedInt("geometry"))
          else GeometryType()
        case "geography" =>
          if peekContains('(') then GeographyType(parseParenthesizedInt("geography"))
          else GeographyType()
        case "interval" =>
          parseIntervalType()
        case other =>
          parseTypeName(other)

    private def parseStructField(requireColon: Boolean): StructField =
      skipWhitespace()
      val name = parseFieldName()
      skipWhitespace()
      if requireColon then expect(':')
      val dataType = parseDataType()
      val nullable = parseNullability()
      StructField(name, dataType, nullable)

    private def parseIntervalType(): DataType =
      skipWhitespace()
      if consumeKeyword("day") then
        consumeKeyword("to")
        consumeKeyword("second")
        DayTimeIntervalType
      else if consumeKeyword("year") then
        consumeKeyword("to")
        consumeKeyword("month")
        YearMonthIntervalType
      else CalendarIntervalType

    private def parseNullability(): Boolean =
      skipWhitespace()
      if consumeKeyword("not") then
        if !consumeKeyword("null") then fail("Expected NULL after NOT")
        false
      else
        consumeKeyword("null")
        true

    private def parseParenthesizedInt(typeName: String): Int =
      if !consumeIf('(') then fail(s"Expected precision or length for $typeName")
      val value = parseInt()
      expect(')')
      value

    private def parseInt(): Int =
      skipWhitespace()
      val start = pos
      if current == '-' then pos += 1
      while !atEnd && current.isDigit do pos += 1
      if start == pos || (input.charAt(start) == '-' && pos == start + 1) then
        fail("Expected integer")
      input.substring(start, pos).toInt

    private def parseFieldName(): String =
      if current == '`' then parseBacktickIdentifier()
      else parseWord()

    private def parseBacktickIdentifier(): String =
      expect('`')
      val sb = StringBuilder()
      while !atEnd do
        val ch = current
        pos += 1
        if ch == '`' then
          if !atEnd && current == '`' then
            sb.append('`')
            pos += 1
          else return sb.toString
        else sb.append(ch)
      fail("Unclosed quoted identifier")

    private def parseWord(): String =
      skipWhitespace()
      val start = pos
      while !atEnd && !current.isWhitespace && !Set('<', '>', ':', ',', '(', ')').contains(current)
      do pos += 1
      if start == pos then fail("Expected identifier or data type")
      input.substring(start, pos)

    private def consumeKeyword(keyword: String): Boolean =
      skipWhitespace()
      val end = pos + keyword.length
      if end <= input.length &&
        input.regionMatches(true, pos, keyword, 0, keyword.length) &&
        (end == input.length || !isIdentifierPart(input.charAt(end)))
      then
        pos = end
        true
      else false

    private def expect(ch: Char): Unit =
      skipWhitespace()
      if !consumeIf(ch) then fail(s"Expected '$ch'")

    private def consumeIf(ch: Char): Boolean =
      skipWhitespace()
      if !atEnd && current == ch then
        pos += 1
        true
      else false

    private def peekContains(ch: Char): Boolean =
      skipWhitespace()
      !atEnd && current == ch

    private def requireEnd(): Unit =
      if !atEnd then fail("Unexpected trailing input")

    private def skipWhitespace(): Unit =
      while !atEnd && current.isWhitespace do pos += 1

    private def current: Char = input.charAt(pos)
    private def atEnd: Boolean = pos >= input.length

    private def isIdentifierPart(ch: Char): Boolean =
      ch.isLetterOrDigit || ch == '_'

    private def fail(message: String): Nothing =
      throw IllegalArgumentException(s"$message at position $pos in '$input'")

  private[types] object DDLParser:
    def apply(input: String): DDLParser = new DDLParser(input)

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

final case class StructField(
    name: String,
    dataType: DataType,
    nullable: Boolean = true,
    metadata: Metadata = Metadata.empty
)

final case class StructType(fields: Seq[StructField]) extends DataType:
  def typeName = "struct"

  /** Render this struct as a Spark DDL string, e.g. `STRUCT<a: INT, b: STRING>`.
    *
    * Without this override the inherited default returns just `"STRUCT"`, dropping every field —
    * and because [[ArrayType.sql]] / [[MapType.sql]] recurse into `elementType.sql` /
    * `valueType.sql`, that loss propagates through any outer composite type (`ARRAY<STRUCT<...>>`,
    * `MAP<STRING, STRUCT<...>>`).
    */
  override def sql: String =
    s"STRUCT<${fields.map(f => s"${f.name}: ${f.dataType.sql}").mkString(", ")}>"

  def apply(name: String): StructField =
    // Use the lazy name→field map instead of a linear `fields.find` — O(1) regardless of the
    // underlying Seq type (avoids O(n) `fields(i)` on List-backed schemas).
    fieldByName.get(name) match
      case Some(f) => f
      case None    =>
        throw java.util.NoSuchElementException(s"Field '$name' not found in $this")

  def fieldNames: Array[String] = fields.map(_.name).toArray

  /** Cache of `name -> StructField` built once lazily. First-wins on duplicate names to match the
    * pre-cache `fields.find(_.name == name)` semantics and upstream Spark behavior. Built via
    * iterator — O(n) for any Seq backing.
    */
  private lazy val fieldByName: Map[String, StructField] =
    val m = scala.collection.mutable.LinkedHashMap.empty[String, StructField]
    val it = fields.iterator
    while it.hasNext do
      val f = it.next()
      if !m.contains(f.name) then m(f.name) = f
    m.toMap

  /** Name → index map with the same first-wins / O(n)-build invariants as `fieldByName`. */
  private lazy val fieldNameIndex: Map[String, Int] =
    val m = scala.collection.mutable.LinkedHashMap.empty[String, Int]
    val it = fields.iterator
    var i = 0
    while it.hasNext do
      val f = it.next()
      if !m.contains(f.name) then m(f.name) = i
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
      if indent <= maxLevel then
        fields.foreach { f =>
          val prefix = " |" * indent + "-- "
          val nullStr = if f.nullable then "nullable = true" else "nullable = false"
          sb.append(s"$prefix${f.name}: ${f.dataType.simpleString} ($nullStr)\n")
          recurseType(f.dataType, indent + 1)
        }

    def recurseType(dt: DataType, indent: Int): Unit =
      if indent < maxLevel then
        dt match
          case st: StructType     => buildTree(st.fields, indent)
          case ArrayType(et, _)   => recurseType(et, indent)
          case MapType(kt, vt, _) => recurseType(kt, indent); recurseType(vt, indent)
          case _                  => ()
    buildTree(fields, 1)
    sb.toString

object StructType:
  val empty: StructType = StructType(Seq.empty)

  def fromDDL(ddl: String): StructType =
    DataType.DDLParser(ddl).parseTableSchema()

  def apply(fields: Array[StructField]): StructType = StructType(fields.toIndexedSeq)

  def apply(fields: java.util.List[StructField]): StructType =
    import scala.jdk.CollectionConverters.*
    StructType(fields.asScala.toSeq)

/** Represents an unparsed data type returned by the server as a raw type string.
  *
  * This is used when the server sends a DataType.Unparsed proto that this client cannot resolve to
  * a concrete type.
  */
final case class UnparsedDataType(typeString: String) extends DataType:
  def typeName = "unparsed"
  override def simpleString = s"unparsed($typeString)"
  override def sql = typeString
