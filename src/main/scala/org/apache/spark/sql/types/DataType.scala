package org.apache.spark.sql.types

/** The base type of all Spark SQL data types. */
sealed trait DataType:
  def typeName: String
  def simpleString: String = typeName

case object BooleanType extends DataType:
  def typeName = "boolean"

case object ByteType extends DataType:
  def typeName = "byte"

case object ShortType extends DataType:
  def typeName = "short"

case object IntegerType extends DataType:
  def typeName = "integer"

case object LongType extends DataType:
  def typeName = "long"

case object FloatType extends DataType:
  def typeName = "float"

case object DoubleType extends DataType:
  def typeName = "double"

case object StringType extends DataType:
  def typeName = "string"

case object BinaryType extends DataType:
  def typeName = "binary"

case object DateType extends DataType:
  def typeName = "date"

case object TimestampType extends DataType:
  def typeName = "timestamp"

case object TimestampNTZType extends DataType:
  def typeName = "timestamp_ntz"

case object NullType extends DataType:
  def typeName = "null"

final case class DecimalType(precision: Int, scale: Int) extends DataType:
  def typeName = "decimal"
  override def simpleString = s"decimal($precision,$scale)"

object DecimalType:
  val DEFAULT: DecimalType = DecimalType(10, 0)

final case class ArrayType(elementType: DataType, containsNull: Boolean) extends DataType:
  def typeName = "array"
  override def simpleString = s"array<${elementType.simpleString}>"

final case class MapType(keyType: DataType, valueType: DataType, valueContainsNull: Boolean)
    extends DataType:
  def typeName = "map"
  override def simpleString = s"map<${keyType.simpleString},${valueType.simpleString}>"

final case class StructField(name: String, dataType: DataType, nullable: Boolean = true)

final case class StructType(fields: Seq[StructField]) extends DataType:
  def typeName = "struct"

  def apply(name: String): StructField =
    fields.find(_.name == name).getOrElse(
      throw java.util.NoSuchElementException(s"Field '$name' not found in $this")
    )

  def fieldNames: Array[String] = fields.map(_.name).toArray

  def fieldIndex(name: String): Int =
    fields.indexWhere(_.name == name) match
      case -1 =>
        throw IllegalArgumentException(s"Field '$name' not found in $this")
      case i => i

  override def simpleString: String =
    s"struct<${fields.map(f => s"${f.name}:${f.dataType.simpleString}").mkString(",")}>"

  def treeString: String =
    val sb = StringBuilder()
    sb.append("root\n")
    fields.foreach { f =>
      val nullStr = if f.nullable then "nullable = true" else "nullable = false"
      sb.append(s" |-- ${f.name}: ${f.dataType.simpleString} ($nullStr)\n")
    }
    sb.toString

object StructType:
  val empty: StructType = StructType(Seq.empty)
