package org.apache.spark.sql.types

/** Abstract class for user-defined types (UDTs).
  *
  * A UDT bridges a custom user-facing type (`UserType`) and a built-in Spark SQL type (`sqlType`).
  * The `serialize` / `deserialize` methods handle conversion between the two representations.
  *
  * This is a minimal port of the upstream `UserDefinedType` — Python UDT support, json4s
  * serialization, and the `@SQLUserDefinedType` annotation are not included.
  */
abstract class UserDefinedType[UserType >: Null] extends DataType with Serializable:

  /** The underlying Spark SQL DataType used to store this UDT. */
  def sqlType: DataType

  /** Paired Python UDT class, if one exists. */
  def pyUDT: String = null

  /** Serialized Python UDT class, if one exists. */
  def serializedPyClass: String = null

  /** Convert a user-type instance to its SQL-storable representation. */
  def serialize(obj: UserType): Any

  /** Convert a SQL-stored value back to the user-type instance. */
  def deserialize(datum: Any): UserType

  /** The `Class` of the user type. */
  def userClass: Class[UserType]

  def typeName: String = s"udt(${getClass.getName})"

  override def sql: String = sqlType.sql

  override def catalogString: String = sqlType.catalogString

  override def defaultSize: Int = sqlType.defaultSize

  /** Convert a UDT value to a string representation. */
  def stringifyValue(obj: Any): String = obj.toString

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other match
    case that: UserDefinedType[?] => this.getClass == that.getClass
    case _                        => false

private[sql] class PythonUserDefinedType(
    val sqlType: DataType,
    override val pyUDT: String,
    override val serializedPyClass: String
) extends UserDefinedType[Any]:

  override def serialize(obj: Any): Any = obj
  override def deserialize(datum: Any): Any = datum
  override def userClass: Class[Any] = null

  override def equals(other: Any): Boolean = other match
    case that: PythonUserDefinedType => pyUDT == that.pyUDT
    case _                           => false

  override def hashCode(): Int = java.util.Objects.hashCode(pyUDT)
