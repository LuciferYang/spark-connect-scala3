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

  /** Convert a user-type instance to its SQL-storable representation. */
  def serialize(obj: UserType): Any

  /** Convert a SQL-stored value back to the user-type instance. */
  def deserialize(datum: Any): UserType

  /** The `Class` of the user type. */
  def userClass: Class[UserType]

  def typeName: String = s"udt(${getClass.getName})"

  override def sql: String = sqlType.sql

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other match
    case that: UserDefinedType[?] => this.getClass == that.getClass
    case _                        => false
