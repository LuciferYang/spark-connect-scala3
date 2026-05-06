package org.apache.spark.sql.types

import java.io.Serializable
import java.util.Arrays as JArrays

/** The physical data representation of [[VariantType]] — a semi-structured value consisting of two
  * binary components:
  *   - `value`: encodes types and values (but not field names)
  *   - `metadata`: contains a version flag and a list of field names
  *
  * This is the Scala 3 equivalent of upstream Spark's `org.apache.spark.unsafe.types.VariantVal`.
  * It is produced when deserializing Arrow batches containing Variant columns (which the server
  * encodes as a struct with two binary children: "value" + "metadata" with Arrow field metadata
  * `variant=true`).
  */
final class VariantVal(val value: Array[Byte], val metadata: Array[Byte]) extends Serializable:

  def getValue: Array[Byte] = value
  def getMetadata: Array[Byte] = metadata

  def debugString: String =
    s"VariantVal{value=${JArrays.toString(value)}, metadata=${JArrays.toString(metadata)}}"

  override def toString: String = debugString

  override def equals(other: Any): Boolean = other match
    case o: VariantVal =>
      JArrays.equals(value, o.value) && JArrays.equals(metadata, o.metadata)
    case _ => false

  override def hashCode(): Int =
    31 * JArrays.hashCode(value) + JArrays.hashCode(metadata)
