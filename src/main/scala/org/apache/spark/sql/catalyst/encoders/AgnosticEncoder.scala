package org.apache.spark.sql.catalyst.encoders

import org.apache.spark.sql.types.*

import java.io.{ObjectStreamException, Serializable}
import scala.reflect.ClassTag

/** Minimal stub of upstream Spark's `AgnosticEncoder` hierarchy.
  *
  * The server-side `SparkConnectPlanner` deserializes `UdfPacket` which contains `AgnosticEncoder`
  * instances. These stubs use `writeReplace` to substitute a [[EncoderSerializationProxy]] that
  * carries only a type name. On the server side, the proxy's `readResolve` uses reflection to
  * look up the real encoder singleton from the server's own `AgnosticEncoders` object (loaded by
  * the parent classloader). This avoids all serialVersionUID and class-structure mismatches
  * between our Scala 3 stubs and the server's Scala 2.13 classes.
  */
trait AgnosticEncoder[T] extends Serializable:
  def isPrimitive: Boolean
  def nullable: Boolean = !isPrimitive
  def dataType: DataType
  def lenientSerialization: Boolean = false
  def clsTag: ClassTag[T]

object AgnosticEncoders:
  /** Base class for leaf encoders that use writeReplace to produce a cross-version-compatible
    * serialization proxy.
    */
  abstract class LeafEncoder[E: ClassTag](val dataType: DataType, val encoderName: String)
      extends AgnosticEncoder[E]:
    override def isPrimitive: Boolean = false
    override val clsTag: ClassTag[E] = summon[ClassTag[E]]

    @throws[ObjectStreamException]
    protected def writeReplace(): AnyRef = EncoderSerializationProxy(encoderName)

  abstract class PrimitiveLeafEncoder[E: ClassTag](dataType: DataType, encoderName: String)
      extends LeafEncoder[E](dataType, encoderName):
    override def isPrimitive: Boolean = true

  abstract class BoxedLeafEncoder[E: ClassTag, P](
      dataType: DataType,
      encoderName: String,
      val primitive: PrimitiveLeafEncoder[P]
  ) extends LeafEncoder[E](dataType, encoderName)

  case object PrimitiveBooleanEncoder
      extends PrimitiveLeafEncoder[Boolean](BooleanType, "PrimitiveBooleanEncoder")
  case object PrimitiveByteEncoder
      extends PrimitiveLeafEncoder[Byte](ByteType, "PrimitiveByteEncoder")
  case object PrimitiveShortEncoder
      extends PrimitiveLeafEncoder[Short](ShortType, "PrimitiveShortEncoder")
  case object PrimitiveIntEncoder
      extends PrimitiveLeafEncoder[Int](IntegerType, "PrimitiveIntEncoder")
  case object PrimitiveLongEncoder
      extends PrimitiveLeafEncoder[Long](LongType, "PrimitiveLongEncoder")
  case object PrimitiveFloatEncoder
      extends PrimitiveLeafEncoder[Float](FloatType, "PrimitiveFloatEncoder")
  case object PrimitiveDoubleEncoder
      extends PrimitiveLeafEncoder[Double](DoubleType, "PrimitiveDoubleEncoder")

  case object BoxedBooleanEncoder
      extends BoxedLeafEncoder[java.lang.Boolean, Boolean](
        BooleanType,
        "BoxedBooleanEncoder",
        PrimitiveBooleanEncoder
      )
  case object BoxedByteEncoder
      extends BoxedLeafEncoder[java.lang.Byte, Byte](
        ByteType,
        "BoxedByteEncoder",
        PrimitiveByteEncoder
      )
  case object BoxedShortEncoder
      extends BoxedLeafEncoder[java.lang.Short, Short](
        ShortType,
        "BoxedShortEncoder",
        PrimitiveShortEncoder
      )
  case object BoxedIntEncoder
      extends BoxedLeafEncoder[java.lang.Integer, Int](
        IntegerType,
        "BoxedIntEncoder",
        PrimitiveIntEncoder
      )
  case object BoxedLongEncoder
      extends BoxedLeafEncoder[java.lang.Long, Long](
        LongType,
        "BoxedLongEncoder",
        PrimitiveLongEncoder
      )
  case object BoxedFloatEncoder
      extends BoxedLeafEncoder[java.lang.Float, Float](
        FloatType,
        "BoxedFloatEncoder",
        PrimitiveFloatEncoder
      )
  case object BoxedDoubleEncoder
      extends BoxedLeafEncoder[java.lang.Double, Double](
        DoubleType,
        "BoxedDoubleEncoder",
        PrimitiveDoubleEncoder
      )

  case object NullEncoder extends LeafEncoder[java.lang.Void](NullType, "NullEncoder")
  case object StringEncoder extends LeafEncoder[String](StringType, "StringEncoder")
  case object BinaryEncoder extends LeafEncoder[Array[Byte]](BinaryType, "BinaryEncoder")

/** Serialization proxy for [[AgnosticEncoder]] instances.
  *
  * When a Scala 3 encoder stub is serialized, `writeReplace` substitutes this proxy. On the
  * server side (Scala 2.13), `readResolve` uses reflection to look up the real encoder singleton
  * from the server's own classes (loaded by the parent classloader). This completely avoids
  * serialVersionUID and bytecode-layout mismatches between Scala 3 and Scala 2.13 classes.
  *
  * In Scala 2.13, `case object PrimitiveIntEncoder` inside `object AgnosticEncoders` compiles to
  * a class `AgnosticEncoders$PrimitiveIntEncoder$` with a static `MODULE$` field holding the
  * singleton. We use this knowledge to look up the encoder via reflection.
  */
@SerialVersionUID(1L)
final class EncoderSerializationProxy(val encoderName: String) extends Serializable:

  @throws[ObjectStreamException]
  private def readResolve(): AnyRef =
    // Use the parent classloader to get the server's version of the encoder class.
    val cl = getClass.getClassLoader match
      case null => ClassLoader.getSystemClassLoader
      case c    => Option(c.getParent).getOrElse(c)
    // In Scala 2.13, case object Foo inside object Bar compiles to class Bar$Foo$
    // with a static MODULE$ field.
    val className =
      s"org.apache.spark.sql.catalyst.encoders.AgnosticEncoders$$$encoderName$$"
    val clazz = Class.forName(className, true, cl)
    clazz.getField("MODULE$").get(null)
