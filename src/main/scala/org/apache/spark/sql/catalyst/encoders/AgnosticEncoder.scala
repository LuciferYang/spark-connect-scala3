package org.apache.spark.sql.catalyst.encoders

import org.apache.spark.sql.types.*

import java.io.{ObjectStreamException, Serializable}
import java.time.{Instant, LocalDate, LocalDateTime}
import scala.reflect.ClassTag

/** Minimal stub of upstream Spark's `AgnosticEncoder` hierarchy.
  *
  * The server-side `SparkConnectPlanner` deserializes `UdfPacket` which contains `AgnosticEncoder`
  * instances. These stubs use `writeReplace` to substitute an `EncoderSerializationProxy` that
  * carries only a type name. On the server side, the proxy's `readResolve` uses reflection to look
  * up the real encoder singleton from the server's own `AgnosticEncoders` object (loaded by the
  * parent classloader). This avoids all serialVersionUID and class-structure mismatches between our
  * Scala 3 stubs and the server's Scala 2.13 classes.
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

  /** Encoder for unbound Row types used in foreachBatch/foreach. */
  case object UnboundRowEncoder
      extends LeafEncoder[org.apache.spark.sql.Row](
        StructType(Seq.empty),
        "UnboundRowEncoder"
      )

  // ---------------------------------------------------------------------------
  // Parameterized Encoders (Date, Timestamp, Decimal, etc.)
  // ---------------------------------------------------------------------------

  /** Base class for parameterized encoders that need constructor args on the server side.
    *
    * Unlike singleton `case object` encoders, these use `ParameterizedEncoderProxy` to serialize
    * the encoder name and constructor args. On the server side, the proxy reconstructs the encoder
    * via reflection.
    */
  abstract class ParameterizedEncoder[E: ClassTag](
      val dataType: DataType,
      val encoderName: String
  ) extends AgnosticEncoder[E]:
    override def isPrimitive: Boolean = false
    override val clsTag: ClassTag[E] = summon[ClassTag[E]]

  case class DateEncoder(lenient: Boolean)
      extends ParameterizedEncoder[java.sql.Date](DateType, "DateEncoder"):
    override def lenientSerialization: Boolean = lenient
    @throws[ObjectStreamException]
    private def writeReplace(): AnyRef =
      ParameterizedEncoderProxy(
        "DateEncoder",
        Array(java.lang.Boolean.valueOf(lenient)),
        Array(classOf[Boolean])
      )

  case class LocalDateEncoder(lenient: Boolean)
      extends ParameterizedEncoder[LocalDate](DateType, "LocalDateEncoder"):
    override def lenientSerialization: Boolean = lenient
    @throws[ObjectStreamException]
    private def writeReplace(): AnyRef =
      ParameterizedEncoderProxy(
        "LocalDateEncoder",
        Array(java.lang.Boolean.valueOf(lenient)),
        Array(classOf[Boolean])
      )

  case class TimestampEncoder(lenient: Boolean)
      extends ParameterizedEncoder[java.sql.Timestamp](TimestampType, "TimestampEncoder"):
    override def lenientSerialization: Boolean = lenient
    @throws[ObjectStreamException]
    private def writeReplace(): AnyRef =
      ParameterizedEncoderProxy(
        "TimestampEncoder",
        Array(java.lang.Boolean.valueOf(lenient)),
        Array(classOf[Boolean])
      )

  case class InstantEncoder(lenient: Boolean)
      extends ParameterizedEncoder[Instant](TimestampType, "InstantEncoder"):
    override def lenientSerialization: Boolean = lenient
    @throws[ObjectStreamException]
    private def writeReplace(): AnyRef =
      ParameterizedEncoderProxy(
        "InstantEncoder",
        Array(java.lang.Boolean.valueOf(lenient)),
        Array(classOf[Boolean])
      )

  case object LocalDateTimeEncoder
      extends LeafEncoder[LocalDateTime](TimestampNTZType, "LocalDateTimeEncoder")

  case class ScalaDecimalEncoder(dt: DecimalType)
      extends ParameterizedEncoder[BigDecimal](dt, "ScalaDecimalEncoder"):
    @throws[ObjectStreamException]
    private def writeReplace(): AnyRef =
      DecimalEncoderProxy(
        "ScalaDecimalEncoder",
        dt.precision,
        dt.scale,
        Array.empty,
        Array.empty
      )

  case class JavaDecimalEncoder(dt: DecimalType, lenient: Boolean)
      extends ParameterizedEncoder[java.math.BigDecimal](dt, "JavaDecimalEncoder"):
    override def lenientSerialization: Boolean = lenient
    @throws[ObjectStreamException]
    private def writeReplace(): AnyRef =
      DecimalEncoderProxy(
        "JavaDecimalEncoder",
        dt.precision,
        dt.scale,
        Array(java.lang.Boolean.valueOf(lenient)),
        Array(classOf[Boolean])
      )

  case object ScalaBigIntEncoder
      extends LeafEncoder[BigInt](DecimalType.DEFAULT, "ScalaBigIntEncoder")

  // Convenience constants
  val STRICT_DATE_ENCODER: DateEncoder = DateEncoder(false)
  val STRICT_LOCAL_DATE_ENCODER: LocalDateEncoder = LocalDateEncoder(false)
  val STRICT_TIMESTAMP_ENCODER: TimestampEncoder = TimestampEncoder(false)
  val STRICT_INSTANT_ENCODER: InstantEncoder = InstantEncoder(false)
  val DEFAULT_SCALA_DECIMAL_ENCODER: ScalaDecimalEncoder = ScalaDecimalEncoder(DecimalType.DEFAULT)
  val DEFAULT_JAVA_DECIMAL_ENCODER: JavaDecimalEncoder = JavaDecimalEncoder(
    DecimalType.DEFAULT,
    false
  )

  // ---------------------------------------------------------------------------
  // Collection Type Encoders
  // ---------------------------------------------------------------------------

  /** Encoder for Option[E] values. */
  @SerialVersionUID(1L)
  case class OptionEncoder[E](element: AgnosticEncoder[E])
      extends AgnosticEncoder[Option[E]]
      with Serializable:
    override def isPrimitive: Boolean = false
    override def nullable: Boolean = true
    override def dataType: DataType = element.dataType
    override val clsTag: ClassTag[Option[E]] =
      ClassTag(classOf[Option[?]])
    @throws[ObjectStreamException]
    private def writeReplace(): AnyRef =
      CollectionEncoderProxy("OptionEncoder", element, None, None, None)

  /** Encoder for Array[E] values. */
  @SerialVersionUID(1L)
  case class ArrayEncoder[E](element: AgnosticEncoder[E], containsNull: Boolean)
      extends AgnosticEncoder[Array[E]]
      with Serializable:
    override def isPrimitive: Boolean = false
    override def dataType: DataType = ArrayType(element.dataType, containsNull)
    override val clsTag: ClassTag[Array[E]] =
      ClassTag(classOf[Array[?]])
    @throws[ObjectStreamException]
    private def writeReplace(): AnyRef =
      CollectionEncoderProxy(
        "ArrayEncoder",
        element,
        Some(containsNull),
        None,
        None
      )

  /** Encoder for Iterable-like collection types (Seq, List, etc.). */
  @SerialVersionUID(1L)
  case class IterableEncoder[C, E](
      override val clsTag: ClassTag[C],
      element: AgnosticEncoder[E],
      containsNull: Boolean
  ) extends AgnosticEncoder[C]
      with Serializable:
    override def isPrimitive: Boolean = false
    override def dataType: DataType = ArrayType(element.dataType, containsNull)
    @throws[ObjectStreamException]
    private def writeReplace(): AnyRef =
      CollectionEncoderProxy(
        "IterableEncoder",
        element,
        Some(containsNull),
        Some(clsTag.runtimeClass.getName),
        None
      )

  /** Encoder for Map[K, V] types. */
  @SerialVersionUID(1L)
  case class MapEncoder[C, K, V](
      override val clsTag: ClassTag[C],
      keyEncoder: AgnosticEncoder[K],
      valueEncoder: AgnosticEncoder[V],
      valueContainsNull: Boolean
  ) extends AgnosticEncoder[C]
      with Serializable:
    override def isPrimitive: Boolean = false
    override def dataType: DataType =
      MapType(keyEncoder.dataType, valueEncoder.dataType, valueContainsNull)
    @throws[ObjectStreamException]
    private def writeReplace(): AnyRef =
      MapEncoderProxy(
        clsTag.runtimeClass.getName,
        keyEncoder,
        valueEncoder,
        valueContainsNull
      )

  // ---------------------------------------------------------------------------
  // Product (case class) Encoder stubs
  // ---------------------------------------------------------------------------

  /** Metadata placeholder (matches server-side Metadata.empty). */
  case class Metadata(json: String = "{}")
  object Metadata:
    val empty: Metadata = Metadata()

  /** Describes a single field within a ProductEncoder. */
  case class EncoderField(
      name: String,
      enc: AgnosticEncoder[?],
      nullable: Boolean,
      metadata: Metadata,
      readMethod: Option[String] = None,
      writeMethod: Option[String] = None
  )

  /** Encoder for Product types (case classes, tuples).
    *
    * The `fields` describe each element's name, encoder, and nullability. On the server side this
    * is reconstructed and used to build InternalRow encoders.
    */
  @SerialVersionUID(1L)
  case class ProductEncoder[K](
      override val clsTag: ClassTag[K],
      fields: Seq[EncoderField],
      outerPointerGetter: Option[() => AnyRef] = None
  ) extends AgnosticEncoder[K]
      with Serializable:
    override def isPrimitive: Boolean = false
    override def dataType: DataType =
      StructType(fields.map(f => StructField(f.name, f.enc.dataType, f.nullable)))
    @throws[ObjectStreamException]
    private def writeReplace(): AnyRef =
      ProductEncoderProxy(
        clsTag.runtimeClass.getName,
        fields.map(f =>
          EncoderFieldProxy(
            f.name,
            f.enc,
            f.nullable,
            f.metadata.json,
            f.readMethod,
            f.writeMethod
          ): AnyRef
        ).toArray
      )

/** Serialization proxy for [[AgnosticEncoder]] instances.
  *
  * When a Scala 3 encoder stub is serialized, `writeReplace` substitutes this proxy. On the server
  * side (Scala 2.13), `readResolve` uses reflection to look up the real encoder singleton from the
  * server's own classes (loaded by the parent classloader). This completely avoids serialVersionUID
  * and bytecode-layout mismatches between Scala 3 and Scala 2.13 classes.
  *
  * In Scala 2.13, `case object PrimitiveIntEncoder` inside `object AgnosticEncoders` compiles to a
  * class `AgnosticEncoders$PrimitiveIntEncoder$` with a static `MODULE$` field holding the
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

/** Serialization proxy for parameterized [[AgnosticEncoder]] instances.
  *
  * Unlike `EncoderSerializationProxy` which only handles singletons (`case object`), this proxy
  * carries constructor arguments to reconstruct parameterized encoders (e.g., `DateEncoder(false)`,
  * `InstantEncoder(true)`) on the server side.
  */
@SerialVersionUID(1L)
final class ParameterizedEncoderProxy(
    val encoderName: String,
    val args: Array[AnyRef],
    val argTypes: Array[Class[?]]
) extends Serializable:

  @throws[ObjectStreamException]
  private def readResolve(): AnyRef =
    val cl = getClass.getClassLoader match
      case null => ClassLoader.getSystemClassLoader
      case c    => Option(c.getParent).getOrElse(c)
    val className =
      s"org.apache.spark.sql.catalyst.encoders.AgnosticEncoders$$$encoderName"
    val clazz = Class.forName(className, true, cl)
    val ctor = clazz.getConstructors
      .find { c =>
        c.getParameterCount == argTypes.length &&
        c.getParameterTypes.zip(argTypes).forall { (actual, expected) =>
          actual.isAssignableFrom(expected) ||
          // Handle primitive/boxed mismatch
          (actual == java.lang.Boolean.TYPE && expected == classOf[Boolean])
        }
      }
      .getOrElse(
        throw ClassNotFoundException(s"No matching constructor for $encoderName")
      )
    ctor.newInstance(args*)

/** Serialization proxy for Decimal-based [[AgnosticEncoder]] instances.
  *
  * The server-side `DecimalType` is a Scala 2.13 case class that needs to be reconstructed via
  * `DecimalType.apply(precision, scale)`. This proxy handles that reconstruction along with any
  * additional constructor arguments.
  */
@SerialVersionUID(1L)
final class DecimalEncoderProxy(
    val encoderName: String,
    val precision: Int,
    val scale: Int,
    val extraArgs: Array[AnyRef],
    val extraArgTypes: Array[Class[?]]
) extends Serializable:

  @throws[ObjectStreamException]
  private def readResolve(): AnyRef =
    val cl = getClass.getClassLoader match
      case null => ClassLoader.getSystemClassLoader
      case c    => Option(c.getParent).getOrElse(c)
    // Reconstruct the server-side DecimalType via DecimalType.apply(precision, scale)
    val dtClass = Class.forName(
      "org.apache.spark.sql.types.DecimalType$",
      true,
      cl
    )
    val dtModule = dtClass.getField("MODULE$").get(null)
    val applyMethod = dtClass.getMethod("apply", classOf[Int], classOf[Int])
    val dt = applyMethod.invoke(
      dtModule,
      java.lang.Integer.valueOf(precision),
      java.lang.Integer.valueOf(scale)
    )
    // Reconstruct the encoder
    val encClassName =
      s"org.apache.spark.sql.catalyst.encoders.AgnosticEncoders$$$encoderName"
    val encClass = Class.forName(encClassName, true, cl)
    val dtBaseClass = Class.forName("org.apache.spark.sql.types.DecimalType", true, cl)
    val allArgs: Array[AnyRef] = Array(dt) ++ extraArgs
    val allArgTypes: Array[Class[?]] = Array(dtBaseClass) ++ extraArgTypes
    val ctor = encClass.getConstructors
      .find { c =>
        c.getParameterCount == allArgTypes.length &&
        c.getParameterTypes.zip(allArgTypes).forall { (actual, expected) =>
          actual.isAssignableFrom(expected) ||
          (actual == java.lang.Boolean.TYPE && expected == classOf[Boolean])
        }
      }
      .getOrElse(
        throw ClassNotFoundException(s"No matching constructor for $encoderName")
      )
    ctor.newInstance(allArgs*)

/** Serialization proxy for a single field within a [[ProductEncoder]].
  *
  * Each field carries its encoder (which will use its own `writeReplace`), name, nullability, and
  * metadata JSON. On the server side the proxy's `readResolve` reconstructs the server's
  * `EncoderField` case class.
  */
@SerialVersionUID(1L)
final class EncoderFieldProxy(
    val name: String,
    val enc: AgnosticEncoder[?],
    val nullable: Boolean,
    val metadataJson: String,
    val readMethod: Option[String],
    val writeMethod: Option[String]
) extends Serializable:
  @throws[ObjectStreamException]
  private def readResolve(): AnyRef =
    val cl = getClass.getClassLoader match
      case null => ClassLoader.getSystemClassLoader
      case c    => Option(c.getParent).getOrElse(c)
    // Reconstruct server-side Metadata
    val metaClass = Class.forName(
      "org.apache.spark.sql.types.Metadata$",
      true,
      cl
    )
    val metaModule = metaClass.getField("MODULE$").get(null)
    val fromJsonMethod = metaClass.getMethod("fromJson", classOf[String])
    val metadata = fromJsonMethod.invoke(metaModule, metadataJson)
    // Reconstruct server-side EncoderField
    val efClass = Class.forName(
      "org.apache.spark.sql.catalyst.encoders.AgnosticEncoders$EncoderField",
      true,
      cl
    )
    val ctor = efClass.getConstructors
      .find(_.getParameterCount == 6)
      .getOrElse(
        throw ClassNotFoundException(s"No 6-arg constructor for EncoderField")
      )
    ctor.newInstance(
      name,
      enc,
      java.lang.Boolean.valueOf(nullable),
      metadata,
      readMethod,
      writeMethod
    )

/** Serialization proxy for [[ProductEncoder]].
  *
  * Stores the class name (for ClassTag) and field descriptors. On the server side, `readResolve`
  * uses reflection to construct the server's `ProductEncoder` case class with the reconstructed
  * `ClassTag`, fields, and `outerPointerGetter = None`.
  */
@SerialVersionUID(1L)
final class ProductEncoderProxy(
    val className: String,
    val fields: Array[AnyRef]
) extends Serializable:
  @throws[ObjectStreamException]
  private def readResolve(): AnyRef =
    // Use parent classloader for server-side Spark/Scala classes
    val serverCl = getClass.getClassLoader match
      case null => ClassLoader.getSystemClassLoader
      case c    => Option(c.getParent).getOrElse(c)
    // Use context classloader (session classloader) for user classes like Person, Dept etc.
    val sessionCl = Thread.currentThread().getContextClassLoader match
      case null => serverCl
      case c    => c
    // Reconstruct ClassTag on the server side
    val clsTagModule = Class.forName("scala.reflect.ClassTag$", true, serverCl)
    val ctModule = clsTagModule.getField("MODULE$").get(null)
    val applyMethod =
      clsTagModule.getMethod("apply", classOf[Class[?]])
    // Load the user's class with session classloader (it may be an uploaded artifact)
    val runtimeClass = Class.forName(className, true, sessionCl)
    val clsTag = applyMethod.invoke(ctModule, runtimeClass)
    // Resolve each field (EncoderFieldProxy.readResolve will be called by Java serialization)
    val resolvedFields = fields.toSeq
    // Reconstruct server-side ProductEncoder
    val peClass = Class.forName(
      "org.apache.spark.sql.catalyst.encoders.AgnosticEncoders$ProductEncoder",
      true,
      serverCl
    )
    val ctor = peClass.getConstructors
      .find(_.getParameterCount == 3)
      .getOrElse(
        throw ClassNotFoundException("No 3-arg constructor for ProductEncoder")
      )
    ctor.newInstance(clsTag, resolvedFields, None)

/** Serialization proxy for collection-type encoders: [[OptionEncoder]], [[ArrayEncoder]],
  * [[IterableEncoder]].
  *
  * These share a common pattern: an encoder name, an element encoder, optional containsNull flag,
  * and optional class name (for ClassTag in IterableEncoder).
  */
@SerialVersionUID(1L)
final class CollectionEncoderProxy(
    val encoderName: String,
    val element: AgnosticEncoder[?],
    val containsNull: Option[Boolean],
    val className: Option[String],
    val lenientSerialization: Option[Boolean]
) extends Serializable:
  @throws[ObjectStreamException]
  private def readResolve(): AnyRef =
    val cl = getClass.getClassLoader match
      case null => ClassLoader.getSystemClassLoader
      case c    => Option(c.getParent).getOrElse(c)
    val encPkg = "org.apache.spark.sql.catalyst.encoders.AgnosticEncoders$"
    encoderName match
      case "OptionEncoder" =>
        val clazz = Class.forName(s"${encPkg}OptionEncoder", true, cl)
        val ctor = clazz.getConstructors.find(_.getParameterCount == 1).get
        ctor.newInstance(element)
      case "ArrayEncoder" =>
        val clazz = Class.forName(s"${encPkg}ArrayEncoder", true, cl)
        val ctor = clazz.getConstructors.find(_.getParameterCount == 2).get
        ctor.newInstance(element, java.lang.Boolean.valueOf(containsNull.getOrElse(false)))
      case "IterableEncoder" =>
        val clazz = Class.forName(s"${encPkg}IterableEncoder", true, cl)
        val ctor = clazz.getConstructors.find(_.getParameterCount == 4).get
        val runtimeClass = Class.forName(className.get, true, cl)
        val clsTagModule = Class.forName("scala.reflect.ClassTag$", true, cl)
        val ctModule = clsTagModule.getField("MODULE$").get(null)
        val applyMethod = clsTagModule.getMethod("apply", classOf[Class[?]])
        val clsTag = applyMethod.invoke(ctModule, runtimeClass)
        ctor.newInstance(
          clsTag,
          element,
          java.lang.Boolean.valueOf(containsNull.getOrElse(false)),
          java.lang.Boolean.valueOf(lenientSerialization.getOrElse(false))
        )
      case other =>
        throw ClassNotFoundException(s"Unknown collection encoder: $other")

/** Serialization proxy for [[MapEncoder]].
  *
  * Reconstructs the server-side `MapEncoder` case class via reflection, carrying the ClassTag class
  * name, key/value encoders, and valueContainsNull flag.
  */
@SerialVersionUID(1L)
final class MapEncoderProxy(
    val className: String,
    val keyEncoder: AgnosticEncoder[?],
    val valueEncoder: AgnosticEncoder[?],
    val valueContainsNull: Boolean
) extends Serializable:
  @throws[ObjectStreamException]
  private def readResolve(): AnyRef =
    val cl = getClass.getClassLoader match
      case null => ClassLoader.getSystemClassLoader
      case c    => Option(c.getParent).getOrElse(c)
    val clazz = Class.forName(
      "org.apache.spark.sql.catalyst.encoders.AgnosticEncoders$MapEncoder",
      true,
      cl
    )
    val runtimeClass = Class.forName(className, true, cl)
    val clsTagModule = Class.forName("scala.reflect.ClassTag$", true, cl)
    val ctModule = clsTagModule.getField("MODULE$").get(null)
    val applyMethod = clsTagModule.getMethod("apply", classOf[Class[?]])
    val clsTag = applyMethod.invoke(ctModule, runtimeClass)
    val ctor = clazz.getConstructors
      .find(_.getParameterCount == 4)
      .getOrElse(
        throw ClassNotFoundException("No 4-arg constructor for MapEncoder")
      )
    ctor.newInstance(
      clsTag,
      keyEncoder,
      valueEncoder,
      java.lang.Boolean.valueOf(valueContainsNull)
    )
