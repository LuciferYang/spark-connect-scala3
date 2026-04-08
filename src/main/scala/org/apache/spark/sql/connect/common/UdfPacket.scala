package org.apache.spark.sql.connect.common

import java.io.{
  ByteArrayOutputStream,
  ObjectInputStream,
  ObjectOutputStream,
  Serializable
}

import scala.collection.mutable

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder

/** A serializable wrapper for a UDF function and its encoder metadata.
  *
  * The server-side Spark Connect planner deserializes the UDF payload as a `UdfPacket`, so the
  * client must produce a serialization-compatible byte stream. The server's `UdfPacket` is a
  * standard Scala 2.13 case class that uses `defaultWriteObject`/`defaultReadObject`.
  *
  * This class uses `defaultWriteObject()` to produce the same wire format. The `AgnosticEncoder`
  * instances use `writeReplace` to substitute an `EncoderSerializationProxy` that, on the server
  * side, resolves to the server's own encoder singletons via reflection. This avoids
  * serialVersionUID mismatches between Scala 3 and Scala 2.13 classes.
  */
@SerialVersionUID(8866761834651399125L)
final class UdfPacket(
    val function: AnyRef,
    val inputEncoders: Seq[AgnosticEncoder[?]],
    val outputEncoder: AgnosticEncoder[?]
) extends Serializable

object UdfPacket:
  /** Serialize a UdfPacket to bytes using Java serialization.
    *
    * Before serializing, runs the user function through `ClosureCleaner.clean(...,
    * cleanTransitively = true, ...)`. The cleaner walks the closure's enclosing-class chain via
    * ASM, identifies which `this$0` outer references the closure actually uses, and nulls out the
    * unused ones. Without this step, Scala 3 lambdas often capture the test suite, the
    * SparkSession, and the gRPC client, none of which are deserializable on a Scala 2.13 server —
    * yielding the generic "Failed to unpack scala udf" error.
    *
    * Note: SC3 deliberately does NOT perform a client-side round-trip
    * (`ObjectInputStream.readObject`) check the way upstream Spark Connect does. SC3's
    * `EncoderSerializationProxy` resolves to a Scala 2.13 server-side encoder class that does not
    * exist on the SC3 client classpath, so a client-side round-trip would always fail at the
    * encoder boundary even when the closure is fine.
    */
  def serialize(packet: UdfPacket): Array[Byte] =
    val cleanedFunction = ClosureCleaner
      .clean(packet.function, cleanTransitively = true, mutable.Map.empty)
      .getOrElse(packet.function)
    val cleanedPacket =
      if cleanedFunction eq packet.function then packet
      else UdfPacket(cleanedFunction, packet.inputEncoders, packet.outputEncoder)

    val bos = ByteArrayOutputStream()
    val oos = ObjectOutputStream(bos)
    try
      oos.writeObject(cleanedPacket)
      oos.flush()
      bos.toByteArray
    finally
      oos.close()
      bos.close()
