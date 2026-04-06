package org.apache.spark.sql.connect.common

import java.io.{
  ByteArrayOutputStream,
  ObjectInputStream,
  ObjectOutputStream,
  Serializable
}

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
  /** Serialize a UdfPacket to bytes using Java serialization. */
  def serialize(packet: UdfPacket): Array[Byte] =
    val bos = ByteArrayOutputStream()
    val oos = ObjectOutputStream(bos)
    try
      oos.writeObject(packet)
      oos.flush()
      bos.toByteArray
    finally
      oos.close()
      bos.close()
