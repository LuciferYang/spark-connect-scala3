package org.apache.spark.sql.connect.common

import java.io.{ByteArrayOutputStream, ObjectOutputStream, Serializable}

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder

/** A serializable wrapper for a foreach writer/batch function and its encoder.
  *
  * The server-side Spark Connect planner deserializes this as a `ForeachWriterPacket`, so the
  * serialVersionUID must match exactly.
  */
@SerialVersionUID(3882541391565582579L)
final class ForeachWriterPacket(
    val foreachWriter: AnyRef,
    val datasetEncoder: AgnosticEncoder[?]
) extends Serializable

object ForeachWriterPacket:
  def serialize(packet: ForeachWriterPacket): Array[Byte] =
    val bos = ByteArrayOutputStream()
    val oos = ObjectOutputStream(bos)
    try
      oos.writeObject(packet)
      oos.flush()
      bos.toByteArray
    finally
      oos.close()
      bos.close()
