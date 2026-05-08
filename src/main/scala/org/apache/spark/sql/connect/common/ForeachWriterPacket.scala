package org.apache.spark.sql.connect.common

import java.io.{ByteArrayOutputStream, ObjectOutputStream, Serializable}
import java.lang.invoke.SerializedLambda

import scala.collection.mutable

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
  /** Serialize a ForeachWriterPacket to bytes using Java serialization.
    *
    * Before serializing, runs the foreachWriter through `ClosureCleaner.clean(...,
    * cleanTransitively = true, ...)` to null out unused outer references. Additionally, the custom
    * `ObjectOutputStream` intercepts every `SerializedLambda` produced by Scala 3 lambda
    * `writeReplace()` and replaces it with a `LambdaSerializationProxy`. This mirrors the pattern
    * used in `UdfPacket.serialize()` and solves the Scala 3 → 2.13 lambda incompatibility.
    */
  def serialize(packet: ForeachWriterPacket): Array[Byte] =
    val cleanedWriter = ClosureCleaner
      .clean(packet.foreachWriter, cleanTransitively = true, mutable.Map.empty)
      .getOrElse(packet.foreachWriter)
    val cleanedPacket =
      if cleanedWriter eq packet.foreachWriter then packet
      else ForeachWriterPacket(cleanedWriter, packet.datasetEncoder)

    val bos = ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos):
      enableReplaceObject(true)
      override def replaceObject(obj: AnyRef): AnyRef = obj match
        case sl: SerializedLambda => LambdaSerializationProxy.fromSerializedLambda(sl)
        case other                => other
    try
      oos.writeObject(cleanedPacket)
      oos.flush()
      bos.toByteArray
    finally
      oos.close()
      bos.close()
