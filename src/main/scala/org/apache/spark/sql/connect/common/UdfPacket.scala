package org.apache.spark.sql.connect.common

import java.io.{
  ByteArrayOutputStream,
  ObjectOutputStream,
  ObjectStreamException,
  Serializable
}
import java.lang.invoke.SerializedLambda

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
) extends Serializable:
  /** Replace with a proxy that avoids DefaultSerializationProxy/Seq type-check issues in Java 17+
    * when the server uses defaultReadObject.
    */
  @throws[ObjectStreamException]
  private def writeReplace(): AnyRef =
    UdfPacketSerializationProxy(
      function,
      inputEncoders.toArray[AnyRef],
      outputEncoder
    )

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
    * Additionally, the custom `ObjectOutputStream` intercepts every `SerializedLambda` produced by
    * Scala 3 lambda `writeReplace()` and replaces it with a `LambdaSerializationProxy`. This proxy
    * bypasses `\$deserializeLambda\$` entirely, reconstructing the lambda on the server via
    * `MethodHandle` invocation. This solves the core Scala 3 → 2.13 lambda incompatibility.
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
    // Custom OOS that replaces SerializedLambda with LambdaSerializationProxy.
    // This is called for every object in the graph, so nested lambdas (e.g., a lambda
    // captured by an adaptor) are also intercepted automatically.
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
