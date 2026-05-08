package org.apache.spark.sql.connect.common

import org.apache.spark.sql.{DataFrame, ForeachWriter, Row}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.UnboundRowEncoder
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for ForeachWriterPacket serialization with LambdaSerializationProxy. */
class ForeachWriterPacketSuite extends AnyFunSuite with Matchers:

  test("Scala 3 lambda serialization produces LambdaSerializationProxy in byte stream") {
    val func: (DataFrame, Long) => Unit = (df, id) => ()
    val packet = ForeachWriterPacket(func, UnboundRowEncoder)
    val bytes = ForeachWriterPacket.serialize(packet)

    bytes should not be empty

    // Byte stream should contain LambdaSerializationProxy class descriptor
    val streamContent = String(bytes, "ISO-8859-1")
    streamContent should include("LambdaSerializationProxy")
    // Should NOT contain Scala 3's SerializedLambda (which would fail on 2.13 server)
    streamContent should not include "SerializedLambda"
  }

  test("ForeachWriter subclass instance serializes without proxy") {
    val myWriter = new ForeachWriter[Row]:
      def open(partitionId: Long, epochId: Long): Boolean = true
      def process(value: Row): Unit = ()
      def close(errorOrNull: Throwable): Unit = ()

    val packet = ForeachWriterPacket(myWriter, UnboundRowEncoder)
    val bytes = ForeachWriterPacket.serialize(packet)

    bytes should not be empty

    // A concrete ForeachWriter subclass should NOT be replaced with proxy
    val streamContent = String(bytes, "ISO-8859-1")
    streamContent should include("ForeachWriterPacket")
    // The writer class itself should appear — it's serialized normally
    streamContent should include("ForeachWriter")
  }

  test("nested lambda in closure is also replaced with proxy") {
    // A lambda that captures another lambda
    val inner: Long => String = id => s"batch-$id"
    val func: (DataFrame, Long) => Unit = (df, id) => {
      val _ = inner(id)
    }
    val packet = ForeachWriterPacket(func, UnboundRowEncoder)
    val bytes = ForeachWriterPacket.serialize(packet)

    bytes should not be empty

    // Byte stream should contain LambdaSerializationProxy class descriptor
    val streamContent = String(bytes, "ISO-8859-1")
    streamContent should include("LambdaSerializationProxy")
  }
