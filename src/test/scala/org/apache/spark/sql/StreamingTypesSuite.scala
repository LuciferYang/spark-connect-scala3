package org.apache.spark.sql

import org.apache.spark.sql.streaming.*
import org.apache.spark.sql.connect.common.ForeachWriterPacket
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Duration

class StreamingTypesSuite extends AnyFunSuite with Matchers:

  // ---------------------------------------------------------------------------
  // OutputMode
  // ---------------------------------------------------------------------------

  test("OutputMode.Append toString") {
    OutputMode.Append.toString shouldBe "Append"
  }

  test("OutputMode.Update toString") {
    OutputMode.Update.toString shouldBe "Update"
  }

  test("OutputMode.Complete toString") {
    OutputMode.Complete.toString shouldBe "Complete"
  }

  // ---------------------------------------------------------------------------
  // GroupStateTimeout
  // ---------------------------------------------------------------------------

  test("GroupStateTimeout.NoTimeout toString") {
    GroupStateTimeout.NoTimeout.toString shouldBe "NoTimeout"
  }

  test("GroupStateTimeout.ProcessingTimeTimeout toString") {
    GroupStateTimeout.ProcessingTimeTimeout.toString shouldBe "ProcessingTimeTimeout"
  }

  test("GroupStateTimeout.EventTimeTimeout toString") {
    GroupStateTimeout.EventTimeTimeout.toString shouldBe "EventTimeTimeout"
  }

  // ---------------------------------------------------------------------------
  // TimeMode
  // ---------------------------------------------------------------------------

  test("TimeMode.None toString") {
    TimeMode.None.toString shouldBe "None"
  }

  test("TimeMode.ProcessingTime toString") {
    TimeMode.ProcessingTime.toString shouldBe "ProcessingTime"
  }

  test("TimeMode.EventTime toString") {
    TimeMode.EventTime.toString shouldBe "EventTime"
  }

  // ---------------------------------------------------------------------------
  // TTLConfig
  // ---------------------------------------------------------------------------

  test("TTLConfig construction") {
    val config = TTLConfig(Duration.ofMinutes(5))
    config.ttlDuration shouldBe Duration.ofMinutes(5)
  }

  test("TTLConfig.NONE has zero duration") {
    TTLConfig.NONE.ttlDuration shouldBe Duration.ZERO
  }

  // ---------------------------------------------------------------------------
  // StatefulProcessor
  // ---------------------------------------------------------------------------

  test("StatefulProcessor is Serializable") {
    val processor = new StatefulProcessor[String, Int, String]:
      def handleInputRows(
          key: String,
          inputRows: Iterator[Int],
          timerValues: TimerValues
      ): Iterator[String] =
        inputRows.map(v => s"$key:$v")

    // Verify serialization roundtrip
    val bos = java.io.ByteArrayOutputStream()
    val oos = java.io.ObjectOutputStream(bos)
    oos.writeObject(processor)
    oos.close()
    bos.toByteArray.length should be > 0
  }

  test("EmptyInitialStateStruct is a case class") {
    val e = EmptyInitialStateStruct()
    e shouldBe EmptyInitialStateStruct()
  }

  // ---------------------------------------------------------------------------
  // ForeachWriterPacket
  // ---------------------------------------------------------------------------

  test("ForeachWriterPacket serialization produces non-empty bytes") {
    import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders
    val func: AnyRef = ((df: DataFrame, id: Long) => ()).asInstanceOf[AnyRef]
    val packet = ForeachWriterPacket(func, AgnosticEncoders.UnboundRowEncoder)
    val bytes = ForeachWriterPacket.serialize(packet)
    bytes.length should be > 0
  }

  test("ForeachWriterPacket has correct serialVersionUID") {
    // Verify the @SerialVersionUID matches the upstream value by checking the field
    val field = classOf[ForeachWriterPacket].getDeclaredField("serialVersionUID")
    field.setAccessible(true)
    field.getLong(null) shouldBe 3882541391565582579L
  }
