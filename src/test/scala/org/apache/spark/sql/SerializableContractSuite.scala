package org.apache.spark.sql

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Pin the upstream contract that `Encoder[T]`, `Dataset[T]`, and `Row` extend
  * `java.io.Serializable`.
  *
  * The static marker is what matters: it lets users embed `Encoder[BUF]` as a field inside
  * `Aggregator` subclasses (which extend `Serializable` upstream) without tripping JVM
  * serializability checks at the field declaration level.
  *
  * Notes on round-trip behavior:
  *   - `Encoder` instances rely on a `writeReplace` proxy chain whose `readResolve` resolves
  *     server-side (Scala 2.13) `AgnosticEncoders$Foo$` classes via `Class.forName`. On the
  *     client we therefore only assert that `writeObject` succeeds; full round-trip is exercised
  *     against the Spark Connect server in integration tests.
  *   - `Row` carries primitive/String values plus an optional `StructType` schema, all
  *     `Serializable`, so a complete client-side round-trip is asserted.
  *   - `Dataset[T]` references a `SparkConnectClient` (gRPC, non-`Serializable`) via its session,
  *     so we only assert the type marker, not a round-trip.
  */
class SerializableContractSuite extends AnyFunSuite with Matchers:

  private def writeBytes(value: AnyRef): Array[Byte] =
    val baos = ByteArrayOutputStream()
    val oos = ObjectOutputStream(baos)
    try oos.writeObject(value)
    finally oos.close()
    baos.toByteArray

  private def roundTrip[A <: AnyRef](value: A): A =
    val ois = ObjectInputStream(ByteArrayInputStream(writeBytes(value)))
    try ois.readObject().asInstanceOf[A]
    finally ois.close()

  // ---------------------------------------------------------------------------
  // Encoder
  // ---------------------------------------------------------------------------

  test("Encoder trait extends Serializable") {
    classOf[java.io.Serializable].isAssignableFrom(classOf[Encoder[?]]) shouldBe true
  }

  test("Encoders.scalaInt is writable through Java serialization") {
    val bytes = writeBytes(Encoders.scalaInt)
    bytes should not be empty
  }

  test("Encoders.STRING is writable through Java serialization") {
    val bytes = writeBytes(Encoders.STRING)
    bytes should not be empty
  }

  // ---------------------------------------------------------------------------
  // Row
  // ---------------------------------------------------------------------------

  test("Row class extends Serializable") {
    classOf[java.io.Serializable].isAssignableFrom(classOf[Row]) shouldBe true
  }

  test("Row(values*) round-trips through Java serialization") {
    val row = Row(1, "hello", 3.14)
    val recovered = roundTrip(row)
    recovered.size shouldBe 3
    recovered.get(0) shouldBe 1
    recovered.get(1) shouldBe "hello"
    recovered.get(2) shouldBe 3.14
  }

  test("Row.fromSeq round-trips through Java serialization") {
    val row = Row.fromSeq(Seq("a", 42L, true))
    val recovered = roundTrip(row)
    recovered.size shouldBe 3
    recovered.get(0) shouldBe "a"
    recovered.get(1) shouldBe 42L
    recovered.get(2) shouldBe true
  }

  // ---------------------------------------------------------------------------
  // Dataset (type-marker only — gRPC client field is non-Serializable)
  // ---------------------------------------------------------------------------

  test("Dataset class extends Serializable") {
    classOf[java.io.Serializable].isAssignableFrom(classOf[Dataset[?]]) shouldBe true
  }
