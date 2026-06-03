package org.apache.spark.sql.types

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MetadataSuite extends AnyFunSuite with Matchers:

  test("MetadataBuilder stores scalar values") {
    val metadata = new MetadataBuilder()
      .putString("name", "alice")
      .putLong("age", 42L)
      .putDouble("score", 3.5)
      .putBoolean("active", true)
      .putNull("missing")
      .build()

    metadata.getString("name") shouldBe "alice"
    metadata.getLong("age") shouldBe 42L
    metadata.getDouble("score") shouldBe 3.5
    metadata.getBoolean("active") shouldBe true
    metadata.contains("missing") shouldBe true
  }

  test("MetadataBuilder stores arrays and nested metadata") {
    val nested = new MetadataBuilder().putString("inner", "value").build()
    val metadata = new MetadataBuilder()
      .putLongArray("ids", Array(1L, 2L))
      .putStringArray("names", Array("a", "b"))
      .putMetadata("nested", nested)
      .putMetadataArray("nestedArray", Array(nested))
      .build()

    metadata.getLongArray("ids") shouldBe Array(1L, 2L)
    metadata.getStringArray("names") shouldBe Array("a", "b")
    metadata.getMetadata("nested").getString("inner") shouldBe "value"
    metadata.getMetadataArray("nestedArray").head shouldBe nested
  }

  test("Metadata json and fromJson round-trip") {
    val metadata = new MetadataBuilder()
      .putString("description", "field")
      .putLongArray("ids", Array(1L, 2L))
      .putMetadata("nested", new MetadataBuilder().putBoolean("ok", true).build())
      .build()

    val parsed = Metadata.fromJson(metadata.json)
    parsed shouldBe metadata
    parsed.getMetadata("nested").getBoolean("ok") shouldBe true
  }

  test("Metadata supports key removal and empty singleton") {
    Metadata.empty.isEmpty shouldBe true

    val metadata = new MetadataBuilder()
      .putString("a", "x")
      .putString("b", "y")
      .build()

    metadata.withKeyRemoved("a").contains("a") shouldBe false
    metadata.withKeysRemoved(Seq("a", "b")).isEmpty shouldBe true
  }

  test("StructField carries metadata with upstream-compatible default") {
    val metadata = new MetadataBuilder().putString("comment", "id column").build()
    val withMetadata = StructField("id", LongType, nullable = false, metadata)

    StructField("name", StringType).metadata shouldBe Metadata.empty
    withMetadata.metadata.getString("comment") shouldBe "id column"
  }
