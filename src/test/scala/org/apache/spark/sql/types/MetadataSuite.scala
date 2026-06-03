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
      .putDoubleArray("scores", Array(1.5, 2.5))
      .putBooleanArray("flags", Array(true, false))
      .putStringArray("names", Array("a", "b"))
      .putMetadata("nested", nested)
      .putMetadataArray("nestedArray", Array(nested))
      .build()

    metadata.getLongArray("ids") shouldBe Array(1L, 2L)
    metadata.getDoubleArray("scores") shouldBe Array(1.5, 2.5)
    metadata.getBooleanArray("flags") shouldBe Array(true, false)
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

  test("Metadata fromJson parses supported arrays and nulls") {
    val parsed = Metadata.fromJson(
      """{"empty":[],"nullValue":null,"longs":[1,2],"doubles":[1.5,2.5],"bools":[true,false],"strings":["a","b"],"nestedArray":[{"x":1}]}"""
    )

    parsed.getLongArray("empty") shouldBe empty
    parsed.contains("nullValue") shouldBe true
    parsed.getLongArray("longs") shouldBe Array(1L, 2L)
    parsed.getDoubleArray("doubles") shouldBe Array(1.5, 2.5)
    parsed.getBooleanArray("bools") shouldBe Array(true, false)
    parsed.getStringArray("strings") shouldBe Array("a", "b")
    parsed.getMetadataArray("nestedArray").head.getLong("x") shouldBe 1L
  }

  test("Metadata rejects unsupported JSON arrays") {
    an[IllegalArgumentException] shouldBe thrownBy {
      Metadata.fromJson("""{"bad":[null]}""")
    }
  }

  test("Metadata supports key removal and empty singleton") {
    Metadata.empty.isEmpty shouldBe true

    val metadata = new MetadataBuilder()
      .putString("a", "x")
      .putString("b", "y")
      .build()

    metadata.withKeyRemoved("a").contains("a") shouldBe false
    metadata.withKeyRemoved("missing") shouldBe metadata
    metadata.withKeysRemoved(Seq.empty) shouldBe metadata
    metadata.withKeysRemoved(Seq("a", "b")).isEmpty shouldBe true
  }

  test("MetadataBuilder merges and removes entries") {
    val base = new MetadataBuilder()
      .putString("a", "x")
      .putString("b", "y")
      .build()
    val metadata = new MetadataBuilder()
      .withMetadata(base)
      .remove("b")
      .putLong("c", 3L)
      .build()

    metadata.getString("a") shouldBe "x"
    metadata.contains("b") shouldBe false
    metadata.getLong("c") shouldBe 3L
  }

  test("Metadata equality and hashCode handle arrays by content") {
    val left = new MetadataBuilder()
      .putLongArray("longs", Array(1L, 2L))
      .putDoubleArray("doubles", Array(1.0, 2.0))
      .putBooleanArray("bools", Array(true, false))
      .putStringArray("strings", Array("a", "b"))
      .putMetadataArray("nested", Array(new MetadataBuilder().putString("x", "y").build()))
      .build()
    val right = new MetadataBuilder()
      .putLongArray("longs", Array(1L, 2L))
      .putDoubleArray("doubles", Array(1.0, 2.0))
      .putBooleanArray("bools", Array(true, false))
      .putStringArray("strings", Array("a", "b"))
      .putMetadataArray("nested", Array(new MetadataBuilder().putString("x", "y").build()))
      .build()

    left shouldBe right
    left.hashCode() shouldBe right.hashCode()
    left.equals("not metadata") shouldBe false
    left.toString shouldBe left.json
  }

  test("StructField carries metadata with upstream-compatible default") {
    val metadata = new MetadataBuilder().putString("comment", "id column").build()
    val withMetadata = StructField("id", LongType, nullable = false, metadata)

    StructField("name", StringType).metadata shouldBe Metadata.empty
    withMetadata.metadata.getString("comment") shouldBe "id column"
  }
