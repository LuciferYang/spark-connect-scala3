package org.apache.spark.sql

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.tags.IntegrationTest
import org.apache.spark.sql.types.*

/** Integration tests for Spatial types (Geometry/Geography). */
@IntegrationTest
class SpatialIntegrationSuite extends IntegrationTestBase:

  // A minimal WKB for POINT(1.0 2.0): 01 01000000 000000000000F03F 0000000000000040
  private val pointWkb: Array[Byte] = Array(
    0x01, 0x01, 0x00, 0x00, 0x00, // little-endian, type=Point
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f, // x = 1.0
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40 // y = 2.0
  ).map(_.toByte)

  test("st_geomfromwkb produces geometry column") {
    val df = spark.sql("SELECT 1 AS id").select(
      col("id"),
      st_geomfromwkb(lit(pointWkb)).as("geom")
    )
    val schema = df.schema
    assert(schema.fields.length == 2)
    // The geometry column should have GeometryType in the schema
    val geomField = schema.fields.find(_.name == "geom")
    assert(geomField.isDefined, "geom column not found in schema")
    assert(
      geomField.get.dataType.isInstanceOf[GeometryType],
      s"Expected GeometryType but got ${geomField.get.dataType}"
    )
  }

  test("st_geomfromwkb with srid") {
    val df = spark.sql("SELECT 1 AS id").select(
      st_geomfromwkb(lit(pointWkb), 4326).as("geom")
    )
    val rows = df.collect()
    assert(rows.length == 1)
    val geom = rows(0).getGeometry(0)
    assert(geom != null)
    assert(geom.getSrid == 4326)
    assert(geom.getBytes.nonEmpty)
  }

  test("st_geogfromwkb produces geography column") {
    val df = spark.sql("SELECT 1 AS id").select(
      col("id"),
      st_geogfromwkb(lit(pointWkb)).as("geog")
    )
    val schema = df.schema
    val geogField = schema.fields.find(_.name == "geog")
    assert(geogField.isDefined, "geog column not found in schema")
    assert(
      geogField.get.dataType.isInstanceOf[GeographyType],
      s"Expected GeographyType but got ${geogField.get.dataType}"
    )
  }

  test("getGeometry returns Geometry value from collect") {
    val df = spark.sql("SELECT 1 AS id").select(
      st_geomfromwkb(lit(pointWkb)).as("geom")
    )
    val row = df.collect()(0)
    val geom = row.getGeometry(0)
    assert(geom != null)
    assert(geom.getBytes.length > 0)
  }

  test("getGeography returns Geography value from collect") {
    val df = spark.sql("SELECT 1 AS id").select(
      st_geogfromwkb(lit(pointWkb)).as("geog")
    )
    val row = df.collect()(0)
    val geog = row.getGeography(0)
    assert(geog != null)
    assert(geog.getBytes.length > 0)
    assert(geog.getSrid == 4326) // default for Geography
  }

  test("Encoders.GEOMETRY schema has correct type") {
    val enc = Encoders.GEOMETRY
    assert(enc.schema.fields.head.dataType == GeometryType())
  }

  test("Encoders.GEOGRAPHY schema has correct type") {
    val enc = Encoders.GEOGRAPHY
    assert(enc.schema.fields.head.dataType == GeographyType())
  }
