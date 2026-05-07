package org.apache.spark.sql

import org.apache.spark.sql.types.*

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

/** Generates Arrow IPC byte arrays for JMH benchmarks.
  *
  * Lives in `org.apache.spark.sql` to access `private[sql] ArrowSerializer`.
  */
object BenchmarkDataGenerator:

  def generateIntData(numRows: Int): Array[Byte] =
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("value", LongType, nullable = false)
    ))
    val rows = (0 until numRows).map(i => Row(i, i.toLong * 42))
    ArrowSerializer.encodeRows(rows, schema)

  def generateStringData(numRows: Int): Array[Byte] =
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = false),
      StructField("city", StringType, nullable = true)
    ))
    val cities = Array("Beijing", "Shanghai", "Shenzhen", "Hangzhou", "Chengdu")
    val rows = (0 until numRows).map(i =>
      Row(s"user_$i", cities(i % cities.length))
    )
    ArrowSerializer.encodeRows(rows, schema)

  def generateDecimalData(numRows: Int): Array[Byte] =
    val schema = StructType(Seq(
      StructField("amount", DecimalType(18, 2), nullable = false),
      StructField("rate", DecimalType(10, 6), nullable = false)
    ))
    val rows = (0 until numRows).map(i =>
      Row(
        java.math.BigDecimal.valueOf(i * 100L + 99, 2),
        java.math.BigDecimal.valueOf(i * 1000L + 123456, 6)
      )
    )
    ArrowSerializer.encodeRows(rows, schema)

  def generateTimestampData(numRows: Int): Array[Byte] =
    val schema = StructType(Seq(
      StructField("created_at", TimestampType, nullable = false),
      StructField("updated_at", TimestampType, nullable = true)
    ))
    val baseEpoch = 1700000000000L // ~2023-11-14
    val rows = (0 until numRows).map(i =>
      Row(
        Timestamp.from(Instant.ofEpochMilli(baseEpoch + i * 1000L)),
        if i % 5 == 0 then null
        else Timestamp.from(Instant.ofEpochMilli(baseEpoch + i * 2000L))
      )
    )
    ArrowSerializer.encodeRows(rows, schema)

  def generateNestedStructData(numRows: Int): Array[Byte] =
    val addressType = StructType(Seq(
      StructField("street", StringType, nullable = false),
      StructField("zip", IntegerType, nullable = false)
    ))
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("address", addressType, nullable = false)
    ))
    val rows = (0 until numRows).map(i =>
      Row(i, Row(s"Street $i", 100000 + i))
    )
    ArrowSerializer.encodeRows(rows, schema)

  def generateArrayData(numRows: Int): Array[Byte] =
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("tags", ArrayType(StringType, containsNull = false), nullable = false)
    ))
    val rows = (0 until numRows).map(i =>
      Row(i, Seq(s"tag_${i % 10}", s"tag_${(i + 1) % 10}", s"tag_${(i + 2) % 10}"))
    )
    ArrowSerializer.encodeRows(rows, schema)

  def generateMapData(numRows: Int): Array[Byte] =
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("props", MapType(StringType, IntegerType, valueContainsNull = false), nullable = false)
    ))
    val rows = (0 until numRows).map(i =>
      Row(i, Map("a" -> i, "b" -> i * 2, "c" -> i * 3))
    )
    ArrowSerializer.encodeRows(rows, schema)

  def generateMixedData(numRows: Int): Array[Byte] =
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("score", DoubleType, nullable = false),
      StructField("active", BooleanType, nullable = false),
      StructField("created", TimestampType, nullable = false),
      StructField("amount", DecimalType(18, 2), nullable = true)
    ))
    val baseEpoch = 1700000000000L
    val rows = (0 until numRows).map(i =>
      Row(
        i,
        s"item_$i",
        i * 1.5,
        i % 2 == 0,
        Timestamp.from(Instant.ofEpochMilli(baseEpoch + i * 1000L)),
        if i % 7 == 0 then null else java.math.BigDecimal.valueOf(i * 100L + 50, 2)
      )
    )
    ArrowSerializer.encodeRows(rows, schema)
