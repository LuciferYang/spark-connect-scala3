package org.apache.spark.sql

import org.apache.spark.sql.types.*

import scala.reflect.ClassTag

/** Scala 3 implicits for idiomatic Spark column references and dataset conversions.
  *
  * Usage:
  * {{{
  *   import org.apache.spark.sql.implicits.*
  *
  *   val df = spark.range(10)
  *   df.select($"id", $"id" + 1)
  *   df.filter($"id" > 5)
  *
  *   // ColumnName for StructField helpers
  *   val field: StructField = $"name".string
  *
  *   // Seq.toDS / Seq.toDF
  *   given spark: SparkSession = ...
  *   Seq(1, 2, 3).toDS
  *   Seq(1, 2, 3).toDF
  * }}}
  */
object implicits:

  /** A Column wrapper that provides StructField helper methods.
    *
    * Unlike Spark 2/3 which uses subclassing (`class ColumnName extends Column`), this uses a
    * wrapper class because `Column` is `final`. The wrapper delegates to the underlying Column for
    * all Column operations via implicit conversion.
    */
  class ColumnName(val name: String):
    /** The underlying Column for this name. */
    def column: Column = Column(name)

    // StructField convenience methods
    def boolean: StructField = StructField(name, BooleanType)
    def byte: StructField = StructField(name, ByteType)
    def short: StructField = StructField(name, ShortType)
    def int: StructField = StructField(name, IntegerType)
    def long: StructField = StructField(name, LongType)
    def float: StructField = StructField(name, FloatType)
    def double: StructField = StructField(name, DoubleType)
    def string: StructField = StructField(name, StringType)
    def date: StructField = StructField(name, DateType)
    def timestamp: StructField = StructField(name, TimestampType)
    def binary: StructField = StructField(name, BinaryType)
    def decimal: StructField = StructField(name, DecimalType.DEFAULT)
    def decimal(precision: Int, scale: Int): StructField =
      StructField(name, DecimalType(precision, scale))

  /** Implicit conversion from ColumnName to Column. */
  given Conversion[ColumnName, Column] with
    def apply(cn: ColumnName): Column = cn.column

  /** String interpolator for column references: `$"colName"`.
    *
    * Returns a ColumnName which provides StructField helpers and implicitly converts to Column.
    */
  extension (sc: StringContext)
    def $(args: Any*): ColumnName =
      val name = sc.s(args*)
      ColumnName(name)

  /** Implicit conversion from Symbol to Column (Scala 2 compat style). */
  given Conversion[Symbol, Column] with
    def apply(s: Symbol): Column = Column(s.name)

  /** Implicit conversion from String to Column for select("colName") etc. */
  extension (colName: String) def col: Column = Column(colName)

  // ---------------------------------------------------------------------------
  // Seq[T].toDS / Seq[T].toDF convenience extensions
  // ---------------------------------------------------------------------------

  extension [T: Encoder: ClassTag](seq: Seq[T])
    /** Convert a Seq to a Dataset. Requires an implicit SparkSession. */
    def toDS(using spark: SparkSession): Dataset[T] =
      spark.createDataset(seq)

    /** Convert a Seq to a DataFrame. Requires an implicit SparkSession. */
    def toDF(using spark: SparkSession): DataFrame =
      spark.createDataset(seq).toDF()

    /** Convert a Seq to a DataFrame with specified column names. */
    def toDF(colNames: String*)(using spark: SparkSession): DataFrame =
      spark.createDataset(seq).toDF(colNames*)
