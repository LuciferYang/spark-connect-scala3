package org.apache.spark.sql

import scala.language.implicitConversions
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

  type ColumnName = _root_.org.apache.spark.sql.ColumnName

  /** Implicit conversion from ColumnName to Column. */
  given Conversion[ColumnName, Column] with
    def apply(cn: ColumnName): Column = cn

  /** String interpolator for column references: `$"colName"`.
    *
    * Returns a ColumnName which provides StructField helpers and implicitly converts to Column.
    */
  extension (sc: StringContext)
    def $(args: Any*): ColumnName =
      val name = sc.s(args*)
      new ColumnName(name)

  /** Implicit conversion from Symbol to Column (Scala 2 compat style). */
  given Conversion[Symbol, Column] with
    def apply(s: Symbol): Column = Column(s.name)

  /** Implicit conversion from String to Column for select("colName") etc. */
  extension (colName: String) def col: Column = Column(colName)

  // ---------------------------------------------------------------------------
  // Seq[T].toDS / Seq[T].toDF convenience extensions
  // ---------------------------------------------------------------------------

  private class LocalSeqDatasetHolder[T: Encoder: ClassTag](
      seq: Seq[T]
  )(using spark: SparkSession)
      extends DatasetHolder[T]:
    def toDS(): Dataset[T] = spark.createDataset(seq)

    def toDF(): DataFrame = toDS().toDF()

    def toDF(colNames: String*): DataFrame = toDS().toDF(colNames*)

  implicit def localSeqToDatasetHolder[T: Encoder: ClassTag](seq: Seq[T])(using
      SparkSession
  ): DatasetHolder[T] =
    LocalSeqDatasetHolder(seq)

  extension [T: Encoder: ClassTag](seq: Seq[T])
    /** Convert a Seq to a Dataset. Requires an implicit SparkSession. */
    def toDS(using spark: SparkSession): Dataset[T] =
      LocalSeqDatasetHolder(seq).toDS()

    /** Convert a Seq to a DataFrame. Requires an implicit SparkSession. */
    def toDF(using spark: SparkSession): DataFrame =
      LocalSeqDatasetHolder(seq).toDF()

    /** Convert a Seq to a DataFrame with specified column names. */
    def toDF(colNames: String*)(using spark: SparkSession): DataFrame =
      LocalSeqDatasetHolder(seq).toDF(colNames*)
