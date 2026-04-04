package org.apache.spark.sql

import org.apache.spark.connect.proto.expressions.Expression
import org.apache.spark.connect.proto.relations.*

/**
 * Statistic functions for DataFrames.
 * Access via `df.stat`.
 */
final class DataFrameStatFunctions private[sql] (df: DataFrame):

  /** Computes a pair-wise frequency table (contingency table). */
  def crosstab(col1: String, col2: String): DataFrame =
    df.withRelation(Relation.RelType.Crosstab(
      StatCrosstab(input = Some(df.relation), col1 = col1, col2 = col2)
    ))

  /** Computes the sample covariance between two columns. */
  def cov(col1: String, col2: String): Double =
    val result = df.withRelation(Relation.RelType.Cov(
      StatCov(input = Some(df.relation), col1 = col1, col2 = col2)
    ))
    result.collect().head.getDouble(0)

  /** Computes Pearson correlation between two columns. */
  def corr(col1: String, col2: String): Double =
    corr(col1, col2, "pearson")

  /** Computes correlation between two columns using the given method. */
  def corr(col1: String, col2: String, method: String): Double =
    val result = df.withRelation(Relation.RelType.Corr(
      StatCorr(input = Some(df.relation), col1 = col1, col2 = col2, method = Some(method))
    ))
    result.collect().head.getDouble(0)

  /** Computes approximate quantiles for numeric columns. */
  def approxQuantile(
      cols: Array[String],
      probabilities: Array[Double],
      relativeError: Double
  ): Array[Array[Double]] =
    val result = df.withRelation(Relation.RelType.ApproxQuantile(
      StatApproxQuantile(
        input = Some(df.relation),
        cols = cols.toSeq,
        probabilities = probabilities.toSeq,
        relativeError = relativeError
      )
    ))
    val rows = result.collect()
    rows.map { row =>
      (0 until row.size).map(row.getDouble).toArray
    }

  /** Computes approximate quantiles for a single numeric column. */
  def approxQuantile(
      col: String,
      probabilities: Array[Double],
      relativeError: Double
  ): Array[Double] =
    approxQuantile(Array(col), probabilities, relativeError).head

  /** Finds frequent items for the given columns using the given support threshold. */
  def freqItems(cols: Seq[String], support: Double): DataFrame =
    df.withRelation(Relation.RelType.FreqItems(
      StatFreqItems(input = Some(df.relation), cols = cols, support = Some(support))
    ))

  /** Finds frequent items for the given columns with default support of 1%. */
  def freqItems(cols: Seq[String]): DataFrame =
    df.withRelation(Relation.RelType.FreqItems(
      StatFreqItems(input = Some(df.relation), cols = cols)
    ))

  /** Returns a stratified sample without replacement. */
  def sampleBy[T](col: Column, fractions: Map[T, Double], seed: Long): DataFrame =
    val protoFractions = fractions.map { (stratum, fraction) =>
      StatSampleBy.Fraction(
        stratum = Some(Column.lit(stratum).expr.exprType match {
          case Expression.ExprType.Literal(lit) => lit
          case _ => throw IllegalArgumentException("sampleBy stratum must be a literal")
        }),
        fraction = fraction
      )
    }.toSeq
    df.withRelation(Relation.RelType.SampleBy(
      StatSampleBy(
        input = Some(df.relation),
        col = Some(col.expr),
        fractions = protoFractions,
        seed = Some(seed)
      )
    ))

  /** Returns a stratified sample without replacement using column name. */
  def sampleBy[T](col: String, fractions: Map[T, Double], seed: Long): DataFrame =
    sampleBy(Column(col), fractions, seed)
