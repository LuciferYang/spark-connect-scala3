package org.apache.spark.sql

import org.apache.spark.connect.proto.*

/** Statistic functions for DataFrames. Access via `df.stat`.
  */
final class DataFrameStatFunctions private[sql] (df: DataFrame):

  /** Computes a pair-wise frequency table (contingency table). */
  def crosstab(col1: String, col2: String): DataFrame =
    df.withRelation(_.setCrosstab(
      StatCrosstab.newBuilder().setInput(df.relation).setCol1(col1).setCol2(col2).build()
    ))

  /** Computes the sample covariance between two columns. */
  def cov(col1: String, col2: String): Double =
    val result = df.withRelation(_.setCov(
      StatCov.newBuilder().setInput(df.relation).setCol1(col1).setCol2(col2).build()
    ))
    result.collect().head.getDouble(0)

  /** Computes Pearson correlation between two columns. */
  def corr(col1: String, col2: String): Double =
    corr(col1, col2, "pearson")

  /** Computes correlation between two columns using the given method. */
  def corr(col1: String, col2: String, method: String): Double =
    val result = df.withRelation(_.setCorr(
      StatCorr.newBuilder().setInput(df.relation)
        .setCol1(col1).setCol2(col2).setMethod(method).build()
    ))
    result.collect().head.getDouble(0)

  /** Computes approximate quantiles for numeric columns. */
  def approxQuantile(
      cols: Array[String],
      probabilities: Array[Double],
      relativeError: Double
  ): Array[Array[Double]] =
    val aqBuilder = StatApproxQuantile.newBuilder()
      .setInput(df.relation)
      .setRelativeError(relativeError)
    cols.foreach(aqBuilder.addCols)
    probabilities.foreach(aqBuilder.addProbabilities)
    val result = df.withRelation(_.setApproxQuantile(aqBuilder.build()))
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
    val fiBuilder = StatFreqItems.newBuilder().setInput(df.relation).setSupport(support)
    cols.foreach(fiBuilder.addCols)
    df.withRelation(_.setFreqItems(fiBuilder.build()))

  /** Finds frequent items for the given columns with default support of 1%. */
  def freqItems(cols: Seq[String]): DataFrame =
    val fiBuilder = StatFreqItems.newBuilder().setInput(df.relation)
    cols.foreach(fiBuilder.addCols)
    df.withRelation(_.setFreqItems(fiBuilder.build()))

  /** Returns a stratified sample without replacement. */
  def sampleBy[T](col: Column, fractions: Map[T, Double], seed: Long): DataFrame =
    val sbBuilder = StatSampleBy.newBuilder()
      .setInput(df.relation)
      .setCol(col.expr)
      .setSeed(seed)
    fractions.foreach { (stratum, fraction) =>
      val litExpr = Column.lit(stratum).expr
      if !litExpr.hasLiteral then
        throw IllegalArgumentException("sampleBy stratum must be a literal")
      sbBuilder.addFractions(
        StatSampleBy.Fraction.newBuilder()
          .setStratum(litExpr.getLiteral)
          .setFraction(fraction)
          .build()
      )
    }
    df.withRelation(_.setSampleBy(sbBuilder.build()))

  /** Returns a stratified sample without replacement using column name. */
  def sampleBy[T](col: String, fractions: Map[T, Double], seed: Long): DataFrame =
    sampleBy(Column(col), fractions, seed)
