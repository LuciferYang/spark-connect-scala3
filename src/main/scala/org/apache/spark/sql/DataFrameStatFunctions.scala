package org.apache.spark.sql

import org.apache.spark.connect.proto.*
import org.apache.spark.util.sketch.{BloomFilter, CountMinSketch}

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
    val row = result.collect().head
    // Server returns a single row with one column of type array<array<double>>
    row.get(0).asInstanceOf[Seq[Seq[Any]]].map(_.map {
      case n: Number => n.doubleValue()
      case other     => other.asInstanceOf[Double]
    }.toArray).toArray

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

  // -- Count-Min Sketch -------------------------------------------------------

  /** Builds a Count-min Sketch over a specified column. */
  def countMinSketch(colName: String, depth: Int, width: Int, seed: Int): CountMinSketch =
    countMinSketch(Column(colName), depth, width, seed)

  /** Builds a Count-min Sketch over a specified column. */
  def countMinSketch(colName: String, eps: Double, confidence: Double, seed: Int): CountMinSketch =
    countMinSketch(Column(colName), eps, confidence, seed)

  /** Builds a Count-min Sketch over a specified column. */
  def countMinSketch(col: Column, depth: Int, width: Int, seed: Int): CountMinSketch =
    val eps = 2.0 / width
    val confidence = 1 - 1 / Math.pow(2, depth)
    countMinSketch(col, eps, confidence, seed)

  /** Builds a Count-min Sketch over a specified column. */
  def countMinSketch(col: Column, eps: Double, confidence: Double, seed: Int): CountMinSketch =
    import functions.{count_min_sketch, lit}
    val cms = count_min_sketch(col, lit(eps), lit(confidence), lit(seed))
    val bytes = df.select(cms).head().get(0).asInstanceOf[Array[Byte]]
    CountMinSketch.readFrom(bytes)

  // -- Bloom Filter ------------------------------------------------------------

  /** Builds a Bloom filter over a specified column. */
  def bloomFilter(colName: String, expectedNumItems: Long, fpp: Double): BloomFilter =
    bloomFilter(Column(colName), expectedNumItems, fpp)

  /** Builds a Bloom filter over a specified column. */
  def bloomFilter(col: Column, expectedNumItems: Long, fpp: Double): BloomFilter =
    val numBits = BloomFilter.optimalNumOfBits(expectedNumItems, fpp)
    bloomFilter(col, expectedNumItems, numBits)

  /** Builds a Bloom filter over a specified column. */
  def bloomFilter(colName: String, expectedNumItems: Long, numBits: Long): BloomFilter =
    bloomFilter(Column(colName), expectedNumItems, numBits)

  /** Builds a Bloom filter over a specified column. */
  def bloomFilter(col: Column, expectedNumItems: Long, numBits: Long): BloomFilter =
    import functions.{bloom_filter_agg, lit}
    val bf = bloom_filter_agg(col, lit(expectedNumItems), lit(numBits))
    val bytes = df.select(bf).head().get(0).asInstanceOf[Array[Byte]]
    BloomFilter.readFrom(bytes)
