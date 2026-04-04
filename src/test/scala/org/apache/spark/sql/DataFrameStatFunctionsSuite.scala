package org.apache.spark.sql

import org.apache.spark.connect.proto.expressions.Expression
import org.apache.spark.connect.proto.expressions.Expression.ExprType
import org.apache.spark.connect.proto.relations.Relation
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataFrameStatFunctionsSuite extends AnyFunSuite with Matchers:

  /** Helper: create a minimal DataFrame stub for testing relation building. */
  private def stubDf: DataFrame =
    // We can't create a real SparkSession in unit tests, so we test
    // the proto construction indirectly via DataFrameStatFunctions.
    // For now, we just test that the class compiles and the API surface is correct.
    // Integration tests will verify actual execution.
    null.asInstanceOf[DataFrame]

  test("DataFrameStatFunctions class has expected methods") {
    // Verify the API surface exists by checking method signatures compile
    val clazz = classOf[DataFrameStatFunctions]
    clazz.getMethod("crosstab", classOf[String], classOf[String]) should not be null
    clazz.getMethod("cov", classOf[String], classOf[String]) should not be null
    clazz.getMethod("corr", classOf[String], classOf[String]) should not be null
    clazz.getMethod("corr", classOf[String], classOf[String], classOf[String]) should not be null
    clazz.getMethod("freqItems", classOf[Seq[String]], classOf[Double]) should not be null
    clazz.getMethod("freqItems", classOf[Seq[String]]) should not be null
  }

  test("StatCrosstab proto construction") {
    import org.apache.spark.connect.proto.relations.StatCrosstab
    val proto = StatCrosstab(input = None, col1 = "a", col2 = "b")
    proto.col1 shouldBe "a"
    proto.col2 shouldBe "b"
  }

  test("StatCorr proto construction") {
    import org.apache.spark.connect.proto.relations.StatCorr
    val proto = StatCorr(input = None, col1 = "x", col2 = "y", method = Some("pearson"))
    proto.col1 shouldBe "x"
    proto.col2 shouldBe "y"
    proto.method shouldBe Some("pearson")
  }

  test("StatApproxQuantile proto construction") {
    import org.apache.spark.connect.proto.relations.StatApproxQuantile
    val proto = StatApproxQuantile(
      input = None,
      cols = Seq("a", "b"),
      probabilities = Seq(0.25, 0.5, 0.75),
      relativeError = 0.01
    )
    proto.cols shouldBe Seq("a", "b")
    proto.probabilities shouldBe Seq(0.25, 0.5, 0.75)
    proto.relativeError shouldBe 0.01
  }

  test("StatFreqItems proto construction") {
    import org.apache.spark.connect.proto.relations.StatFreqItems
    val proto = StatFreqItems(input = None, cols = Seq("col1", "col2"), support = Some(0.05))
    proto.cols shouldBe Seq("col1", "col2")
    proto.support shouldBe Some(0.05)
  }

  test("StatSampleBy.Fraction proto construction") {
    import org.apache.spark.connect.proto.relations.StatSampleBy
    val lit = Column.lit("stratum_a").expr.exprType match {
      case ExprType.Literal(l) => l
      case _ => fail("Expected Literal")
    }
    val fraction = StatSampleBy.Fraction(stratum = Some(lit), fraction = 0.5)
    fraction.fraction shouldBe 0.5
    fraction.stratum shouldBe defined
  }
