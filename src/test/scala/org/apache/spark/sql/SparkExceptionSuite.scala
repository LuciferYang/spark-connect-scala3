package org.apache.spark.sql

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkExceptionSuite extends AnyFunSuite with Matchers:

  // ---------------------------------------------------------------------------
  // Inheritance chain
  // ---------------------------------------------------------------------------

  test("AnalysisException is a SparkException") {
    val ex = AnalysisException("test")
    ex shouldBe a[SparkException]
  }

  test("ParseException is an AnalysisException and SparkException") {
    val ex = ParseException("test")
    ex shouldBe a[AnalysisException]
    ex shouldBe a[SparkException]
  }

  test("StreamingQueryException is a SparkException") {
    val ex = StreamingQueryException("test")
    ex shouldBe a[SparkException]
  }

  test("SparkRuntimeException is a SparkException") {
    val ex = SparkRuntimeException("test")
    ex shouldBe a[SparkException]
  }

  test("SparkUpgradeException is a SparkException") {
    val ex = SparkUpgradeException("test")
    ex shouldBe a[SparkException]
  }

  test("SparkArithmeticException is a SparkException") {
    val ex = SparkArithmeticException("test")
    ex shouldBe a[SparkException]
  }

  test("SparkNumberFormatException is a SparkException") {
    val ex = SparkNumberFormatException("test")
    ex shouldBe a[SparkException]
  }

  test("SparkIllegalArgumentException is a SparkException") {
    val ex = SparkIllegalArgumentException("test")
    ex shouldBe a[SparkException]
  }

  test("SparkUnsupportedOperationException is a SparkException") {
    val ex = SparkUnsupportedOperationException("test")
    ex shouldBe a[SparkException]
  }

  test("SparkArrayIndexOutOfBoundsException is a SparkException") {
    val ex = SparkArrayIndexOutOfBoundsException("test")
    ex shouldBe a[SparkException]
  }

  test("SparkDateTimeException is a SparkException") {
    val ex = SparkDateTimeException("test")
    ex shouldBe a[SparkException]
  }

  test("SparkNoSuchElementException is a SparkException") {
    val ex = SparkNoSuchElementException("test")
    ex shouldBe a[SparkException]
  }

  // Catalog exceptions extend AnalysisException

  test("NamespaceAlreadyExistsException is an AnalysisException") {
    val ex = NamespaceAlreadyExistsException("test")
    ex shouldBe a[AnalysisException]
    ex shouldBe a[SparkException]
  }

  test("TableAlreadyExistsException is an AnalysisException") {
    val ex = TableAlreadyExistsException("test")
    ex shouldBe a[AnalysisException]
  }

  test("TempTableAlreadyExistsException is an AnalysisException") {
    val ex = TempTableAlreadyExistsException("test")
    ex shouldBe a[AnalysisException]
  }

  test("NoSuchDatabaseException is an AnalysisException") {
    val ex = NoSuchDatabaseException("test")
    ex shouldBe a[AnalysisException]
  }

  test("NoSuchNamespaceException is an AnalysisException") {
    val ex = NoSuchNamespaceException("test")
    ex shouldBe a[AnalysisException]
  }

  test("NoSuchTableException is an AnalysisException") {
    val ex = NoSuchTableException("test")
    ex shouldBe a[AnalysisException]
  }

  // ---------------------------------------------------------------------------
  // errorClass / sqlState carried through
  // ---------------------------------------------------------------------------

  test("AnalysisException carries errorClass and sqlState") {
    val ex = AnalysisException(
      "bad query",
      errorClass = Some("TABLE_NOT_FOUND"),
      sqlState = Some("42P01"),
      messageParameters = Map("table" -> "foo")
    )
    ex.errorClass shouldBe Some("TABLE_NOT_FOUND")
    ex.sqlState shouldBe Some("42P01")
    ex.messageParameters shouldBe Map("table" -> "foo")
    ex.getMessage shouldBe "bad query"
  }

  test("ParseException carries errorClass and sqlState") {
    val ex = ParseException(
      "syntax error",
      errorClass = Some("PARSE_SYNTAX_ERROR"),
      sqlState = Some("42601")
    )
    ex.errorClass shouldBe Some("PARSE_SYNTAX_ERROR")
    ex.sqlState shouldBe Some("42601")
  }

  test("SparkArithmeticException carries errorClass") {
    val ex = SparkArithmeticException(
      "divide by zero",
      errorClass = Some("DIVIDE_BY_ZERO")
    )
    ex.errorClass shouldBe Some("DIVIDE_BY_ZERO")
  }

  // ---------------------------------------------------------------------------
  // Cause chaining
  // ---------------------------------------------------------------------------

  test("exception subclass preserves cause chain") {
    val root = RuntimeException("root cause")
    val inner = AnalysisException("inner", cause = root)
    val outer = SparkException("outer", cause = inner)
    outer.getCause shouldBe inner
    inner.getCause shouldBe root
  }
