package org.apache.spark.sql

import scala.jdk.CollectionConverters.*

import org.apache.spark.connect.proto.*
import org.apache.spark.connect.proto.DataType as ProtoDataType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataFrameNaFunctionsSuite extends AnyFunSuite with Matchers:

  /** Stub SparkSession using null client — proto-only methods never touch gRPC. */
  private def stubSession: SparkSession = SparkSession(null)

  /** Stub DataFrame backed by an empty LocalRelation. */
  private def stubDf: DataFrame =
    val session = stubSession
    val rel = Relation
      .newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(session.nextPlanId()).build())
      .setLocalRelation(LocalRelation.getDefaultInstance)
      .build()
    DataFrame(session, rel)

  private def naFunctions: DataFrameNaFunctions = stubDf.na

  // ---------------------------------------------------------------------------
  // drop(how, cols)
  // ---------------------------------------------------------------------------

  test("drop(how='any', cols) builds NADrop without minNonNulls") {
    val result = naFunctions.drop("any", Seq("a", "b"))
    val rel = result.relation
    assert(rel.hasDropNa)
    val naDrop = rel.getDropNa
    naDrop.getColsList.asScala.toSeq shouldBe Seq("a", "b")
    naDrop.hasMinNonNulls shouldBe false
  }

  test("drop(how='all', cols) builds NADrop with minNonNulls=1") {
    val result = naFunctions.drop("all", Seq("x"))
    val naDrop = result.relation.getDropNa
    naDrop.getColsList.asScala.toSeq shouldBe Seq("x")
    naDrop.getMinNonNulls shouldBe 1
  }

  test("drop(how) is case-insensitive for 'ALL'") {
    val result = naFunctions.drop("ALL", Seq("c"))
    result.relation.getDropNa.getMinNonNulls shouldBe 1
  }

  // ---------------------------------------------------------------------------
  // drop(minNonNulls)
  // ---------------------------------------------------------------------------

  test("drop(minNonNulls) builds NADrop with correct threshold") {
    val result = naFunctions.drop(3)
    val naDrop = result.relation.getDropNa
    naDrop.getMinNonNulls shouldBe 3
  }

  // ---------------------------------------------------------------------------
  // fill(value, cols)
  // ---------------------------------------------------------------------------

  test("fill(Double, cols) builds NAFill with double literal") {
    val result = naFunctions.fill(42.0, Seq("a", "b"))
    val rel = result.relation
    assert(rel.hasFillNa)
    val naFill = rel.getFillNa
    naFill.getColsList.asScala.toSeq shouldBe Seq("a", "b")
    naFill.getValuesCount shouldBe 1
    naFill.getValues(0).getDouble shouldBe 42.0
  }

  test("fill(String, cols) builds NAFill with string literal") {
    val result = naFunctions.fill("N/A", Seq("name"))
    val naFill = result.relation.getFillNa
    naFill.getColsList.asScala.toSeq shouldBe Seq("name")
    naFill.getValuesCount shouldBe 1
    naFill.getValues(0).getString shouldBe "N/A"
  }

  // ---------------------------------------------------------------------------
  // fill(valueMap)
  // ---------------------------------------------------------------------------

  test("fill(Map) builds NAFill with per-column values") {
    val result = naFunctions.fill(Map("a" -> 1, "b" -> "hello"))
    val naFill = result.relation.getFillNa
    naFill.getColsCount shouldBe 2
    naFill.getValuesCount shouldBe 2
  }

  test("fill(Map) handles various literal types") {
    val result = naFunctions.fill(Map(
      "bool" -> true,
      "int" -> 42,
      "long" -> 100L,
      "float" -> 1.5f,
      "double" -> 2.5,
      "str" -> "abc"
    ))
    val naFill = result.relation.getFillNa
    naFill.getColsCount shouldBe 6
    naFill.getValuesCount shouldBe 6
    val values = naFill.getValuesList.asScala.toSeq
    assert(values.exists(_.hasBoolean))
    assert(values.exists(_.hasInteger))
    assert(values.exists(_.hasLong))
    assert(values.exists(_.hasFloat))
    assert(values.exists(_.hasDouble))
    assert(values.exists(_.hasString))
  }

  test("fill(Map) converts null to null literal") {
    val result = naFunctions.fill(Map("x" -> null))
    val naFill = result.relation.getFillNa
    naFill.getValuesCount shouldBe 1
    naFill.getValues(0).hasNull shouldBe true
  }

  // ---------------------------------------------------------------------------
  // replace
  // ---------------------------------------------------------------------------

  test("replace builds NAReplace with correct replacements") {
    val result = naFunctions.replace("col1", Map(1 -> 10, 2 -> 20))
    val rel = result.relation
    assert(rel.hasReplace)
    val naReplace = rel.getReplace
    naReplace.getColsList.asScala.toSeq shouldBe Seq("col1")
    naReplace.getReplacementsCount shouldBe 2
    val r0 = naReplace.getReplacements(0)
    r0.getOldValue.getInteger shouldBe 1
    r0.getNewValue.getInteger shouldBe 10
  }

  test("replace with String values") {
    val result = naFunctions.replace("name", Map("old" -> "new"))
    val naReplace = result.relation.getReplace
    naReplace.getReplacementsCount shouldBe 1
    naReplace.getReplacements(0).getOldValue.getString shouldBe "old"
    naReplace.getReplacements(0).getNewValue.getString shouldBe "new"
  }

  test("replace with Double values") {
    val result = naFunctions.replace("price", Map(0.0 -> 99.9))
    val naReplace = result.relation.getReplace
    naReplace.getReplacementsCount shouldBe 1
    naReplace.getReplacements(0).getOldValue.getDouble shouldBe 0.0
    naReplace.getReplacements(0).getNewValue.getDouble shouldBe 99.9
  }

  // ---------------------------------------------------------------------------
  // Relation wiring
  // ---------------------------------------------------------------------------

  test("result relation's input is the original DataFrame relation") {
    val df = stubDf
    val result = df.na.drop("any", Seq("a"))
    result.relation.getDropNa.hasInput shouldBe true
    result.relation.getDropNa.getInput shouldBe df.relation
  }

  test("fill result relation's input is the original DataFrame relation") {
    val df = stubDf
    val result = df.na.fill(0.0, Seq("a"))
    result.relation.getFillNa.getInput shouldBe df.relation
  }

  test("replace result relation's input is the original DataFrame relation") {
    val df = stubDf
    val result = df.na.replace("a", Map(1 -> 2))
    result.relation.getReplace.getInput shouldBe df.relation
  }
