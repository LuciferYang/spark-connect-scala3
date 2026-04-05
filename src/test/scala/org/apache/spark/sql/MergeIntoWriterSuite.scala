package org.apache.spark.sql

import org.apache.spark.connect.proto.*
import org.scalatest.funsuite.AnyFunSuite

class MergeIntoWriterSuite extends AnyFunSuite:

  /** Minimal DataFrame stub for proto construction tests. */
  private def dummyDf: DataFrame =
    val rel = Relation.newBuilder()
      .setRange(Range.newBuilder().setStart(0).setEnd(10).setStep(1).build())
      .build()
    DataFrame(null, rel)

  test("mergeInto returns MergeIntoWriter") {
    val df = dummyDf
    val writer = MergeIntoWriter("target", df, Column("id") === Column("id"))
    assert(writer != null)
  }

  test("WhenMatched fluent API compiles") {
    val df = dummyDf
    val writer = MergeIntoWriter("target", df, Column("id") === Column("id"))
    val result = writer.whenMatched().updateAll()
    assert(result eq writer)
  }

  test("WhenMatched with condition and update map") {
    val df = dummyDf
    val writer = MergeIntoWriter("target", df, Column("id") === Column("id"))
    val result = writer
      .whenMatched(Column("status") === Column.lit("active"))
      .update(Map("name" -> Column("source_name")))
    assert(result eq writer)
  }

  test("WhenMatched delete") {
    val df = dummyDf
    val writer = MergeIntoWriter("target", df, Column("id") === Column("id"))
    val result = writer.whenMatched().delete()
    assert(result eq writer)
  }

  test("WhenNotMatched insertAll") {
    val df = dummyDf
    val writer = MergeIntoWriter("target", df, Column("id") === Column("id"))
    val result = writer.whenNotMatched().insertAll()
    assert(result eq writer)
  }

  test("WhenNotMatched insert with assignments") {
    val df = dummyDf
    val writer = MergeIntoWriter("target", df, Column("id") === Column("id"))
    val result = writer
      .whenNotMatched()
      .insert(Map("id" -> Column("source_id"), "name" -> Column("source_name")))
    assert(result eq writer)
  }

  test("WhenNotMatchedBySource updateAll") {
    val df = dummyDf
    val writer = MergeIntoWriter("target", df, Column("id") === Column("id"))
    val result = writer.whenNotMatchedBySource().updateAll()
    assert(result eq writer)
  }

  test("WhenNotMatchedBySource delete") {
    val df = dummyDf
    val writer = MergeIntoWriter("target", df, Column("id") === Column("id"))
    val result = writer.whenNotMatchedBySource().delete()
    assert(result eq writer)
  }

  test("withSchemaEvolution returns same writer") {
    val df = dummyDf
    val writer = MergeIntoWriter("target", df, Column("id") === Column("id"))
    val result = writer.withSchemaEvolution()
    assert(result eq writer)
  }

  test("merge without actions throws SparkException") {
    val df = dummyDf
    val writer = MergeIntoWriter("target", df, Column("id") === Column("id"))
    val ex = intercept[SparkException] {
      writer.merge()
    }
    assert(ex.errorClass.contains("NO_MERGE_ACTION_SPECIFIED"))
  }

  test("full fluent chain compiles") {
    val df = dummyDf
    val writer = MergeIntoWriter("target", df, Column("id") === Column("id"))
    val result = writer
      .whenMatched().updateAll()
      .whenNotMatched().insertAll()
      .whenNotMatchedBySource().delete()
      .withSchemaEvolution()
    assert(result eq writer)
  }

  test("buildMergeAction produces correct proto for UPDATE_STAR") {
    val df = dummyDf
    val writer = MergeIntoWriter("target", df, Column("id") === Column("id"))
    val action = writer.buildMergeAction(
      MergeAction.ActionType.ACTION_TYPE_UPDATE_STAR,
      None
    )
    assert(action.hasMergeAction)
    assert(action.getMergeAction.getActionType == MergeAction.ActionType.ACTION_TYPE_UPDATE_STAR)
    assert(!action.getMergeAction.hasCondition)
    assert(action.getMergeAction.getAssignmentsCount == 0)
  }

  test("buildMergeAction produces correct proto for INSERT with assignments") {
    val df = dummyDf
    val writer = MergeIntoWriter("target", df, Column("id") === Column("id"))
    val condition = Column("flag") === Column.lit(true)
    val action = writer.buildMergeAction(
      MergeAction.ActionType.ACTION_TYPE_INSERT,
      Some(condition),
      Map("name" -> Column("src_name"))
    )
    assert(action.hasMergeAction)
    val ma = action.getMergeAction
    assert(ma.getActionType == MergeAction.ActionType.ACTION_TYPE_INSERT)
    assert(ma.hasCondition)
    assert(ma.getAssignmentsCount == 1)
  }

  test("buildMergeAction produces correct proto for DELETE with condition") {
    val df = dummyDf
    val writer = MergeIntoWriter("target", df, Column("id") === Column("id"))
    val condition = Column("status") === Column.lit("deleted")
    val action = writer.buildMergeAction(
      MergeAction.ActionType.ACTION_TYPE_DELETE,
      Some(condition)
    )
    assert(action.hasMergeAction)
    val ma = action.getMergeAction
    assert(ma.getActionType == MergeAction.ActionType.ACTION_TYPE_DELETE)
    assert(ma.hasCondition)
    assert(ma.getAssignmentsCount == 0)
  }
