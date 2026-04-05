package org.apache.spark.sql

import scala.jdk.CollectionConverters.*
import org.apache.spark.connect.proto.*

/** Writer for executing MERGE INTO operations.
  *
  * {{{
  *   source.mergeInto("target_table", source("id") === target("id"))
  *     .whenMatched().updateAll()
  *     .whenNotMatched().insertAll()
  *     .merge()
  * }}}
  */
final class MergeIntoWriter private[sql] (table: String, df: DataFrame, on: Column):

  private val builder = MergeIntoTableCommand.newBuilder()
    .setTargetTableName(table)
    .setSourceTablePlan(df.relation)
    .setMergeCondition(on.expr)

  private var hasActions: Boolean = false

  def whenMatched(): WhenMatched = WhenMatched(this, None)
  def whenMatched(condition: Column): WhenMatched = WhenMatched(this, Some(condition))

  def whenNotMatched(): WhenNotMatched = WhenNotMatched(this, None)
  def whenNotMatched(condition: Column): WhenNotMatched = WhenNotMatched(this, Some(condition))

  def whenNotMatchedBySource(): WhenNotMatchedBySource = WhenNotMatchedBySource(this, None)
  def whenNotMatchedBySource(condition: Column): WhenNotMatchedBySource =
    WhenNotMatchedBySource(this, Some(condition))

  def withSchemaEvolution(): MergeIntoWriter =
    builder.setWithSchemaEvolution(true)
    this

  def merge(): Unit =
    if !hasActions then
      throw SparkException(
        "MERGE operation requires at least one action",
        errorClass = Some("NO_MERGE_ACTION_SPECIFIED")
      )
    val command = Command.newBuilder()
      .setMergeIntoTableCommand(builder.build())
      .build()
    val plan = Plan.newBuilder().setCommand(command).build()
    val responses = df.session.client.execute(plan)
    responses.foreach(_ => ()) // drain iterator

  // ---- Internal callbacks used by WhenXxx helpers ----

  private[sql] def addMatchAction(action: Expression): MergeIntoWriter =
    builder.addMatchActions(action)
    hasActions = true
    this

  private[sql] def addNotMatchedAction(action: Expression): MergeIntoWriter =
    builder.addNotMatchedActions(action)
    hasActions = true
    this

  private[sql] def addNotMatchedBySourceAction(action: Expression): MergeIntoWriter =
    builder.addNotMatchedBySourceActions(action)
    hasActions = true
    this

  // ---- Proto helper ----

  private[sql] def buildMergeAction(
      actionType: MergeAction.ActionType,
      condition: Option[Column],
      assignments: Map[String, Column] = Map.empty
  ): Expression =
    val actionBuilder = MergeAction.newBuilder().setActionType(actionType)
    condition.foreach(c => actionBuilder.setCondition(c.expr))
    assignments.foreach { (key, value) =>
      actionBuilder.addAssignments(
        MergeAction.Assignment.newBuilder()
          .setKey(functions.expr(key).expr)
          .setValue(value.expr)
          .build()
      )
    }
    Expression.newBuilder().setMergeAction(actionBuilder.build()).build()

// ---------------------------------------------------------------------------
// When-clause helper classes
// ---------------------------------------------------------------------------

/** Actions available when the merge condition is matched. */
final class WhenMatched private[sql] (writer: MergeIntoWriter, condition: Option[Column]):

  def updateAll(): MergeIntoWriter =
    writer.addMatchAction(
      writer.buildMergeAction(MergeAction.ActionType.ACTION_TYPE_UPDATE_STAR, condition)
    )

  def update(assignments: Map[String, Column]): MergeIntoWriter =
    writer.addMatchAction(
      writer.buildMergeAction(MergeAction.ActionType.ACTION_TYPE_UPDATE, condition, assignments)
    )

  def delete(): MergeIntoWriter =
    writer.addMatchAction(
      writer.buildMergeAction(MergeAction.ActionType.ACTION_TYPE_DELETE, condition)
    )

/** Actions available when the merge condition is not matched (source rows without target match). */
final class WhenNotMatched private[sql] (writer: MergeIntoWriter, condition: Option[Column]):

  def insertAll(): MergeIntoWriter =
    writer.addNotMatchedAction(
      writer.buildMergeAction(MergeAction.ActionType.ACTION_TYPE_INSERT_STAR, condition)
    )

  def insert(assignments: Map[String, Column]): MergeIntoWriter =
    writer.addNotMatchedAction(
      writer.buildMergeAction(MergeAction.ActionType.ACTION_TYPE_INSERT, condition, assignments)
    )

/** Actions available when the merge condition is not matched by source (target rows without source
  * match).
  */
final class WhenNotMatchedBySource private[sql] (
    writer: MergeIntoWriter,
    condition: Option[Column]
):

  def updateAll(): MergeIntoWriter =
    writer.addNotMatchedBySourceAction(
      writer.buildMergeAction(MergeAction.ActionType.ACTION_TYPE_UPDATE_STAR, condition)
    )

  def update(assignments: Map[String, Column]): MergeIntoWriter =
    writer.addNotMatchedBySourceAction(
      writer.buildMergeAction(MergeAction.ActionType.ACTION_TYPE_UPDATE, condition, assignments)
    )

  def delete(): MergeIntoWriter =
    writer.addNotMatchedBySourceAction(
      writer.buildMergeAction(MergeAction.ActionType.ACTION_TYPE_DELETE, condition)
    )
