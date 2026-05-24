package org.apache.spark.sql

import org.apache.spark.connect.proto.*

/** Writer for executing MERGE INTO operations.
  *
  * {{{
  *   source.mergeInto("target_table", source("id") === target("id"))
  *     .whenMatched().updateAll()
  *     .whenNotMatched().insertAll()
  *     .merge()
  * }}}
  *
  * The type parameter `T` propagates the source dataset element type through the DSL chain so typed
  * `Dataset[T].mergeInto(...)` flows the element type into the `WhenMatched[T]` /
  * `WhenNotMatched[T]` / `WhenNotMatchedBySource[T]` helper classes.
  */
final class MergeIntoWriter[T] private[sql] (table: String, df: DataFrame, on: Column):

  private val builder = MergeIntoTableCommand.newBuilder()
    .setTargetTableName(table)
    .setSourceTablePlan(df.relation)
    .setMergeCondition(on.expr)

  private var hasActions: Boolean = false

  def whenMatched(): WhenMatched[T] = WhenMatched(this, None)
  def whenMatched(condition: Column): WhenMatched[T] = WhenMatched(this, Some(condition))

  def whenNotMatched(): WhenNotMatched[T] = WhenNotMatched(this, None)
  def whenNotMatched(condition: Column): WhenNotMatched[T] = WhenNotMatched(this, Some(condition))

  def whenNotMatchedBySource(): WhenNotMatchedBySource[T] = WhenNotMatchedBySource(this, None)
  def whenNotMatchedBySource(condition: Column): WhenNotMatchedBySource[T] =
    WhenNotMatchedBySource(this, Some(condition))

  def withSchemaEvolution(): MergeIntoWriter[T] =
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
    df.session.client.executeCommand(command)

  // ---- Internal callbacks used by WhenXxx helpers ----

  private[sql] def addMatchAction(action: Expression): MergeIntoWriter[T] =
    builder.addMatchActions(action)
    hasActions = true
    this

  private[sql] def addNotMatchedAction(action: Expression): MergeIntoWriter[T] =
    builder.addNotMatchedActions(action)
    hasActions = true
    this

  private[sql] def addNotMatchedBySourceAction(action: Expression): MergeIntoWriter[T] =
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

private[sql] object MergeIntoWriter:
  /** Convenience constructor: untyped writer over a DataFrame, defaults to Row marker. */
  def apply(table: String, df: DataFrame, on: Column): MergeIntoWriter[Row] =
    new MergeIntoWriter[Row](table, df, on)

// ---------------------------------------------------------------------------
// When-clause helper classes
// ---------------------------------------------------------------------------

/** Actions available when the merge condition is matched. */
final class WhenMatched[T] private[sql] (writer: MergeIntoWriter[T], condition: Option[Column]):

  def updateAll(): MergeIntoWriter[T] =
    writer.addMatchAction(
      writer.buildMergeAction(MergeAction.ActionType.ACTION_TYPE_UPDATE_STAR, condition)
    )

  def update(assignments: Map[String, Column]): MergeIntoWriter[T] =
    writer.addMatchAction(
      writer.buildMergeAction(MergeAction.ActionType.ACTION_TYPE_UPDATE, condition, assignments)
    )

  def delete(): MergeIntoWriter[T] =
    writer.addMatchAction(
      writer.buildMergeAction(MergeAction.ActionType.ACTION_TYPE_DELETE, condition)
    )

/** Actions available when the merge condition is not matched (source rows without target match). */
final class WhenNotMatched[T] private[sql] (writer: MergeIntoWriter[T], condition: Option[Column]):

  def insertAll(): MergeIntoWriter[T] =
    writer.addNotMatchedAction(
      writer.buildMergeAction(MergeAction.ActionType.ACTION_TYPE_INSERT_STAR, condition)
    )

  def insert(assignments: Map[String, Column]): MergeIntoWriter[T] =
    writer.addNotMatchedAction(
      writer.buildMergeAction(MergeAction.ActionType.ACTION_TYPE_INSERT, condition, assignments)
    )

/** Actions available when the merge condition is not matched by source (target rows without source
  * match).
  */
final class WhenNotMatchedBySource[T] private[sql] (
    writer: MergeIntoWriter[T],
    condition: Option[Column]
):

  def updateAll(): MergeIntoWriter[T] =
    writer.addNotMatchedBySourceAction(
      writer.buildMergeAction(MergeAction.ActionType.ACTION_TYPE_UPDATE_STAR, condition)
    )

  def update(assignments: Map[String, Column]): MergeIntoWriter[T] =
    writer.addNotMatchedBySourceAction(
      writer.buildMergeAction(MergeAction.ActionType.ACTION_TYPE_UPDATE, condition, assignments)
    )

  def delete(): MergeIntoWriter[T] =
    writer.addNotMatchedBySourceAction(
      writer.buildMergeAction(MergeAction.ActionType.ACTION_TYPE_DELETE, condition)
    )
