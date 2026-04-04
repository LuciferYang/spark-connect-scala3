package org.apache.spark.sql

import org.apache.spark.connect.proto.*
import org.apache.spark.connect.proto.DataType as ProtoDataType

/** Functions for handling missing data (null / NaN) in DataFrames.
  */
final class DataFrameNaFunctions private[sql] (private val df: DataFrame):

  def drop(): DataFrame = drop("any")

  def drop(how: String): DataFrame = drop(how, df.columns.toSeq)

  def drop(how: String, cols: Seq[String]): DataFrame =
    val naDropBuilder = NADrop.newBuilder().setInput(df.relation)
    cols.foreach(naDropBuilder.addCols)
    how.toLowerCase match
      case "all" => naDropBuilder.setMinNonNulls(1)
      case _     => // 'any' or default — don't set minNonNulls
    df.withRelation(_.setDropNa(naDropBuilder.build()))

  def drop(minNonNulls: Int): DataFrame =
    val naDropBuilder = NADrop.newBuilder()
      .setInput(df.relation)
      .setMinNonNulls(minNonNulls)
    df.withRelation(_.setDropNa(naDropBuilder.build()))

  def fill(value: Double): DataFrame =
    fill(value, df.columns.toSeq)

  def fill(value: String): DataFrame =
    fill(value, df.columns.toSeq)

  def fill(value: Double, cols: Seq[String]): DataFrame =
    val lit = toLiteral(value)
    val naFillBuilder = NAFill.newBuilder().setInput(df.relation).addValues(lit)
    cols.foreach(naFillBuilder.addCols)
    df.withRelation(_.setFillNa(naFillBuilder.build()))

  def fill(value: String, cols: Seq[String]): DataFrame =
    val lit = toLiteral(value)
    val naFillBuilder = NAFill.newBuilder().setInput(df.relation).addValues(lit)
    cols.foreach(naFillBuilder.addCols)
    df.withRelation(_.setFillNa(naFillBuilder.build()))

  def fill(valueMap: Map[String, Any]): DataFrame =
    val naFillBuilder = NAFill.newBuilder().setInput(df.relation)
    valueMap.keys.foreach(naFillBuilder.addCols)
    valueMap.values.foreach(v => naFillBuilder.addValues(toLiteral(v)))
    df.withRelation(_.setFillNa(naFillBuilder.build()))

  def replace[T](col: String, replacement: Map[T, T]): DataFrame =
    val naReplaceBuilder = NAReplace.newBuilder()
      .setInput(df.relation)
      .addCols(col)
    replacement.foreach { (old, nw) =>
      naReplaceBuilder.addReplacements(
        NAReplace.Replacement.newBuilder()
          .setOldValue(toLiteral(old))
          .setNewValue(toLiteral(nw))
          .build()
      )
    }
    df.withRelation(_.setReplace(naReplaceBuilder.build()))

  private def toLiteral(value: Any): Expression.Literal = value match
    case null => Expression.Literal.newBuilder()
        .setNull(ProtoDataType.newBuilder()
          .setNull(ProtoDataType.NULL.getDefaultInstance).build())
        .build()
    case v: Boolean => Expression.Literal.newBuilder().setBoolean(v).build()
    case v: Int     => Expression.Literal.newBuilder().setInteger(v).build()
    case v: Long    => Expression.Literal.newBuilder().setLong(v).build()
    case v: Float   => Expression.Literal.newBuilder().setFloat(v).build()
    case v: Double  => Expression.Literal.newBuilder().setDouble(v).build()
    case v: String  => Expression.Literal.newBuilder().setString(v).build()
    case v          => Expression.Literal.newBuilder().setString(v.toString).build()
