package org.apache.spark.sql

import org.apache.spark.connect.proto.expressions.Expression
import org.apache.spark.connect.proto.expressions.Expression.ExprType
import org.apache.spark.connect.proto.relations.*

/**
 * Functions for handling missing data (null / NaN) in DataFrames.
 */
final class DataFrameNaFunctions private[sql] (private val df: DataFrame):

  def drop(): DataFrame = drop("any")

  def drop(how: String): DataFrame = drop(how, df.columns.toSeq)

  def drop(how: String, cols: Seq[String]): DataFrame =
    // 'all' → set min_non_nulls 1; 'any' → unset (None)
    val minNonNulls = how.toLowerCase match
      case "all" => Some(1)
      case "any" => None
      case _     => None
    DataFrame(df.session, Relation(
      common = Some(RelationCommon(planId = Some(df.session.nextPlanId()))),
      relType = Relation.RelType.DropNa(
        NADrop(
          input = Some(df.relation),
          cols = cols,
          minNonNulls = minNonNulls
        )
      )
    ))

  def drop(minNonNulls: Int): DataFrame =
    DataFrame(df.session, Relation(
      common = Some(RelationCommon(planId = Some(df.session.nextPlanId()))),
      relType = Relation.RelType.DropNa(
        NADrop(
          input = Some(df.relation),
          cols = Seq.empty,
          minNonNulls = Some(minNonNulls)
        )
      )
    ))

  def fill(value: Double): DataFrame =
    fill(value, df.columns.toSeq)

  def fill(value: String): DataFrame =
    fill(value, df.columns.toSeq)

  def fill(value: Double, cols: Seq[String]): DataFrame =
    val lit = toLiteral(value)
    DataFrame(df.session, Relation(
      common = Some(RelationCommon(planId = Some(df.session.nextPlanId()))),
      relType = Relation.RelType.FillNa(
        NAFill(
          input = Some(df.relation),
          cols = cols,
          values = Seq(lit)
        )
      )
    ))

  def fill(value: String, cols: Seq[String]): DataFrame =
    val lit = toLiteral(value)
    DataFrame(df.session, Relation(
      common = Some(RelationCommon(planId = Some(df.session.nextPlanId()))),
      relType = Relation.RelType.FillNa(
        NAFill(
          input = Some(df.relation),
          cols = cols,
          values = Seq(lit)
        )
      )
    ))

  def fill(valueMap: Map[String, Any]): DataFrame =
    val cols = valueMap.keys.toSeq
    val values = valueMap.values.map(v => toLiteral(v)).toSeq
    DataFrame(df.session, Relation(
      common = Some(RelationCommon(planId = Some(df.session.nextPlanId()))),
      relType = Relation.RelType.FillNa(
        NAFill(
          input = Some(df.relation),
          cols = cols,
          values = values
        )
      )
    ))

  def replace[T](col: String, replacement: Map[T, T]): DataFrame =
    val replacements = replacement.map { (old, nw) =>
      NAReplace.Replacement(
        oldValue = Some(toLiteral(old)),
        newValue = Some(toLiteral(nw))
      )
    }.toSeq
    DataFrame(df.session, Relation(
      common = Some(RelationCommon(planId = Some(df.session.nextPlanId()))),
      relType = Relation.RelType.Replace(
        NAReplace(
          input = Some(df.relation),
          cols = Seq(col),
          replacements = replacements
        )
      )
    ))

  private def toLiteral(value: Any): Expression.Literal = value match
    case null       => Expression.Literal(literalType = Expression.Literal.LiteralType.Null(
                         org.apache.spark.connect.proto.types.DataType(
                           kind = org.apache.spark.connect.proto.types.DataType.Kind.Null(
                             org.apache.spark.connect.proto.types.DataType.NULL()
                           )
                         )
                       ))
    case v: Boolean => Expression.Literal(literalType = Expression.Literal.LiteralType.Boolean(v))
    case v: Int     => Expression.Literal(literalType = Expression.Literal.LiteralType.Integer(v))
    case v: Long    => Expression.Literal(literalType = Expression.Literal.LiteralType.Long(v))
    case v: Float   => Expression.Literal(literalType = Expression.Literal.LiteralType.Float(v))
    case v: Double  => Expression.Literal(literalType = Expression.Literal.LiteralType.Double(v))
    case v: String  => Expression.Literal(literalType = Expression.Literal.LiteralType.String(v))
    case v          => Expression.Literal(literalType = Expression.Literal.LiteralType.String(v.toString))
