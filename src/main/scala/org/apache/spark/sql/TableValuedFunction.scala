package org.apache.spark.sql

import org.apache.spark.connect.proto.{
  Relation,
  RelationCommon,
  UnresolvedTableValuedFunction as ProtoTVF
}

import scala.jdk.CollectionConverters.*

/** Provides access to table-valued functions (TVFs) such as `explode`, `inline`, `posexplode`, etc.
  *
  * Access via `SparkSession.tvf`:
  * {{{
  *   spark.tvf.explode(array(lit(1), lit(2), lit(3))).show()
  *   spark.tvf.range(10).show()
  * }}}
  */
class TableValuedFunction private[sql] (sparkSession: SparkSession):

  // ---------------------------------------------------------------------------
  // Range (delegates to SparkSession.range)
  // ---------------------------------------------------------------------------

  def range(end: Long): DataFrame = sparkSession.range(end)

  def range(start: Long, end: Long): DataFrame = sparkSession.range(start, end)

  def range(start: Long, end: Long, step: Long): DataFrame =
    sparkSession.range(start, end, step)

  // ---------------------------------------------------------------------------
  // Explode family
  // ---------------------------------------------------------------------------

  def explode(collection: Column): DataFrame = fn("explode", Seq(collection))

  def explode_outer(collection: Column): DataFrame = fn("explode_outer", Seq(collection))

  def posexplode(collection: Column): DataFrame = fn("posexplode", Seq(collection))

  def posexplode_outer(collection: Column): DataFrame = fn("posexplode_outer", Seq(collection))

  // ---------------------------------------------------------------------------
  // Inline family
  // ---------------------------------------------------------------------------

  def inline(input: Column): DataFrame = fn("inline", Seq(input))

  def inline_outer(input: Column): DataFrame = fn("inline_outer", Seq(input))

  // ---------------------------------------------------------------------------
  // JSON / Stack / Collations / SQL keywords / Variant
  // ---------------------------------------------------------------------------

  def json_tuple(input: Column, fields: Column*): DataFrame = fn("json_tuple", input +: fields)

  def stack(n: Column, fields: Column*): DataFrame = fn("stack", n +: fields)

  def collations(): DataFrame = fn("collations", Seq.empty)

  def sql_keywords(): DataFrame = fn("sql_keywords", Seq.empty)

  def variant_explode(input: Column): DataFrame = fn("variant_explode", Seq(input))

  def variant_explode_outer(input: Column): DataFrame =
    fn("variant_explode_outer", Seq(input))

  // ---------------------------------------------------------------------------
  // Private helper
  // ---------------------------------------------------------------------------

  private def fn(name: String, args: Seq[Column]): DataFrame =
    val relation = Relation
      .newBuilder()
      .setCommon(
        RelationCommon.newBuilder().setPlanId(sparkSession.nextPlanId()).build()
      )
      .setUnresolvedTableValuedFunction(
        ProtoTVF
          .newBuilder()
          .setFunctionName(name)
          .addAllArguments(args.map(_.expr).asJava)
      )
      .build()
    DataFrame(sparkSession, relation)
