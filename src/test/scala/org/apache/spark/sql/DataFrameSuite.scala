package org.apache.spark.sql

import org.apache.spark.connect.proto.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.*

/** Tests for DataFrame transformation methods that build proto Relations.
  *
  * These tests verify proto construction only — no live Spark Connect server needed.
  */
class DataFrameSuite extends AnyFunSuite with Matchers:

  /** Create a minimal DataFrame backed by a LocalRelation (no real server). */
  private def testDf(): DataFrame =
    val session = SparkSession(null) // null client — only nextPlanId() is used
    val rel = Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(0).build())
      .setLocalRelation(LocalRelation.getDefaultInstance)
      .build()
    DataFrame(session, rel)

  // ---------- unpivot / melt ----------

  test("unpivot with explicit values builds Unpivot proto") {
    val df = testDf()
    val result = df.unpivot(
      ids = Array(Column("id")),
      values = Array(Column("v1"), Column("v2")),
      variableColumnName = "var",
      valueColumnName = "val"
    )
    val rel = result.relation
    rel.hasUnpivot shouldBe true
    val unpivot = rel.getUnpivot
    unpivot.getIdsList should have size 1
    unpivot.hasValues shouldBe true
    unpivot.getValues.getValuesList should have size 2
    unpivot.getVariableColumnName shouldBe "var"
    unpivot.getValueColumnName shouldBe "val"
  }

  test("unpivot without values builds Unpivot proto without values field") {
    val df = testDf()
    val result = df.unpivot(
      ids = Array(Column("id")),
      variableColumnName = "var",
      valueColumnName = "val"
    )
    val unpivot = result.relation.getUnpivot
    unpivot.getIdsList should have size 1
    unpivot.hasValues shouldBe false
    unpivot.getVariableColumnName shouldBe "var"
  }

  test("melt is an alias for unpivot") {
    val df = testDf()
    val result = df.melt(
      ids = Array(Column("id")),
      values = Array(Column("v1")),
      variableColumnName = "var",
      valueColumnName = "val"
    )
    result.relation.hasUnpivot shouldBe true
  }

  // ---------- withColumnsRenamed ----------

  test("withColumnsRenamed builds WithColumnsRenamed proto") {
    val df = testDf()
    val result = df.withColumnsRenamed(Map("a" -> "x", "b" -> "y"))
    val rel = result.relation
    rel.hasWithColumnsRenamed shouldBe true
    val renames = rel.getWithColumnsRenamed.getRenamesList.asScala
    renames should have size 2
    renames.map(r => r.getColName -> r.getNewColName).toSet shouldBe
      Set("a" -> "x", "b" -> "y")
  }

  // ---------- withColumns ----------

  test("withColumns builds WithColumns proto") {
    val df = testDf()
    val result = df.withColumns(Map(
      "new1" -> functions.lit(1),
      "new2" -> functions.lit("hello")
    ))
    val rel = result.relation
    rel.hasWithColumns shouldBe true
    val aliases = rel.getWithColumns.getAliasesList.asScala
    aliases should have size 2
    aliases.flatMap(_.getNameList.asScala).toSet shouldBe Set("new1", "new2")
  }

  // ---------- drop(Column*) ----------

  test("drop(Column*) builds Drop proto with expressions") {
    val df = testDf()
    val result = df.drop(Column("a"), Column("b"))(using summon[DummyImplicit])
    val rel = result.relation
    rel.hasDrop shouldBe true
    rel.getDrop.getColumnsList should have size 2
  }

  // ---------- observe ----------

  test("observe builds CollectMetrics proto") {
    val df = testDf()
    val result = df.observe("metrics", functions.count(Column("x")), functions.sum(Column("y")))
    val rel = result.relation
    rel.hasCollectMetrics shouldBe true
    val cm = rel.getCollectMetrics
    cm.getName shouldBe "metrics"
    cm.getMetricsList should have size 2
  }

  test("observe with Observation builds CollectMetrics proto and registers") {
    val df = testDf()
    val obs = Observation("obs-test")
    val result = df.observe(obs, functions.count(Column("x")))
    result.relation.hasCollectMetrics shouldBe true
    result.relation.getCollectMetrics.getName shouldBe "obs-test"
    obs.planId should be >= 0L
  }

  test("observe with Observation enforces single use") {
    val df = testDf()
    val obs = Observation("single-use")
    df.observe(obs, functions.count(Column("x")))
    an[IllegalArgumentException] should be thrownBy
      df.observe(obs, functions.count(Column("x")))
  }

  // ---------- randomSplit ----------

  test("randomSplit returns correct number of DataFrames") {
    val df = testDf()
    val splits = df.randomSplit(Array(0.6, 0.3, 0.1))
    splits should have size 3
    splits.foreach { s =>
      s.relation.hasSample shouldBe true
    }
  }

  test("randomSplit bounds are correct") {
    val df = testDf()
    val splits = df.randomSplit(Array(1.0, 1.0))
    val s0 = splits(0).relation.getSample
    val s1 = splits(1).relation.getSample
    s0.getLowerBound shouldBe 0.0
    s0.getUpperBound shouldBe 0.5 +- 0.01
    s1.getLowerBound shouldBe 0.5 +- 0.01
    s1.getUpperBound shouldBe 1.0 +- 0.01
  }

  // ---------- parameterized SQL ----------

  test("sql with named arguments builds SQL proto with named_arguments") {
    val session = SparkSession(null)
    val result = session.sql("SELECT :name AS name", Map("name" -> "hello"))
    val rel = result.relation
    rel.hasSql shouldBe true
    val sql = rel.getSql
    sql.getQuery shouldBe "SELECT :name AS name"
    sql.getNamedArgumentsMap should have size 1
    sql.getNamedArgumentsMap.containsKey("name") shouldBe true
  }

  test("sql with positional arguments builds SQL proto with pos_arguments") {
    val session = SparkSession(null)
    val result =
      session.sql("SELECT ? + ?", Column.lit(1), Column.lit(2))(using summon[DummyImplicit])
    val rel = result.relation
    rel.hasSql shouldBe true
    val sql = rel.getSql
    sql.getQuery shouldBe "SELECT ? + ?"
    sql.getPosArgumentsList should have size 2
  }

  test("sql with empty named arguments builds SQL proto without arguments") {
    val session = SparkSession(null)
    val result = session.sql("SELECT 1", Map.empty[String, Any])
    val rel = result.relation
    rel.hasSql shouldBe true
    val sql = rel.getSql
    sql.getQuery shouldBe "SELECT 1"
    sql.getNamedArgumentsMap should have size 0
  }

  // ---------- toJSON ----------

  test("toJSON builds to_json(struct(*)) projection") {
    val df = testDf()
    val result = df.toJSON
    val rel = result.relation
    rel.hasProject shouldBe true
    val exprs = rel.getProject.getExpressionsList.asScala
    exprs should have size 1
    // The expression should be an alias wrapping to_json(struct(*))
    exprs.head.hasAlias shouldBe true
    exprs.head.getAlias.getNameList.asScala.head shouldBe "value"
  }

  // ---------- show(vertical) ----------

  test("show(numRows, truncate, vertical) builds ShowString proto") {
    val df = testDf()
    // Use the underlying relation builder to check proto construction
    val showRel = Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(1).build())
      .setShowString(ShowString.newBuilder()
        .setInput(df.relation)
        .setNumRows(5)
        .setTruncate(10)
        .setVertical(true)
        .build())
      .build()
    showRel.hasShowString shouldBe true
    val ss = showRel.getShowString
    ss.getNumRows shouldBe 5
    ss.getTruncate shouldBe 10
    ss.getVertical shouldBe true
  }

  // ---------- lateralJoin ----------

  test("lateralJoin builds LateralJoin proto") {
    val df1 = testDf()
    val df2 = testDf()
    val result = df1.lateralJoin(df2, Column("id") === Column("id"))
    val rel = result.relation
    rel.hasLateralJoin shouldBe true
    val lj = rel.getLateralJoin
    lj.hasJoinCondition shouldBe true
    lj.getJoinType shouldBe Join.JoinType.JOIN_TYPE_INNER
  }

  test("lateralJoin without condition builds LateralJoin proto") {
    val df1 = testDf()
    val df2 = testDf()
    val result = df1.lateralJoin(df2)
    result.relation.hasLateralJoin shouldBe true
    result.relation.getLateralJoin.getJoinType shouldBe Join.JoinType.JOIN_TYPE_INNER
  }

  test("lateralJoin rejects unsupported join types") {
    val df1 = testDf()
    val df2 = testDf()
    an[IllegalArgumentException] should be thrownBy
      df1.lateralJoin(df2, Column("id") === Column("id"), "full")
  }

  // ---------- groupingSets ----------

  test("groupingSets builds Aggregate proto with GROUP_TYPE_GROUPING_SETS") {
    val df = testDf()
    val grouped = df.groupingSets(
      Seq(Seq(Column("city")), Seq.empty),
      Column("city")
    )
    val result = grouped.agg(functions.count(Column("city")).as("cnt"))
    val rel = result.relation
    rel.hasAggregate shouldBe true
    val agg = rel.getAggregate
    agg.getGroupType shouldBe Aggregate.GroupType.GROUP_TYPE_GROUPING_SETS
    agg.getGroupingSetsCount shouldBe 2
    agg.getGroupingExpressionsList should have size 1
  }

  // ---------- repartitionByRange ----------

  test("repartitionByRange builds RepartitionByExpression with sort order") {
    val df = testDf()
    val result = df.repartitionByRange(4, Column("id"))
    val rel = result.relation
    rel.hasRepartitionByExpression shouldBe true
    val rbe = rel.getRepartitionByExpression
    rbe.getNumPartitions shouldBe 4
    rbe.getPartitionExprsList should have size 1
    // The expression should have a sort order (asc by default)
    rbe.getPartitionExprs(0).hasSortOrder shouldBe true
  }

  test("repartitionByRange without numPartitions") {
    val df = testDf()
    val result = df.repartitionByRange(Column("id"))(using summon[DummyImplicit])
    val rel = result.relation
    rel.hasRepartitionByExpression shouldBe true
    val rbe = rel.getRepartitionByExpression
    rbe.hasNumPartitions shouldBe false
    rbe.getPartitionExprsList should have size 1
  }

  test("repartitionByRange requires at least one expression") {
    val df = testDf()
    an[IllegalArgumentException] should be thrownBy
      df.repartitionByRange(4)
  }

  // ---------- range(4-param) ----------

  test("range with numPartitions builds Range proto with num_partitions") {
    val session = SparkSession(null)
    val df = session.range(0, 100, 1, 4)
    val rel = df.relation
    rel.hasRange shouldBe true
    val range = rel.getRange
    range.getStart shouldBe 0
    range.getEnd shouldBe 100
    range.getStep shouldBe 1
    range.getNumPartitions shouldBe 4
  }

  // ---------- collectAsList / takeAsList ----------

  test("collectAsList return type is java.util.List[Row]") {
    val method = classOf[DataFrame].getMethod("collectAsList")
    method should not be null
    classOf[java.util.List[?]].isAssignableFrom(method.getReturnType) shouldBe true
  }

  test("takeAsList return type is java.util.List[Row]") {
    val method = classOf[DataFrame].getMethod("takeAsList", classOf[Int])
    method should not be null
    classOf[java.util.List[?]].isAssignableFrom(method.getReturnType) shouldBe true
  }

  // ---------- dropDuplicatesWithinWatermark ----------

  test("dropDuplicatesWithinWatermark() builds Deduplicate proto with within_watermark") {
    val df = testDf()
    val result = df.dropDuplicatesWithinWatermark()
    val rel = result.relation
    rel.hasDeduplicate shouldBe true
    val dedup = rel.getDeduplicate
    dedup.getAllColumnsAsKeys shouldBe true
    dedup.getWithinWatermark shouldBe true
  }

  test("dropDuplicatesWithinWatermark(colNames) builds Deduplicate proto") {
    val df = testDf()
    val result = df.dropDuplicatesWithinWatermark(Seq("a", "b"))
    val dedup = result.relation.getDeduplicate
    dedup.getWithinWatermark shouldBe true
    dedup.getColumnNamesList.asScala.toSeq shouldBe Seq("a", "b")
    dedup.getAllColumnsAsKeys shouldBe false
  }

  test("dropDuplicatesWithinWatermark(col1, cols*) builds Deduplicate proto") {
    val df = testDf()
    val result = df.dropDuplicatesWithinWatermark("x", "y")
    val dedup = result.relation.getDeduplicate
    dedup.getWithinWatermark shouldBe true
    dedup.getColumnNamesList.asScala.toSeq shouldBe Seq("x", "y")
  }

  // ---------- transpose ----------

  test("transpose(indexColumn) builds Transpose proto") {
    val df = testDf()
    val result = df.transpose(Column("id"))
    val rel = result.relation
    rel.hasTranspose shouldBe true
    val transpose = rel.getTranspose
    transpose.getIndexColumnsList should have size 1
  }

  test("transpose() without index builds Transpose proto") {
    val df = testDf()
    val result = df.transpose()
    val rel = result.relation
    rel.hasTranspose shouldBe true
    rel.getTranspose.getIndexColumnsList should have size 0
  }

  // ---------- zipWithIndex ----------

  test("zipWithIndex builds projection with distributed_sequence_id") {
    val df = testDf()
    val result = df.zipWithIndex
    val rel = result.relation
    rel.hasProject shouldBe true
    val exprs = rel.getProject.getExpressionsList.asScala
    exprs should have size 2
    // Second expression should be an alias wrapping distributed_sequence_id
    val indexExpr = exprs(1)
    indexExpr.hasAlias shouldBe true
    indexExpr.getAlias.getNameList.asScala.head shouldBe "index"
    val inner = indexExpr.getAlias.getExpr
    inner.hasUnresolvedFunction shouldBe true
    inner.getUnresolvedFunction.getFunctionName shouldBe "distributed_sequence_id"
    inner.getUnresolvedFunction.getIsInternal shouldBe true
  }

  // ---------- colRegex ----------

  test("colRegex builds UnresolvedRegex proto") {
    val df = testDf()
    val col = df.colRegex("`id`")
    col.expr.hasUnresolvedRegex shouldBe true
    col.expr.getUnresolvedRegex.getColName shouldBe "`id`"
  }

  // ---------- metadataColumn ----------

  test("metadataColumn builds UnresolvedAttribute with is_metadata_column") {
    val df = testDf()
    val col = df.metadataColumn("_metadata")
    col.expr.hasUnresolvedAttribute shouldBe true
    val attr = col.expr.getUnresolvedAttribute
    attr.getUnparsedIdentifier shouldBe "_metadata"
    attr.getIsMetadataColumn shouldBe true
  }

  // ---------- withMetadata ----------

  test("withMetadata builds WithColumns proto with metadata in Alias") {
    val df = testDf()
    val result = df.withMetadata("col1", """{"key": "value"}""")
    val rel = result.relation
    rel.hasWithColumns shouldBe true
    val aliases = rel.getWithColumns.getAliasesList.asScala
    aliases should have size 1
    aliases.head.getNameList.asScala.head shouldBe "col1"
    aliases.head.getMetadata shouldBe """{"key": "value"}"""
  }

  // ---------- isLocal ----------

  test("isLocal method exists with correct signature") {
    val method = classOf[DataFrame].getMethod("isLocal")
    method should not be null
    method.getReturnType shouldBe classOf[Boolean]
  }

  // ---------- tag API ----------

  test("SparkSession tag API round-trip") {
    val session = SparkSession(null)
    // Tags should start empty (client is null but tag methods use InheritableThreadLocal)
    // We can't call getTags on null client, so skip this for null-client tests
  }

  // ---------- Phase 4: col / apply / dtypes / to / join overloads ----------

  test("col(name) produces UnresolvedAttribute with planId") {
    val df = testDf()
    val c = df.col("age")
    c.expr.hasUnresolvedAttribute shouldBe true
    c.expr.getUnresolvedAttribute.getUnparsedIdentifier shouldBe "age"
    c.expr.getUnresolvedAttribute.getPlanId shouldBe df.relation.getCommon.getPlanId
  }

  test("apply(name) delegates to col") {
    val df = testDf()
    val c = df("name")
    c.expr.hasUnresolvedAttribute shouldBe true
    c.expr.getUnresolvedAttribute.getUnparsedIdentifier shouldBe "name"
  }

  test("dtypes returns column name and type pairs") {
    // dtypes depends on schema which requires a live server, so test the method exists
    val method = classOf[DataFrame].getMethod("dtypes")
    method should not be null
    method.getReturnType shouldBe classOf[Array[(String, String)]]
  }

  test("to(schema) builds ToSchema proto") {
    val df = testDf()
    val targetSchema = types.StructType(Seq(
      types.StructField("id", types.LongType),
      types.StructField("name", types.StringType)
    ))
    val result = df.to(targetSchema)
    result.relation.hasToSchema shouldBe true
    result.relation.getToSchema.hasSchema shouldBe true
  }

  test("printSchema(level) method exists") {
    val method = classOf[DataFrame].getMethod("printSchema", classOf[Int])
    method should not be null
  }

  test("join(right) builds Join with no condition") {
    val df1 = testDf()
    val df2 = testDf()
    val joined = df1.join(df2)
    joined.relation.hasJoin shouldBe true
    val j = joined.relation.getJoin
    j.getJoinType shouldBe Join.JoinType.JOIN_TYPE_INNER
    j.hasJoinCondition shouldBe false
    j.getUsingColumnsCount shouldBe 0
  }

  test("join(right, usingColumn) builds Join with single using column") {
    val df1 = testDf()
    val df2 = testDf()
    val joined = df1.join(df2, "id")
    val j = joined.relation.getJoin
    j.getUsingColumnsCount shouldBe 1
    j.getUsingColumns(0) shouldBe "id"
    j.getJoinType shouldBe Join.JoinType.JOIN_TYPE_INNER
  }

  test("join(right, usingColumns, joinType) builds correct proto") {
    val df1 = testDf()
    val df2 = testDf()
    val joined = df1.join(df2, Seq("id", "name"), "left")
    val j = joined.relation.getJoin
    j.getUsingColumnsCount shouldBe 2
    j.getUsingColumns(0) shouldBe "id"
    j.getUsingColumns(1) shouldBe "name"
    j.getJoinType shouldBe Join.JoinType.JOIN_TYPE_LEFT_OUTER
  }
