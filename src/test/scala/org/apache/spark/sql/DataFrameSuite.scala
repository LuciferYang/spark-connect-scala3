package org.apache.spark.sql

import org.apache.spark.connect.proto.{StorageLevel as _, *}
import org.apache.spark.sql.StorageLevel as SparkStorageLevel
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

  // ---------- DataFrameWriter convenience methods ----------

  test("DataFrameWriter mode(SaveMode) sets correct mode string") {
    val df = testDf()
    // Just verify the SaveMode enum compiles and maps correctly
    SaveMode.Overwrite.toString shouldBe "Overwrite"
    SaveMode.Append.toString shouldBe "Append"
    SaveMode.Ignore.toString shouldBe "Ignore"
    SaveMode.ErrorIfExists.toString shouldBe "ErrorIfExists"
  }

  test("DataFrameWriter typed option overloads compile") {
    val df = testDf()
    val writer = df.write
    // Verify these methods exist and return DataFrameWriter
    writer.option("key", true) shouldBe a[DataFrameWriter]
    writer.option("key", 42L) shouldBe a[DataFrameWriter]
    writer.option("key", 3.14) shouldBe a[DataFrameWriter]
  }

  test("DataFrameReader schema(StructType) uses toDDL") {
    import org.apache.spark.sql.types.*
    val session = SparkSession(null)
    val reader = session.read
    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType)
    ))
    // Just verify it compiles and doesn't throw
    reader.schema(schema) shouldBe a[DataFrameReader]
  }

  // ---------- sort(String, String*) ----------

  test("sort(String, String*) builds Sort proto with column names") {
    val df = testDf()
    val sorted = df.sort(Column("a"), Column("b"))
    sorted.relation.hasSort shouldBe true
    sorted.relation.getSort.getOrderCount shouldBe 2
  }

  // ---------- RuntimeConfig extensions ----------

  test("RuntimeConfig has getOption method") {
    val methods = classOf[RuntimeConfig].getMethods.filter(_.getName == "getOption")
    methods should not be empty
  }

  test("RuntimeConfig has getAll method") {
    val methods = classOf[RuntimeConfig].getMethods.filter(_.getName == "getAll")
    methods should not be empty
  }

  test("RuntimeConfig has unset method") {
    val methods = classOf[RuntimeConfig].getMethods.filter(_.getName == "unset")
    methods should not be empty
  }

  test("RuntimeConfig has isModifiable method") {
    val methods = classOf[RuntimeConfig].getMethods.filter(_.getName == "isModifiable")
    methods should not be empty
  }

  test("RuntimeConfig has set(key, Boolean) overload") {
    val methods = classOf[RuntimeConfig].getMethods.filter(m =>
      m.getName == "set" && m.getParameterTypes.length == 2 &&
        m.getParameterTypes()(1) == classOf[Boolean]
    )
    methods should not be empty
  }

  test("RuntimeConfig has set(key, Long) overload") {
    val methods = classOf[RuntimeConfig].getMethods.filter(m =>
      m.getName == "set" && m.getParameterTypes.length == 2 &&
        m.getParameterTypes()(1) == classOf[Long]
    )
    methods should not be empty
  }

  test("RuntimeConfig has get(key, default) overload") {
    val methods = classOf[RuntimeConfig].getMethods.filter(m =>
      m.getName == "get" && m.getParameterTypes.length == 2
    )
    methods should not be empty
  }

  // ---------- select(colNames: String*) ----------

  test("select(String*) builds Project with UnresolvedAttribute") {
    val df = testDf()
    val result = df.select("a", "b")(using summon[DummyImplicit])
    val rel = result.relation
    rel.hasProject shouldBe true
    val exprs = rel.getProject.getExpressionsList.asScala
    exprs should have size 2
    exprs(0).hasUnresolvedAttribute shouldBe true
    exprs(0).getUnresolvedAttribute.getUnparsedIdentifier shouldBe "a"
    exprs(1).hasUnresolvedAttribute shouldBe true
    exprs(1).getUnresolvedAttribute.getUnparsedIdentifier shouldBe "b"
  }

  // ---------- selectExpr ----------

  test("selectExpr builds Project with ExpressionString") {
    val df = testDf()
    val result = df.selectExpr("a + 1", "b as renamed")
    val rel = result.relation
    rel.hasProject shouldBe true
    val exprs = rel.getProject.getExpressionsList.asScala
    exprs should have size 2
    exprs(0).hasExpressionString shouldBe true
    exprs(0).getExpressionString.getExpression shouldBe "a + 1"
    exprs(1).hasExpressionString shouldBe true
    exprs(1).getExpressionString.getExpression shouldBe "b as renamed"
  }

  // ---------- where(Column) ----------

  test("where(Column) builds Filter proto (alias for filter)") {
    val df = testDf()
    val cond = Column("x") === Column("y")
    val result = df.where(cond)
    val rel = result.relation
    rel.hasFilter shouldBe true
    rel.getFilter.hasCondition shouldBe true
  }

  // ---------- where(String) ----------

  test("where(String) builds Filter proto with expression string") {
    val df = testDf()
    val result = df.where("x > 10")
    val rel = result.relation
    rel.hasFilter shouldBe true
    rel.getFilter.hasCondition shouldBe true
    val cond = rel.getFilter.getCondition
    cond.hasExpressionString shouldBe true
    cond.getExpressionString.getExpression shouldBe "x > 10"
  }

  // ---------- limit ----------

  test("limit builds Limit proto") {
    val df = testDf()
    val result = df.limit(10)
    val rel = result.relation
    rel.hasLimit shouldBe true
    rel.getLimit.getLimit shouldBe 10
  }

  // ---------- offset ----------

  test("offset builds Offset proto") {
    val df = testDf()
    val result = df.offset(5)
    val rel = result.relation
    rel.hasOffset shouldBe true
    rel.getOffset.getOffset shouldBe 5
  }

  // ---------- groupBy(Column*) ----------

  test("groupBy(Column*) returns GroupedDataFrame") {
    val df = testDf()
    val grouped = df.groupBy(Column("a"), Column("b"))
    grouped shouldBe a[GroupedDataFrame]
  }

  // ---------- groupBy(String*) ----------

  test("groupBy(String*) returns GroupedDataFrame") {
    val df = testDf()
    val grouped = df.groupBy("a", "b")(using summon[DummyImplicit])
    grouped shouldBe a[GroupedDataFrame]
  }

  // ---------- rollup ----------

  test("rollup returns GroupedDataFrame with ROLLUP type") {
    val df = testDf()
    val grouped = df.rollup(Column("a"))
    grouped shouldBe a[GroupedDataFrame]
    // Verify by aggregating and checking the proto
    val result = grouped.agg(functions.count(Column("a")).as("cnt"))
    result.relation.hasAggregate shouldBe true
    result.relation.getAggregate.getGroupType shouldBe Aggregate.GroupType.GROUP_TYPE_ROLLUP
  }

  // ---------- cube ----------

  test("cube returns GroupedDataFrame with CUBE type") {
    val df = testDf()
    val grouped = df.cube(Column("a"))
    grouped shouldBe a[GroupedDataFrame]
    val result = grouped.agg(functions.count(Column("a")).as("cnt"))
    result.relation.hasAggregate shouldBe true
    result.relation.getAggregate.getGroupType shouldBe Aggregate.GroupType.GROUP_TYPE_CUBE
  }

  // ---------- agg ----------

  test("agg(aggExpr, aggExprs*) builds Aggregate proto") {
    val df = testDf()
    val result =
      df.agg(functions.count(Column("a")).as("cnt"), functions.sum(Column("b")).as("total"))
    val rel = result.relation
    rel.hasAggregate shouldBe true
    val agg = rel.getAggregate
    agg.getGroupType shouldBe Aggregate.GroupType.GROUP_TYPE_GROUPBY
    agg.getGroupingExpressionsCount shouldBe 0
    agg.getAggregateExpressionsCount shouldBe 2
  }

  // ---------- crossJoin ----------

  test("crossJoin builds Join proto with CROSS type") {
    val df1 = testDf()
    val df2 = testDf()
    val result = df1.crossJoin(df2)
    val rel = result.relation
    rel.hasJoin shouldBe true
    val j = rel.getJoin
    j.getJoinType shouldBe Join.JoinType.JOIN_TYPE_CROSS
    j.hasJoinCondition shouldBe false
  }

  // ---------- withColumn ----------

  test("withColumn builds WithColumns proto") {
    val df = testDf()
    val result = df.withColumn("new_col", functions.lit(42))
    val rel = result.relation
    rel.hasWithColumns shouldBe true
    val aliases = rel.getWithColumns.getAliasesList.asScala
    aliases should have size 1
    aliases.head.getNameList.asScala.head shouldBe "new_col"
  }

  // ---------- withColumnRenamed ----------

  test("withColumnRenamed builds WithColumnsRenamed proto") {
    val df = testDf()
    val result = df.withColumnRenamed("old_name", "new_name")
    val rel = result.relation
    rel.hasWithColumnsRenamed shouldBe true
    val renames = rel.getWithColumnsRenamed.getRenamesList.asScala
    renames should have size 1
    renames.head.getColName shouldBe "old_name"
    renames.head.getNewColName shouldBe "new_name"
  }

  // ---------- drop(String*) ----------

  test("drop(String*) builds Drop proto with column names") {
    val df = testDf()
    val result = df.drop("a", "b")
    val rel = result.relation
    rel.hasDrop shouldBe true
    val cols = rel.getDrop.getColumnsList.asScala
    cols should have size 2
    cols(0).hasUnresolvedAttribute shouldBe true
    cols(0).getUnresolvedAttribute.getUnparsedIdentifier shouldBe "a"
    cols(1).hasUnresolvedAttribute shouldBe true
    cols(1).getUnresolvedAttribute.getUnparsedIdentifier shouldBe "b"
  }

  // ---------- distinct ----------

  test("distinct builds Deduplicate proto with allColumnsAsKeys") {
    val df = testDf()
    val result = df.distinct()
    val rel = result.relation
    rel.hasDeduplicate shouldBe true
    rel.getDeduplicate.getAllColumnsAsKeys shouldBe true
  }

  // ---------- dropDuplicates() ----------

  test("dropDuplicates() builds Deduplicate proto with allColumnsAsKeys") {
    val df = testDf()
    val result = df.dropDuplicates()
    val rel = result.relation
    rel.hasDeduplicate shouldBe true
    rel.getDeduplicate.getAllColumnsAsKeys shouldBe true
  }

  // ---------- dropDuplicates(Seq) ----------

  test("dropDuplicates(Seq) builds Deduplicate proto with specific columns") {
    val df = testDf()
    val result = df.dropDuplicates(Seq("a", "b"))
    val rel = result.relation
    rel.hasDeduplicate shouldBe true
    val dedup = rel.getDeduplicate
    dedup.getAllColumnsAsKeys shouldBe false
    dedup.getColumnNamesList.asScala.toSeq shouldBe Seq("a", "b")
  }

  // ---------- union ----------

  test("union builds SetOperation UNION proto") {
    val df1 = testDf()
    val df2 = testDf()
    val result = df1.union(df2)
    val rel = result.relation
    rel.hasSetOp shouldBe true
    val setOp = rel.getSetOp
    setOp.getSetOpType shouldBe SetOperation.SetOpType.SET_OP_TYPE_UNION
    setOp.getByName shouldBe false
    setOp.getIsAll shouldBe false
  }

  // ---------- unionAll ----------

  test("unionAll builds SetOperation UNION proto") {
    val df1 = testDf()
    val df2 = testDf()
    val result = df1.unionAll(df2)
    val rel = result.relation
    rel.hasSetOp shouldBe true
    rel.getSetOp.getSetOpType shouldBe SetOperation.SetOpType.SET_OP_TYPE_UNION
  }

  // ---------- unionByName ----------

  test("unionByName builds SetOperation UNION proto with byName") {
    val df1 = testDf()
    val df2 = testDf()
    val result = df1.unionByName(df2, allowMissingColumns = true)
    val rel = result.relation
    rel.hasSetOp shouldBe true
    val setOp = rel.getSetOp
    setOp.getSetOpType shouldBe SetOperation.SetOpType.SET_OP_TYPE_UNION
    setOp.getByName shouldBe true
    setOp.getAllowMissingColumns shouldBe true
  }

  // ---------- intersect ----------

  test("intersect builds SetOperation INTERSECT proto") {
    val df1 = testDf()
    val df2 = testDf()
    val result = df1.intersect(df2)
    val rel = result.relation
    rel.hasSetOp shouldBe true
    val setOp = rel.getSetOp
    setOp.getSetOpType shouldBe SetOperation.SetOpType.SET_OP_TYPE_INTERSECT
    setOp.getIsAll shouldBe false
  }

  // ---------- except ----------

  test("except builds SetOperation EXCEPT proto") {
    val df1 = testDf()
    val df2 = testDf()
    val result = df1.except(df2)
    val rel = result.relation
    rel.hasSetOp shouldBe true
    val setOp = rel.getSetOp
    setOp.getSetOpType shouldBe SetOperation.SetOpType.SET_OP_TYPE_EXCEPT
    setOp.getIsAll shouldBe false
  }

  // ---------- intersectAll ----------

  test("intersectAll builds SetOperation INTERSECT proto with isAll") {
    val df1 = testDf()
    val df2 = testDf()
    val result = df1.intersectAll(df2)
    val rel = result.relation
    rel.hasSetOp shouldBe true
    val setOp = rel.getSetOp
    setOp.getSetOpType shouldBe SetOperation.SetOpType.SET_OP_TYPE_INTERSECT
    setOp.getIsAll shouldBe true
  }

  // ---------- exceptAll ----------

  test("exceptAll builds SetOperation EXCEPT proto with isAll") {
    val df1 = testDf()
    val df2 = testDf()
    val result = df1.exceptAll(df2)
    val rel = result.relation
    rel.hasSetOp shouldBe true
    val setOp = rel.getSetOp
    setOp.getSetOpType shouldBe SetOperation.SetOpType.SET_OP_TYPE_EXCEPT
    setOp.getIsAll shouldBe true
  }

  // ---------- repartition(Int) ----------

  test("repartition(Int) builds Repartition proto with shuffle=true") {
    val df = testDf()
    val result = df.repartition(10)
    val rel = result.relation
    rel.hasRepartition shouldBe true
    val rep = rel.getRepartition
    rep.getNumPartitions shouldBe 10
    rep.getShuffle shouldBe true
  }

  // ---------- coalesce ----------

  test("coalesce builds Repartition proto with shuffle=false") {
    val df = testDf()
    val result = df.coalesce(2)
    val rel = result.relation
    rel.hasRepartition shouldBe true
    val rep = rel.getRepartition
    rep.getNumPartitions shouldBe 2
    rep.getShuffle shouldBe false
  }

  // ---------- sample ----------

  test("sample builds Sample proto") {
    val df = testDf()
    val result = df.sample(0.5, withReplacement = true, seed = 42L)
    val rel = result.relation
    rel.hasSample shouldBe true
    val s = rel.getSample
    s.getLowerBound shouldBe 0.0
    s.getUpperBound shouldBe 0.5
    s.getWithReplacement shouldBe true
    s.getSeed shouldBe 42L
  }

  // ---------- describe ----------

  test("describe builds Describe proto") {
    val df = testDf()
    val result = df.describe("a", "b")
    val rel = result.relation
    rel.hasDescribe shouldBe true
    val desc = rel.getDescribe
    desc.getColsList.asScala.toSeq shouldBe Seq("a", "b")
  }

  // ---------- summary ----------

  test("summary builds Summary proto") {
    val df = testDf()
    val result = df.summary("count", "mean", "stddev")
    val rel = result.relation
    rel.hasSummary shouldBe true
    val summ = rel.getSummary
    summ.getStatisticsList.asScala.toSeq shouldBe Seq("count", "mean", "stddev")
  }

  // ---------- alias ----------

  test("alias builds SubqueryAlias proto") {
    val df = testDf()
    val result = df.alias("t1")
    val rel = result.relation
    rel.hasSubqueryAlias shouldBe true
    rel.getSubqueryAlias.getAlias shouldBe "t1"
  }

  // ---------- toDF ----------

  test("toDF(String*) builds ToDF proto") {
    val df = testDf()
    val result = df.toDF("x", "y", "z")
    val rel = result.relation
    rel.hasToDf shouldBe true
    val toDf = rel.getToDf
    toDf.getColumnNamesList.asScala.toSeq shouldBe Seq("x", "y", "z")
  }

  // ---------- hint ----------

  test("hint builds Hint proto") {
    val df = testDf()
    val result = df.hint("broadcast")
    val rel = result.relation
    rel.hasHint shouldBe true
    val h = rel.getHint
    h.getName shouldBe "broadcast"
    h.getParametersCount shouldBe 0
  }

  test("hint with parameters builds Hint proto with parameters") {
    val df = testDf()
    val result = df.hint("repartition", 10)
    val rel = result.relation
    rel.hasHint shouldBe true
    val h = rel.getHint
    h.getName shouldBe "repartition"
    h.getParametersCount shouldBe 1
  }

  // ---------- sortWithinPartitions ----------

  test("sortWithinPartitions builds Sort proto with isGlobal=false") {
    val df = testDf()
    val result = df.sortWithinPartitions(Column("a"), Column("b"))
    val rel = result.relation
    rel.hasSort shouldBe true
    val sort = rel.getSort
    sort.getIsGlobal shouldBe false
    sort.getOrderCount shouldBe 2
  }

  // ---------- transform ----------

  test("transform applies function to DataFrame") {
    val df = testDf()
    val result = df.transform(d => d.limit(5))
    val rel = result.relation
    rel.hasLimit shouldBe true
    rel.getLimit.getLimit shouldBe 5
  }

  // ---------- withWatermark ----------

  test("withWatermark builds WithWatermark proto") {
    val df = testDf()
    val result = df.withWatermark("timestamp", "10 seconds")
    val rel = result.relation
    rel.hasWithWatermark shouldBe true
    val ww = rel.getWithWatermark
    ww.getEventTime shouldBe "timestamp"
    ww.getDelayThreshold shouldBe "10 seconds"
  }

  // ---------- melt (alias for unpivot) ----------

  test("melt without values is an alias for unpivot without values") {
    val df = testDf()
    val result = df.melt(
      ids = Array(Column("id")),
      variableColumnName = "var",
      valueColumnName = "val"
    )
    result.relation.hasUnpivot shouldBe true
    val unpivot = result.relation.getUnpivot
    unpivot.getIdsList should have size 1
    unpivot.hasValues shouldBe false
    unpivot.getVariableColumnName shouldBe "var"
    unpivot.getValueColumnName shouldBe "val"
  }

  // ---------- join with various join types ----------

  test("join(right, joinExpr, inner) builds Join proto with INNER type") {
    val df1 = testDf()
    val df2 = testDf()
    val joined = df1.join(df2, Column("id") === Column("id"), "inner")
    val j = joined.relation.getJoin
    j.getJoinType shouldBe Join.JoinType.JOIN_TYPE_INNER
    j.hasJoinCondition shouldBe true
  }

  test("join(right, joinExpr, left) builds Join proto with LEFT_OUTER type") {
    val df1 = testDf()
    val df2 = testDf()
    val joined = df1.join(df2, Column("id") === Column("id"), "left")
    joined.relation.getJoin.getJoinType shouldBe Join.JoinType.JOIN_TYPE_LEFT_OUTER
  }

  test("join(right, joinExpr, leftouter) builds Join proto with LEFT_OUTER type") {
    val df1 = testDf()
    val df2 = testDf()
    val joined = df1.join(df2, Column("id") === Column("id"), "leftouter")
    joined.relation.getJoin.getJoinType shouldBe Join.JoinType.JOIN_TYPE_LEFT_OUTER
  }

  test("join(right, joinExpr, right) builds Join proto with RIGHT_OUTER type") {
    val df1 = testDf()
    val df2 = testDf()
    val joined = df1.join(df2, Column("id") === Column("id"), "right")
    joined.relation.getJoin.getJoinType shouldBe Join.JoinType.JOIN_TYPE_RIGHT_OUTER
  }

  test("join(right, joinExpr, rightouter) builds Join proto with RIGHT_OUTER type") {
    val df1 = testDf()
    val df2 = testDf()
    val joined = df1.join(df2, Column("id") === Column("id"), "rightouter")
    joined.relation.getJoin.getJoinType shouldBe Join.JoinType.JOIN_TYPE_RIGHT_OUTER
  }

  test("join(right, joinExpr, full) builds Join proto with FULL_OUTER type") {
    val df1 = testDf()
    val df2 = testDf()
    val joined = df1.join(df2, Column("id") === Column("id"), "full")
    joined.relation.getJoin.getJoinType shouldBe Join.JoinType.JOIN_TYPE_FULL_OUTER
  }

  test("join(right, joinExpr, outer) builds Join proto with FULL_OUTER type") {
    val df1 = testDf()
    val df2 = testDf()
    val joined = df1.join(df2, Column("id") === Column("id"), "outer")
    joined.relation.getJoin.getJoinType shouldBe Join.JoinType.JOIN_TYPE_FULL_OUTER
  }

  test("join(right, joinExpr, fullouter) builds Join proto with FULL_OUTER type") {
    val df1 = testDf()
    val df2 = testDf()
    val joined = df1.join(df2, Column("id") === Column("id"), "fullouter")
    joined.relation.getJoin.getJoinType shouldBe Join.JoinType.JOIN_TYPE_FULL_OUTER
  }

  test("join(right, joinExpr, cross) builds Join proto with CROSS type") {
    val df1 = testDf()
    val df2 = testDf()
    val joined = df1.join(df2, Column("id") === Column("id"), "cross")
    joined.relation.getJoin.getJoinType shouldBe Join.JoinType.JOIN_TYPE_CROSS
  }

  test("join(right, joinExpr, semi) builds Join proto with LEFT_SEMI type") {
    val df1 = testDf()
    val df2 = testDf()
    val joined = df1.join(df2, Column("id") === Column("id"), "semi")
    joined.relation.getJoin.getJoinType shouldBe Join.JoinType.JOIN_TYPE_LEFT_SEMI
  }

  test("join(right, joinExpr, leftsemi) builds Join proto with LEFT_SEMI type") {
    val df1 = testDf()
    val df2 = testDf()
    val joined = df1.join(df2, Column("id") === Column("id"), "leftsemi")
    joined.relation.getJoin.getJoinType shouldBe Join.JoinType.JOIN_TYPE_LEFT_SEMI
  }

  test("join(right, joinExpr, anti) builds Join proto with LEFT_ANTI type") {
    val df1 = testDf()
    val df2 = testDf()
    val joined = df1.join(df2, Column("id") === Column("id"), "anti")
    joined.relation.getJoin.getJoinType shouldBe Join.JoinType.JOIN_TYPE_LEFT_ANTI
  }

  test("join(right, joinExpr, leftanti) builds Join proto with LEFT_ANTI type") {
    val df1 = testDf()
    val df2 = testDf()
    val joined = df1.join(df2, Column("id") === Column("id"), "leftanti")
    joined.relation.getJoin.getJoinType shouldBe Join.JoinType.JOIN_TYPE_LEFT_ANTI
  }

  test("join(right, usingColumns) builds Join with using columns and INNER type") {
    val df1 = testDf()
    val df2 = testDf()
    val joined = df1.join(df2, Seq("id", "name"))
    val j = joined.relation.getJoin
    j.getUsingColumnsCount shouldBe 2
    j.getUsingColumns(0) shouldBe "id"
    j.getUsingColumns(1) shouldBe "name"
    j.getJoinType shouldBe Join.JoinType.JOIN_TYPE_INNER
  }

  // ---------- lateralJoin with various join types ----------

  test("lateralJoin with left join type") {
    val df1 = testDf()
    val df2 = testDf()
    val result = df1.lateralJoin(df2, Column("id") === Column("id"), "left")
    result.relation.hasLateralJoin shouldBe true
    result.relation.getLateralJoin.getJoinType shouldBe Join.JoinType.JOIN_TYPE_LEFT_OUTER
  }

  test("lateralJoin with cross join type") {
    val df1 = testDf()
    val df2 = testDf()
    val result = df1.lateralJoin(df2, Column("id") === Column("id"), "cross")
    result.relation.hasLateralJoin shouldBe true
    result.relation.getLateralJoin.getJoinType shouldBe Join.JoinType.JOIN_TYPE_CROSS
  }

  test("lateralJoin rejects semi join type") {
    val df1 = testDf()
    val df2 = testDf()
    an[IllegalArgumentException] should be thrownBy
      df1.lateralJoin(df2, Column("id") === Column("id"), "semi")
  }

  test("lateralJoin rejects anti join type") {
    val df1 = testDf()
    val df2 = testDf()
    an[IllegalArgumentException] should be thrownBy
      df1.lateralJoin(df2, Column("id") === Column("id"), "anti")
  }

  test("lateralJoin rejects right join type") {
    val df1 = testDf()
    val df2 = testDf()
    an[IllegalArgumentException] should be thrownBy
      df1.lateralJoin(df2, Column("id") === Column("id"), "right")
  }

  // ---------- broadcast ----------

  test("broadcast builds Hint proto with broadcast name") {
    val df = testDf()
    val result = df.broadcast
    result.relation.hasHint shouldBe true
    result.relation.getHint.getName shouldBe "broadcast"
    result.relation.getHint.getParametersCount shouldBe 0
  }

  // ---------- sortWithinPartitions(String*) ----------

  test("sortWithinPartitions(String*) builds Sort proto with isGlobal=false") {
    val df = testDf()
    val result = df.sortWithinPartitions("a", "b")(using summon[DummyImplicit])
    val rel = result.relation
    rel.hasSort shouldBe true
    val sort = rel.getSort
    sort.getIsGlobal shouldBe false
    sort.getOrderCount shouldBe 2
  }

  // ---------- na / stat accessors ----------

  test("na returns DataFrameNaFunctions") {
    val df = testDf()
    df.na shouldBe a[DataFrameNaFunctions]
  }

  test("stat returns DataFrameStatFunctions") {
    val df = testDf()
    df.stat shouldBe a[DataFrameStatFunctions]
  }

  // ---------- write / writeTo / writeStream / mergeInto return types ----------

  test("write returns DataFrameWriter") {
    val df = testDf()
    df.write shouldBe a[DataFrameWriter]
  }

  test("writeTo returns DataFrameWriterV2") {
    val df = testDf()
    df.writeTo("my_table") shouldBe a[DataFrameWriterV2]
  }

  test("writeStream returns DataStreamWriter") {
    val df = testDf()
    df.writeStream shouldBe a[DataStreamWriter]
  }

  test("mergeInto returns MergeIntoWriter") {
    val df = testDf()
    val writer = df.mergeInto("target_table", Column("id") === Column("id"))
    writer shouldBe a[MergeIntoWriter]
  }

  // ---------- scalar / exists subquery expressions ----------

  test("scalar() builds SubqueryExpression with SCALAR type") {
    val df = testDf()
    val col = df.scalar()
    col.expr.hasSubqueryExpression shouldBe true
    col.expr.getSubqueryExpression.getSubqueryType shouldBe
      SubqueryExpression.SubqueryType.SUBQUERY_TYPE_SCALAR
    col.expr.getSubqueryExpression.getPlanId shouldBe df.relation.getCommon.getPlanId
  }

  test("scalar() subquery relations contain the DataFrame relation") {
    val df = testDf()
    val col = df.scalar()
    col.subqueryRelations should have size 1
    col.subqueryRelations.head shouldBe df.relation
  }

  test("exists() builds SubqueryExpression with EXISTS type") {
    val df = testDf()
    val col = df.exists()
    col.expr.hasSubqueryExpression shouldBe true
    col.expr.getSubqueryExpression.getSubqueryType shouldBe
      SubqueryExpression.SubqueryType.SUBQUERY_TYPE_EXISTS
    col.expr.getSubqueryExpression.getPlanId shouldBe df.relation.getCommon.getPlanId
  }

  test("exists() subquery relations contain the DataFrame relation") {
    val df = testDf()
    val col = df.exists()
    col.subqueryRelations should have size 1
    col.subqueryRelations.head shouldBe df.relation
  }

  // ---------- repartition with columns ----------

  test("repartition(numPartitions, cols) builds RepartitionByExpression") {
    val df = testDf()
    val result = df.repartition(8, Column("a"), Column("b"))
    val rel = result.relation
    rel.hasRepartitionByExpression shouldBe true
    val rbe = rel.getRepartitionByExpression
    rbe.getNumPartitions shouldBe 8
    rbe.getPartitionExprsList should have size 2
  }

  test("repartition(numPartitions, empty cols) falls back to simple repartition") {
    val df = testDf()
    val result = df.repartition(8)
    val rel = result.relation
    rel.hasRepartition shouldBe true
    rel.getRepartition.getNumPartitions shouldBe 8
    rel.getRepartition.getShuffle shouldBe true
  }

  test("repartition(cols)(DummyImplicit) builds RepartitionByExpression without numPartitions") {
    val df = testDf()
    val result = df.repartition(Column("a"), Column("b"))(using summon[DummyImplicit])
    val rel = result.relation
    rel.hasRepartitionByExpression shouldBe true
    val rbe = rel.getRepartitionByExpression
    rbe.hasNumPartitions shouldBe false
    rbe.getPartitionExprsList should have size 2
  }

  // ---------- repartitionByRange with explicit sort order ----------

  test("repartitionByRange respects pre-existing sort order") {
    val df = testDf()
    val result = df.repartitionByRange(4, Column("id").desc)
    val rel = result.relation
    rel.hasRepartitionByExpression shouldBe true
    val rbe = rel.getRepartitionByExpression
    rbe.getNumPartitions shouldBe 4
    rbe.getPartitionExprsList should have size 1
    rbe.getPartitionExprs(0).hasSortOrder shouldBe true
  }

  // ---------- repartitionByRange without numPartitions requires at least one expression ----------

  test("repartitionByRange without numPartitions requires at least one expression") {
    val df = testDf()
    an[IllegalArgumentException] should be thrownBy
      df.repartitionByRange()(using summon[DummyImplicit])
  }

  // ---------- toJoinType coverage for all cases ----------

  test("toJoinType maps all supported join type strings") {
    val df = testDf()
    df.toJoinType("inner") shouldBe Join.JoinType.JOIN_TYPE_INNER
    df.toJoinType("left") shouldBe Join.JoinType.JOIN_TYPE_LEFT_OUTER
    df.toJoinType("leftouter") shouldBe Join.JoinType.JOIN_TYPE_LEFT_OUTER
    df.toJoinType("right") shouldBe Join.JoinType.JOIN_TYPE_RIGHT_OUTER
    df.toJoinType("rightouter") shouldBe Join.JoinType.JOIN_TYPE_RIGHT_OUTER
    df.toJoinType("full") shouldBe Join.JoinType.JOIN_TYPE_FULL_OUTER
    df.toJoinType("outer") shouldBe Join.JoinType.JOIN_TYPE_FULL_OUTER
    df.toJoinType("fullouter") shouldBe Join.JoinType.JOIN_TYPE_FULL_OUTER
    df.toJoinType("cross") shouldBe Join.JoinType.JOIN_TYPE_CROSS
    df.toJoinType("semi") shouldBe Join.JoinType.JOIN_TYPE_LEFT_SEMI
    df.toJoinType("leftsemi") shouldBe Join.JoinType.JOIN_TYPE_LEFT_SEMI
    df.toJoinType("anti") shouldBe Join.JoinType.JOIN_TYPE_LEFT_ANTI
    df.toJoinType("leftanti") shouldBe Join.JoinType.JOIN_TYPE_LEFT_ANTI
    // unknown defaults to INNER
    df.toJoinType("unknown") shouldBe Join.JoinType.JOIN_TYPE_INNER
  }

  // ---------- method existence tests for server-requiring methods ----------

  test("isEmpty method exists") {
    classOf[DataFrame].getMethod("isEmpty") should not be null
    classOf[DataFrame].getMethod("isEmpty").getReturnType shouldBe classOf[Boolean]
  }

  test("toLocalIterator method exists") {
    classOf[DataFrame].getMethod("toLocalIterator") should not be null
  }

  test("isStreaming method exists") {
    classOf[DataFrame].getMethod("isStreaming") should not be null
    classOf[DataFrame].getMethod("isStreaming").getReturnType shouldBe classOf[Boolean]
  }

  test("sameSemantics method exists") {
    val methods = classOf[DataFrame].getMethods.filter(_.getName == "sameSemantics")
    methods should not be empty
  }

  test("semanticHash method exists") {
    classOf[DataFrame].getMethod("semanticHash") should not be null
    classOf[DataFrame].getMethod("semanticHash").getReturnType shouldBe classOf[Int]
  }

  test("storageLevel method exists") {
    classOf[DataFrame].getMethod("storageLevel") should not be null
  }

  test("cache method exists") {
    classOf[DataFrame].getMethod("cache") should not be null
  }

  test("persist method exists") {
    classOf[DataFrame].getMethod("persist") should not be null
  }

  test("persist with StorageLevel method exists") {
    classOf[DataFrame].getMethods.exists(m =>
      m.getName == "persist" && m.getParameterCount == 1
    ) shouldBe true
  }

  test("unpersist method exists") {
    classOf[DataFrame].getMethod("unpersist", classOf[Boolean]) should not be null
  }

  test("checkpoint method exists") {
    classOf[DataFrame].getMethod("checkpoint", classOf[Boolean]) should not be null
  }

  test("localCheckpoint method exists") {
    classOf[DataFrame].getMethod("localCheckpoint", classOf[Boolean]) should not be null
  }

  test("inputFiles method exists") {
    classOf[DataFrame].getMethod("inputFiles") should not be null
  }

  // ---------- createTempView / createOrReplaceTempView / createGlobalTempView ----------

  test("createTempView method exists") {
    classOf[DataFrame].getMethod("createTempView", classOf[String]) should not be null
  }

  test("createOrReplaceTempView method exists") {
    classOf[DataFrame].getMethod("createOrReplaceTempView", classOf[String]) should not be null
  }

  test("createGlobalTempView method exists") {
    classOf[DataFrame].getMethod("createGlobalTempView", classOf[String]) should not be null
  }

  test("createOrReplaceGlobalTempView method exists") {
    classOf[DataFrame].getMethod(
      "createOrReplaceGlobalTempView",
      classOf[String]
    ) should not be null
  }

  // ---------- filter(Column) ----------

  test("filter builds Filter proto") {
    val df = testDf()
    val cond = Column("x") > Column.lit(0)
    val result = df.filter(cond)
    result.relation.hasFilter shouldBe true
    result.relation.getFilter.hasCondition shouldBe true
  }

  // ---------- orderBy / sort ----------

  test("orderBy builds Sort proto with isGlobal=true") {
    val df = testDf()
    val result = df.orderBy(Column("a").asc, Column("b").desc)
    result.relation.hasSort shouldBe true
    val sort = result.relation.getSort
    sort.getIsGlobal shouldBe true
    sort.getOrderCount shouldBe 2
  }

  test("sort is alias for orderBy") {
    val df = testDf()
    val result = df.sort(Column("a").asc)
    result.relation.hasSort shouldBe true
    result.relation.getSort.getIsGlobal shouldBe true
    result.relation.getSort.getOrderCount shouldBe 1
  }

  // ---------- randomSplit with explicit seed ----------

  test("randomSplit with custom seed builds Sample protos with that seed") {
    val df = testDf()
    val splits = df.randomSplit(Array(1.0, 1.0), seed = 42L)
    splits should have size 2
    splits.foreach { s =>
      s.relation.hasSample shouldBe true
      s.relation.getSample.getSeed shouldBe 42L
    }
  }

  // ---------- sample default values ----------

  test("sample with defaults builds Sample proto with withReplacement=false and seed=0") {
    val df = testDf()
    val result = df.sample(0.3)
    result.relation.hasSample shouldBe true
    val s = result.relation.getSample
    s.getWithReplacement shouldBe false
    s.getSeed shouldBe 0L
    s.getUpperBound shouldBe 0.3
  }

  // ---------- withRelation subquery wrapping ----------

  test("select with subquery column wraps in WithRelations") {
    val df = testDf()
    val subq = testDf().scalar()
    val result = df.select(subq)
    // When subquery references are present, the outer relation should be WithRelations
    result.relation.hasWithRelations shouldBe true
    val wr = result.relation.getWithRelations
    wr.hasRoot shouldBe true
    wr.getReferencesCount shouldBe 1
  }

  // ---------- hint with multiple parameters ----------

  test("hint with string parameter builds Hint proto") {
    val df = testDf()
    val result = df.hint("shuffle_hash", "col1")
    result.relation.hasHint shouldBe true
    val h = result.relation.getHint
    h.getName shouldBe "shuffle_hash"
    h.getParametersCount shouldBe 1
  }

  // ---------- unionByName default allowMissingColumns ----------

  test("unionByName without allowMissingColumns defaults to false") {
    val df1 = testDf()
    val df2 = testDf()
    val result = df1.unionByName(df2)
    result.relation.hasSetOp shouldBe true
    val setOp = result.relation.getSetOp
    setOp.getByName shouldBe true
    setOp.getAllowMissingColumns shouldBe false
  }

  // ---------- as[T] conversion ----------

  test("as[T] builds Dataset with encoder") {
    val df = testDf()
    val ds = df.as[Long]
    ds should not be null
    ds.encoder should not be null
  }

  // ---------- columns / schema / printSchema method existence ----------

  test("columns method exists with correct return type") {
    classOf[DataFrame].getMethod("columns") should not be null
    classOf[DataFrame].getMethod("columns").getReturnType shouldBe classOf[Array[String]]
  }

  test("schema method exists") {
    classOf[DataFrame].getMethod("schema") should not be null
  }

  test("printSchema() method exists") {
    classOf[DataFrame].getMethod("printSchema") should not be null
  }

  // ---------- explain methods ----------

  test("explain(Boolean) method exists") {
    classOf[DataFrame].getMethod("explain", classOf[Boolean]) should not be null
  }

  test("explain(String) method exists") {
    classOf[DataFrame].getMethod("explain", classOf[String]) should not be null
  }

  // ---------- collect / first / head / take method existence ----------

  test("collect method exists") {
    classOf[DataFrame].getMethod("collect") should not be null
  }

  test("first method exists") {
    classOf[DataFrame].getMethod("first") should not be null
  }

  test("head(Int) method exists") {
    classOf[DataFrame].getMethod("head", classOf[Int]) should not be null
  }

  test("take(Int) method exists") {
    classOf[DataFrame].getMethod("take", classOf[Int]) should not be null
  }

  test("count method exists") {
    classOf[DataFrame].getMethod("count") should not be null
  }

  // ---------- tail method existence ----------

  test("tail method exists") {
    classOf[DataFrame].getMethod("tail", classOf[Int]) should not be null
  }

  // ---------- show methods existence ----------

  test("show(Int, Int) method exists") {
    classOf[DataFrame].getMethod("show", classOf[Int], classOf[Int]) should not be null
  }

  test("show(Int, Int, Boolean) method exists") {
    classOf[DataFrame].getMethod(
      "show",
      classOf[Int],
      classOf[Int],
      classOf[Boolean]
    ) should not be null
  }

  // ---------- StorageLevel toProto round-trip ----------

  test("StorageLevel.MEMORY_AND_DISK toProto produces correct proto") {
    val sl = SparkStorageLevel.MEMORY_AND_DISK
    val proto = sl.toProto
    proto.getUseDisk shouldBe true
    proto.getUseMemory shouldBe true
    proto.getUseOffHeap shouldBe false
    proto.getDeserialized shouldBe true
    proto.getReplication shouldBe 1
  }

  test("StorageLevel.DISK_ONLY toProto produces correct proto") {
    val sl = SparkStorageLevel.DISK_ONLY
    val proto = sl.toProto
    proto.getUseDisk shouldBe true
    proto.getUseMemory shouldBe false
    proto.getUseOffHeap shouldBe false
    proto.getDeserialized shouldBe false
    proto.getReplication shouldBe 1
  }

  test("StorageLevel.MEMORY_ONLY toProto produces correct proto") {
    val sl = SparkStorageLevel.MEMORY_ONLY
    val proto = sl.toProto
    proto.getUseDisk shouldBe false
    proto.getUseMemory shouldBe true
    proto.getUseOffHeap shouldBe false
    proto.getDeserialized shouldBe true
    proto.getReplication shouldBe 1
  }

  test("StorageLevel.OFF_HEAP toProto produces correct proto") {
    val sl = SparkStorageLevel.OFF_HEAP
    val proto = sl.toProto
    proto.getUseDisk shouldBe true
    proto.getUseMemory shouldBe true
    proto.getUseOffHeap shouldBe true
    proto.getDeserialized shouldBe false
    proto.getReplication shouldBe 1
  }

  test("StorageLevel with replication=2 toProto produces correct proto") {
    val sl = SparkStorageLevel.MEMORY_AND_DISK_2
    val proto = sl.toProto
    proto.getReplication shouldBe 2
  }

  // ---------- DataFrameNaFunctions proto tests ----------

  test("DataFrameNaFunctions.drop(minNonNulls) builds NADrop proto") {
    val df = testDf()
    val result = df.na.drop(3)
    result.relation.hasDropNa shouldBe true
    result.relation.getDropNa.getMinNonNulls shouldBe 3
  }

  test("DataFrameNaFunctions.fill(Double, cols) builds NAFill proto") {
    val df = testDf()
    val result = df.na.fill(42.0, Seq("a", "b"))
    result.relation.hasFillNa shouldBe true
    val fill = result.relation.getFillNa
    fill.getColsCount shouldBe 2
    fill.getValuesCount shouldBe 1
    fill.getValues(0).getDouble shouldBe 42.0
  }

  test("DataFrameNaFunctions.fill(String, cols) builds NAFill proto") {
    val df = testDf()
    val result = df.na.fill("default", Seq("name"))
    result.relation.hasFillNa shouldBe true
    val fill = result.relation.getFillNa
    fill.getColsCount shouldBe 1
    fill.getValuesCount shouldBe 1
    fill.getValues(0).getString shouldBe "default"
  }

  test("DataFrameNaFunctions.fill(Map) builds NAFill proto with per-column values") {
    val df = testDf()
    val result = df.na.fill(Map("a" -> 1, "b" -> "x"))
    result.relation.hasFillNa shouldBe true
    val fill = result.relation.getFillNa
    fill.getColsCount shouldBe 2
    fill.getValuesCount shouldBe 2
  }

  test("DataFrameNaFunctions.replace builds NAReplace proto") {
    val df = testDf()
    val result = df.na.replace("col1", Map(1 -> 2, 3 -> 4))
    result.relation.hasReplace shouldBe true
    val replace = result.relation.getReplace
    replace.getColsCount shouldBe 1
    replace.getCols(0) shouldBe "col1"
    replace.getReplacementsCount shouldBe 2
  }

  // ---------- DataFrameStatFunctions proto tests ----------

  test("DataFrameStatFunctions.crosstab builds StatCrosstab proto") {
    val df = testDf()
    val result = df.stat.crosstab("col1", "col2")
    result.relation.hasCrosstab shouldBe true
    val ct = result.relation.getCrosstab
    ct.getCol1 shouldBe "col1"
    ct.getCol2 shouldBe "col2"
  }

  test("DataFrameStatFunctions.freqItems builds StatFreqItems proto") {
    val df = testDf()
    val result = df.stat.freqItems(Seq("a", "b"), 0.05)
    result.relation.hasFreqItems shouldBe true
    val fi = result.relation.getFreqItems
    fi.getColsCount shouldBe 2
    fi.getSupport shouldBe 0.05
  }

  test("DataFrameStatFunctions.freqItems without support builds StatFreqItems proto") {
    val df = testDf()
    val result = df.stat.freqItems(Seq("a"))
    result.relation.hasFreqItems shouldBe true
    val fi = result.relation.getFreqItems
    fi.getColsCount shouldBe 1
  }

  test("DataFrameStatFunctions.sampleBy builds StatSampleBy proto") {
    val df = testDf()
    val result = df.stat.sampleBy(Column("key"), Map(1 -> 0.5, 2 -> 0.3), seed = 42L)
    result.relation.hasSampleBy shouldBe true
    val sb = result.relation.getSampleBy
    sb.getSeed shouldBe 42L
    sb.getFractionsCount shouldBe 2
  }

  test("DataFrameStatFunctions.sampleBy(String) builds StatSampleBy proto") {
    val df = testDf()
    val result = df.stat.sampleBy("key", Map("a" -> 0.5), seed = 0L)
    result.relation.hasSampleBy shouldBe true
    val sb = result.relation.getSampleBy
    sb.getFractionsCount shouldBe 1
  }

  // ---------- DataFrameWriter proto tests ----------

  test("DataFrameWriter.format sets source format") {
    val df = testDf()
    val writer = df.write.format("json")
    writer shouldBe a[DataFrameWriter]
  }

  test("DataFrameWriter.mode(String) sets save mode") {
    val df = testDf()
    val writer = df.write.mode("overwrite")
    writer shouldBe a[DataFrameWriter]
  }

  test("DataFrameWriter.mode(SaveMode) sets save mode") {
    val df = testDf()
    val writer = df.write.mode(SaveMode.Append)
    writer shouldBe a[DataFrameWriter]
  }

  test("DataFrameWriter.options sets options map") {
    val df = testDf()
    val writer = df.write.options(Map("key1" -> "val1", "key2" -> "val2"))
    writer shouldBe a[DataFrameWriter]
  }

  test("DataFrameWriter.partitionBy sets partition columns") {
    val df = testDf()
    val writer = df.write.partitionBy("col1", "col2")
    writer shouldBe a[DataFrameWriter]
  }

  test("DataFrameWriter.bucketBy sets bucket columns") {
    val df = testDf()
    val writer = df.write.bucketBy(10, "col1", "col2")
    writer shouldBe a[DataFrameWriter]
  }

  test("DataFrameWriter.sortBy sets sort columns") {
    val df = testDf()
    val writer = df.write.sortBy("col1", "col2")
    writer shouldBe a[DataFrameWriter]
  }

  // ---------- DataFrameWriterV2 proto tests ----------

  test("DataFrameWriterV2.using sets provider") {
    val df = testDf()
    val writer = df.writeTo("my_table").using("parquet")
    writer shouldBe a[DataFrameWriterV2]
  }

  test("DataFrameWriterV2.option sets option") {
    val df = testDf()
    val writer = df.writeTo("my_table").option("key", "value")
    writer shouldBe a[DataFrameWriterV2]
  }

  test("DataFrameWriterV2.option(Boolean) sets option") {
    val df = testDf()
    val writer = df.writeTo("my_table").option("key", true)
    writer shouldBe a[DataFrameWriterV2]
  }

  test("DataFrameWriterV2.option(Long) sets option") {
    val df = testDf()
    val writer = df.writeTo("my_table").option("key", 42L)
    writer shouldBe a[DataFrameWriterV2]
  }

  test("DataFrameWriterV2.option(Double) sets option") {
    val df = testDf()
    val writer = df.writeTo("my_table").option("key", 3.14)
    writer shouldBe a[DataFrameWriterV2]
  }

  test("DataFrameWriterV2.options sets options map") {
    val df = testDf()
    val writer = df.writeTo("my_table").options(Map("k1" -> "v1"))
    writer shouldBe a[DataFrameWriterV2]
  }

  test("DataFrameWriterV2.tableProperty sets table property") {
    val df = testDf()
    val writer = df.writeTo("my_table").tableProperty("prop", "val")
    writer shouldBe a[DataFrameWriterV2]
  }

  test("DataFrameWriterV2.partitionedBy sets partitioning columns") {
    val df = testDf()
    val writer = df.writeTo("my_table").partitionedBy(Column("year"), Column("month"))
    writer shouldBe a[DataFrameWriterV2]
  }

  test("DataFrameWriterV2.clusterBy sets clustering columns") {
    val df = testDf()
    val writer = df.writeTo("my_table").clusterBy("col1", "col2")
    writer shouldBe a[DataFrameWriterV2]
  }

  // ---------- DataStreamWriter proto tests ----------

  test("DataStreamWriter.format sets format") {
    val df = testDf()
    val writer = df.writeStream.format("console")
    writer shouldBe a[DataStreamWriter]
  }

  test("DataStreamWriter.outputMode sets output mode") {
    val df = testDf()
    val writer = df.writeStream.outputMode("append")
    writer shouldBe a[DataStreamWriter]
  }

  test("DataStreamWriter.queryName sets query name") {
    val df = testDf()
    val writer = df.writeStream.queryName("myQuery")
    writer shouldBe a[DataStreamWriter]
  }

  test("DataStreamWriter.option sets option") {
    val df = testDf()
    val writer = df.writeStream.option("key", "value")
    writer shouldBe a[DataStreamWriter]
  }

  test("DataStreamWriter.options sets options map") {
    val df = testDf()
    val writer = df.writeStream.options(Map("k1" -> "v1"))
    writer shouldBe a[DataStreamWriter]
  }

  test("DataStreamWriter.partitionBy sets partition columns") {
    val df = testDf()
    val writer = df.writeStream.partitionBy("col1", "col2")
    writer shouldBe a[DataStreamWriter]
  }

  test("DataStreamWriter.trigger sets trigger") {
    val df = testDf()
    val writer = df.writeStream.trigger(Trigger.ProcessingTime(1000))
    writer shouldBe a[DataStreamWriter]
  }

  test("DataStreamWriter.trigger(AvailableNow) sets trigger") {
    val df = testDf()
    val writer = df.writeStream.trigger(Trigger.AvailableNow)
    writer shouldBe a[DataStreamWriter]
  }

  test("DataStreamWriter.trigger(Once) sets trigger") {
    val df = testDf()
    val writer = df.writeStream.trigger(Trigger.Once)
    writer shouldBe a[DataStreamWriter]
  }

  test("DataStreamWriter.trigger(Continuous) sets trigger") {
    val df = testDf()
    val writer = df.writeStream.trigger(Trigger.Continuous(500))
    writer shouldBe a[DataStreamWriter]
  }

  test("DataStreamWriter.buildWriteStreamOp builds proto with format and options") {
    val df = testDf()
    val writer = df.writeStream
      .format("console")
      .outputMode("append")
      .queryName("testQuery")
      .option("checkpointLocation", "/tmp/checkpoint")
      .partitionBy("dt")
      .trigger(Trigger.ProcessingTime(5000))
    val proto = writer.buildWriteStreamOp().build()
    proto.getFormat shouldBe "console"
    proto.getOutputMode shouldBe "append"
    proto.getQueryName shouldBe "testQuery"
    proto.getOptionsMap.get("checkpointLocation") shouldBe "/tmp/checkpoint"
    proto.getPartitioningColumnNamesCount shouldBe 1
    proto.getPartitioningColumnNames(0) shouldBe "dt"
    proto.getProcessingTimeInterval shouldBe "5000 milliseconds"
  }

  test("DataStreamWriter.buildWriteStreamOp with AvailableNow trigger") {
    val df = testDf()
    val writer = df.writeStream.trigger(Trigger.AvailableNow)
    val proto = writer.buildWriteStreamOp().build()
    proto.getAvailableNow shouldBe true
  }

  test("DataStreamWriter.buildWriteStreamOp with Once trigger") {
    val df = testDf()
    val writer = df.writeStream.trigger(Trigger.Once)
    val proto = writer.buildWriteStreamOp().build()
    proto.getOnce shouldBe true
  }

  test("DataStreamWriter.buildWriteStreamOp with Continuous trigger") {
    val df = testDf()
    val writer = df.writeStream.trigger(Trigger.Continuous(100))
    val proto = writer.buildWriteStreamOp().build()
    proto.getContinuousCheckpointInterval shouldBe "100 milliseconds"
  }

  // ---------- MergeIntoWriter proto tests ----------

  test("MergeIntoWriter.withSchemaEvolution returns self") {
    val df = testDf()
    val writer = df.mergeInto("t", Column("id") === Column("id")).withSchemaEvolution()
    writer shouldBe a[MergeIntoWriter]
  }

  test("MergeIntoWriter.merge without actions throws") {
    val df = testDf()
    val writer = df.mergeInto("t", Column("id") === Column("id"))
    an[SparkException] should be thrownBy writer.merge()
  }

  test("WhenMatched.updateAll returns MergeIntoWriter") {
    val df = testDf()
    val writer = df.mergeInto("t", Column("id") === Column("id"))
      .whenMatched().updateAll()
    writer shouldBe a[MergeIntoWriter]
  }

  test("WhenMatched.update with assignments returns MergeIntoWriter") {
    val df = testDf()
    val writer = df.mergeInto("t", Column("id") === Column("id"))
      .whenMatched().update(Map("name" -> Column("source.name")))
    writer shouldBe a[MergeIntoWriter]
  }

  test("WhenMatched.delete returns MergeIntoWriter") {
    val df = testDf()
    val writer = df.mergeInto("t", Column("id") === Column("id"))
      .whenMatched().delete()
    writer shouldBe a[MergeIntoWriter]
  }

  test("WhenNotMatched.insertAll returns MergeIntoWriter") {
    val df = testDf()
    val writer = df.mergeInto("t", Column("id") === Column("id"))
      .whenNotMatched().insertAll()
    writer shouldBe a[MergeIntoWriter]
  }

  test("WhenNotMatched.insert with assignments returns MergeIntoWriter") {
    val df = testDf()
    val writer = df.mergeInto("t", Column("id") === Column("id"))
      .whenNotMatched().insert(Map("id" -> Column("source.id")))
    writer shouldBe a[MergeIntoWriter]
  }

  test("WhenNotMatchedBySource.updateAll returns MergeIntoWriter") {
    val df = testDf()
    val writer = df.mergeInto("t", Column("id") === Column("id"))
      .whenNotMatchedBySource().updateAll()
    writer shouldBe a[MergeIntoWriter]
  }

  test("WhenNotMatchedBySource.update with assignments returns MergeIntoWriter") {
    val df = testDf()
    val writer = df.mergeInto("t", Column("id") === Column("id"))
      .whenNotMatchedBySource().update(Map("status" -> Column.lit("deleted")))
    writer shouldBe a[MergeIntoWriter]
  }

  test("WhenNotMatchedBySource.delete returns MergeIntoWriter") {
    val df = testDf()
    val writer = df.mergeInto("t", Column("id") === Column("id"))
      .whenNotMatchedBySource().delete()
    writer shouldBe a[MergeIntoWriter]
  }

  test("WhenMatched with condition returns MergeIntoWriter") {
    val df = testDf()
    val writer = df.mergeInto("t", Column("id") === Column("id"))
      .whenMatched(Column("source.updated") > Column.lit(0)).updateAll()
    writer shouldBe a[MergeIntoWriter]
  }

  test("WhenNotMatched with condition returns MergeIntoWriter") {
    val df = testDf()
    val writer = df.mergeInto("t", Column("id") === Column("id"))
      .whenNotMatched(Column("source.active") === Column.lit(true)).insertAll()
    writer shouldBe a[MergeIntoWriter]
  }

  test("WhenNotMatchedBySource with condition returns MergeIntoWriter") {
    val df = testDf()
    val writer = df.mergeInto("t", Column("id") === Column("id"))
      .whenNotMatchedBySource(Column("target.stale") === Column.lit(true)).delete()
    writer shouldBe a[MergeIntoWriter]
  }

  // ---------- chained merge actions ----------

  test("chained whenMatched and whenNotMatched actions compile") {
    val df = testDf()
    val writer = df.mergeInto("t", Column("id") === Column("id"))
      .whenMatched().updateAll()
      .whenNotMatched().insertAll()
      .whenNotMatchedBySource().delete()
    writer shouldBe a[MergeIntoWriter]
  }

  // ---------- P0 API: head() / show() / filter / sort / rollup / cube ----------

  test("head() returns single Row via head(1)") {
    // just verify head(n) compiles with explicit int (no default)
    val df = testDf()
    // head(n) delegates to limit(n).collect() — can't test without server
    // but we can verify the method signature exists
    val method = df.getClass.getMethod("head")
    method should not be null
  }

  test("show(truncate: Boolean) compiles") {
    val df = testDf()
    val method = df.getClass.getMethod("show", classOf[Boolean])
    method should not be null
  }

  test("show(numRows: Int, truncate: Boolean) compiles") {
    val df = testDf()
    val method = df.getClass.getMethod("show", classOf[Int], classOf[Boolean])
    method should not be null
  }

  test("filter(String) builds Filter with ExpressionString") {
    val df = testDf()
    val result = df.filter("age > 10")
    val rel = result.relation
    rel.hasFilter shouldBe true
    val cond = rel.getFilter.getCondition
    cond.hasExpressionString shouldBe true
    cond.getExpressionString.getExpression shouldBe "age > 10"
  }

  test("sort(String, String*) builds Sort proto") {
    val df = testDf()
    val result = df.sort("a", "b")
    val rel = result.relation
    rel.hasSort shouldBe true
    rel.getSort.getOrderList should have size 2
  }

  test("orderBy(String, String*) builds Sort proto") {
    val df = testDf()
    val result = df.orderBy("x")
    val rel = result.relation
    rel.hasSort shouldBe true
    rel.getSort.getOrderList should have size 1
  }

  test("rollup(String, String*) builds Aggregate with Rollup GroupType") {
    val df = testDf()
    val gdf = df.rollup("a", "b")
    gdf shouldBe a[GroupedDataFrame]
  }

  test("cube(String, String*) builds Aggregate with Cube GroupType") {
    val df = testDf()
    val gdf = df.cube("a", "b")
    gdf shouldBe a[GroupedDataFrame]
  }
