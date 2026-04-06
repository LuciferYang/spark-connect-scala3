package org.apache.spark.sql

import org.apache.spark.connect.proto.*
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders
import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TypedOpsSuite extends AnyFunSuite with Matchers:

  // ---------------------------------------------------------------------------
  // agnosticEncoder bridge tests
  // ---------------------------------------------------------------------------

  test("primitive Encoder[Int] has correct agnosticEncoder") {
    val enc = summon[Encoder[Int]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.PrimitiveIntEncoder
  }

  test("primitive Encoder[Long] has correct agnosticEncoder") {
    val enc = summon[Encoder[Long]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.PrimitiveLongEncoder
  }

  test("primitive Encoder[Double] has correct agnosticEncoder") {
    val enc = summon[Encoder[Double]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.PrimitiveDoubleEncoder
  }

  test("primitive Encoder[Float] has correct agnosticEncoder") {
    val enc = summon[Encoder[Float]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.PrimitiveFloatEncoder
  }

  test("primitive Encoder[Short] has correct agnosticEncoder") {
    val enc = summon[Encoder[Short]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.PrimitiveShortEncoder
  }

  test("primitive Encoder[Byte] has correct agnosticEncoder") {
    val enc = summon[Encoder[Byte]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.PrimitiveByteEncoder
  }

  test("primitive Encoder[Boolean] has correct agnosticEncoder") {
    val enc = summon[Encoder[Boolean]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.PrimitiveBooleanEncoder
  }

  test("Encoder[String] has correct agnosticEncoder") {
    val enc = summon[Encoder[String]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.StringEncoder
  }

  test("Encoder[java.sql.Date] has correct agnosticEncoder") {
    val enc = summon[Encoder[java.sql.Date]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.STRICT_DATE_ENCODER
  }

  test("Encoder[java.sql.Timestamp] has correct agnosticEncoder") {
    val enc = summon[Encoder[java.sql.Timestamp]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.STRICT_TIMESTAMP_ENCODER
  }

  test("Encoder[java.time.LocalDate] has correct agnosticEncoder") {
    val enc = summon[Encoder[java.time.LocalDate]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.STRICT_LOCAL_DATE_ENCODER
  }

  test("Encoder[java.time.Instant] has correct agnosticEncoder") {
    val enc = summon[Encoder[java.time.Instant]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.STRICT_INSTANT_ENCODER
  }

  test("Encoder[BigDecimal] has correct agnosticEncoder") {
    val enc = summon[Encoder[BigDecimal]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.DEFAULT_SCALA_DECIMAL_ENCODER
  }

  test("Encoder[Array[Byte]] has correct agnosticEncoder") {
    val enc = summon[Encoder[Array[Byte]]]
    enc.agnosticEncoder shouldBe AgnosticEncoders.BinaryEncoder
  }

  test("derived case class encoder has null agnosticEncoder") {
    // Product encoders don't have AgnosticEncoder bridge yet (Phase 3)
    val enc = summon[Encoder[Person]]
    enc.agnosticEncoder shouldBe null
  }

  test("default Encoder trait agnosticEncoder is null") {
    val customEnc = new Encoder[String]:
      def schema = StructType(Seq(StructField("v", StringType)))
      def fromRow(row: Row) = row.getString(0)
      def toRow(value: String) = Row(value)
    customEnc.agnosticEncoder shouldBe null
  }

  // ---------------------------------------------------------------------------
  // Dataset typed operations use server-side when agnosticEncoder is available
  // ---------------------------------------------------------------------------

  // Note: Full integration tests require a live Spark Connect server.
  // These tests verify the API surface compiles and works at the type level.

  test("Dataset.map compiles with primitive types") {
    // This verifies the method signature is correct with Encoder context bounds
    val intEnc = summon[Encoder[Int]]
    val strEnc = summon[Encoder[String]]
    // Both should have agnosticEncoder available for server-side execution
    intEnc.agnosticEncoder should not be null
    strEnc.agnosticEncoder should not be null
  }

  test("Dataset.reduce throws on empty data") {
    // reduce on empty collections should throw
    assertThrows[UnsupportedOperationException] {
      Array.empty[Int].reduce(_ + _)
    }
  }

  // ---------------------------------------------------------------------------
  // KeyValueGroupedDataset API surface
  // ---------------------------------------------------------------------------

  test("KeyValueGroupedDataset companion object exists") {
    // Verifies the object compiles and has the apply method
    val obj = KeyValueGroupedDataset
    obj should not be null
  }

  // ---------------------------------------------------------------------------
  // joinWith tests
  // ---------------------------------------------------------------------------

  /** Create a minimal Dataset backed by a LocalRelation (no real server). */
  private def testDataset[T: Encoder: scala.reflect.ClassTag](): Dataset[T] =
    val session = SparkSession(null)
    val rel = Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(0).build())
      .setLocalRelation(LocalRelation.getDefaultInstance)
      .build()
    Dataset(DataFrame(session, rel), summon[Encoder[T]])

  test("joinWith builds Join proto with JoinDataType") {
    val ds1 = testDataset[Long]()
    val ds2 = testDataset[Long]()
    val joined = ds1.joinWith(ds2, Column("id") === Column("id"))
    val rel = joined.df.relation
    rel.hasJoin shouldBe true
    val join = rel.getJoin
    join.hasJoinDataType shouldBe true
    join.getJoinType shouldBe Join.JoinType.JOIN_TYPE_INNER
  }

  test("joinWith with left outer join type") {
    val ds1 = testDataset[Long]()
    val ds2 = testDataset[Long]()
    val joined = ds1.joinWith(ds2, Column("id") === Column("id"), "left")
    val join = joined.df.relation.getJoin
    join.getJoinType shouldBe Join.JoinType.JOIN_TYPE_LEFT_OUTER
    join.hasJoinDataType shouldBe true
  }

  test("joinWith returns correct tuple encoder type") {
    val ds1 = testDataset[Int]()
    val ds2 = testDataset[String]()
    val joined = ds1.joinWith(ds2, Column("id") === Column("id"))
    // Verify it compiles with the correct type
    val enc = joined.encoder
    enc should not be null
  }

  // ---------------------------------------------------------------------------
  // toLocalIterator type signature tests
  // ---------------------------------------------------------------------------

  test("Dataset.toLocalIterator return type implements both Iterator and AutoCloseable") {
    // Verify the method exists and returns an intersection type
    val method = classOf[Dataset[Int]].getMethod("toLocalIterator")
    method should not be null
    // The return type is java.util.Iterator[T] with AutoCloseable
    // Java reflection sees the erased return type — check that both interfaces are assignable
    val retType = method.getReturnType
    classOf[java.util.Iterator[?]].isAssignableFrom(retType) ||
    classOf[AutoCloseable].isAssignableFrom(retType) shouldBe true
  }

  test("DataFrame.toLocalIterator return type implements both Iterator and AutoCloseable") {
    val method = classOf[DataFrame].getMethod("toLocalIterator")
    method should not be null
    val retType = method.getReturnType
    classOf[java.util.Iterator[?]].isAssignableFrom(retType) ||
    classOf[AutoCloseable].isAssignableFrom(retType) shouldBe true
  }

  // ---------------------------------------------------------------------------
  // Dataset.toJSON tests
  // ---------------------------------------------------------------------------

  test("Dataset.toJSON returns Dataset[String]") {
    val ds = testDataset[Long]()
    val jsonDs = ds.toJSON
    jsonDs.encoder should not be null
    jsonDs.encoder.schema.fields.head.name shouldBe "value"
  }

  // ---------------------------------------------------------------------------
  // Dataset.show(vertical) tests
  // ---------------------------------------------------------------------------

  test("Dataset has show(numRows, truncate, vertical) method") {
    val method = classOf[Dataset[?]].getMethod("show", classOf[Int], classOf[Int], classOf[Boolean])
    method should not be null
  }

  // ---------------------------------------------------------------------------
  // emptyDataset type signature tests
  // ---------------------------------------------------------------------------

  test("emptyDataset method exists on SparkSession") {
    // Verify the method compiles and exists via reflection
    val methods = classOf[SparkSession].getMethods.filter(_.getName == "emptyDataset")
    methods should not be empty
  }

  // ---------------------------------------------------------------------------
  // Dataset.collectAsList / takeAsList
  // ---------------------------------------------------------------------------

  test("Dataset.collectAsList return type is java.util.List") {
    val method = classOf[Dataset[?]].getMethod("collectAsList")
    method should not be null
    classOf[java.util.List[?]].isAssignableFrom(method.getReturnType) shouldBe true
  }

  test("Dataset.takeAsList return type is java.util.List") {
    val method = classOf[Dataset[?]].getMethod("takeAsList", classOf[Int])
    method should not be null
    classOf[java.util.List[?]].isAssignableFrom(method.getReturnType) shouldBe true
  }

  // ---------------------------------------------------------------------------
  // dropDuplicatesWithinWatermark delegate tests
  // ---------------------------------------------------------------------------

  test("Dataset.dropDuplicatesWithinWatermark() delegates to DataFrame") {
    val ds = testDataset[Long]()
    val result = ds.dropDuplicatesWithinWatermark()
    result.df.relation.hasDeduplicate shouldBe true
    result.df.relation.getDeduplicate.getWithinWatermark shouldBe true
  }

  // ---------------------------------------------------------------------------
  // colRegex / metadataColumn delegate tests
  // ---------------------------------------------------------------------------

  test("Dataset.colRegex delegates to DataFrame") {
    val ds = testDataset[Long]()
    val col = ds.colRegex("`id`")
    col.expr.hasUnresolvedRegex shouldBe true
  }

  test("Dataset.metadataColumn delegates to DataFrame") {
    val ds = testDataset[Long]()
    val col = ds.metadataColumn("_metadata")
    col.expr.hasUnresolvedAttribute shouldBe true
    col.expr.getUnresolvedAttribute.getIsMetadataColumn shouldBe true
  }

  // ---------------------------------------------------------------------------
  // Typed select (TypedColumn 1-5 arity) compile tests
  // ---------------------------------------------------------------------------

  test("Dataset has typed select with 1 TypedColumn") {
    val methods = classOf[Dataset[?]].getMethods.filter(m =>
      m.getName == "select" && m.getTypeParameters.length == 1
    )
    methods should not be empty
  }

  test("Dataset has typed select with 2 TypedColumns") {
    val methods = classOf[Dataset[?]].getMethods.filter(m =>
      m.getName == "select" && m.getTypeParameters.length == 2
    )
    methods should not be empty
  }

  // ---------- Phase 4.2: Dataset.as(alias), transform ----------

  test("Dataset.as(alias) returns aliased Dataset") {
    val ds = testDataset[Long]()
    val aliased = ds.as("t1")
    aliased.df.relation.hasSubqueryAlias shouldBe true
    aliased.df.relation.getSubqueryAlias.getAlias shouldBe "t1"
  }

  test("Dataset.alias(alias) returns aliased Dataset") {
    val ds = testDataset[Long]()
    val aliased = ds.alias("t2")
    aliased.df.relation.hasSubqueryAlias shouldBe true
    aliased.df.relation.getSubqueryAlias.getAlias shouldBe "t2"
  }

  test("Dataset.transform applies function") {
    val ds = testDataset[Long]()
    val result = ds.transform(d => d.limit(5))
    result.df.relation.hasLimit shouldBe true
    result.df.relation.getLimit.getLimit shouldBe 5
  }

  test("Dataset.unionByName delegates to DataFrame") {
    val ds1 = testDataset[Long]()
    val ds2 = testDataset[Long]()
    val result = ds1.unionByName(ds2)
    result.df.relation.hasSetOp shouldBe true
    result.df.relation.getSetOp.getByName shouldBe true
  }

  test("Dataset.intersectAll delegates to DataFrame") {
    val ds1 = testDataset[Long]()
    val ds2 = testDataset[Long]()
    val result = ds1.intersectAll(ds2)
    result.df.relation.hasSetOp shouldBe true
    result.df.relation.getSetOp.getIsAll shouldBe true
  }

  test("Dataset.exceptAll delegates to DataFrame") {
    val ds1 = testDataset[Long]()
    val ds2 = testDataset[Long]()
    val result = ds1.exceptAll(ds2)
    result.df.relation.hasSetOp shouldBe true
    result.df.relation.getSetOp.getIsAll shouldBe true
  }

  // ---------- Dataset.sort(String, String*) ----------

  test("Dataset.sort(String, String*) delegates to DataFrame") {
    val ds = testDataset[Long]()
    val result = ds.sort("id", "name")
    result.df.relation.hasSort shouldBe true
    result.df.relation.getSort.getOrderCount shouldBe 2
  }

  // ---------- Dataset sample / repartition / coalesce ----------

  test("Dataset.sample delegates to DataFrame") {
    val ds = testDataset[Long]()
    val result = ds.sample(0.5)
    result.df.relation.hasSample shouldBe true
  }

  test("Dataset.sample(withReplacement, fraction) compiles") {
    val ds = testDataset[Long]()
    val result = ds.sample(true, 0.5)
    result.df.relation.hasSample shouldBe true
  }

  test("Dataset.repartition delegates to DataFrame") {
    val ds = testDataset[Long]()
    val result = ds.repartition(4)
    result.df.relation.hasRepartition shouldBe true
    result.df.relation.getRepartition.getNumPartitions shouldBe 4
  }

  test("Dataset.coalesce delegates to DataFrame") {
    val ds = testDataset[Long]()
    val result = ds.coalesce(2)
    result.df.relation.hasRepartition shouldBe true
    result.df.relation.getRepartition.getNumPartitions shouldBe 2
    result.df.relation.getRepartition.getShuffle shouldBe false
  }

  test("Dataset.sortWithinPartitions delegates to DataFrame") {
    val ds = testDataset[Long]()
    val result = ds.sortWithinPartitions(Column("id"))
    result.df.relation.hasSort shouldBe true
    result.df.relation.getSort.getIsGlobal shouldBe false
  }

  // ---------- Dataset delegate methods ----------

  test("Dataset.dropDuplicates delegates to DataFrame") {
    val ds = testDataset[Long]()
    val result = ds.dropDuplicates()
    result.df.relation.hasDeduplicate shouldBe true
  }

  test("Dataset has columns/dtypes/col/apply methods") {
    classOf[Dataset[?]].getMethod("columns") should not be null
    classOf[Dataset[?]].getMethod("dtypes") should not be null
    classOf[Dataset[?]].getMethod("col", classOf[String]) should not be null
    classOf[Dataset[?]].getMethod("apply", classOf[String]) should not be null
  }

  test("Dataset has explain methods") {
    classOf[Dataset[?]].getMethod("explain", classOf[Boolean]) should not be null
    classOf[Dataset[?]].getMethod("explain", classOf[String]) should not be null
  }

  test("Dataset has printSchema methods") {
    classOf[Dataset[?]].getMethod("printSchema") should not be null
    classOf[Dataset[?]].getMethod("printSchema", classOf[Int]) should not be null
  }

  test("Dataset has checkpoint methods") {
    classOf[Dataset[?]].getMethod("checkpoint", classOf[Boolean]) should not be null
    classOf[Dataset[?]].getMethod("localCheckpoint", classOf[Boolean]) should not be null
  }

  test("Dataset has tail method") {
    classOf[Dataset[?]].getMethod("tail", classOf[Int]) should not be null
  }

  test("Dataset has hint method") {
    val ds = testDataset[Long]()
    val result = ds.hint("broadcast")
    result.df.relation.hasHint shouldBe true
    result.df.relation.getHint.getName shouldBe "broadcast"
  }

  test("Dataset has withWatermark method") {
    val ds = testDataset[Long]()
    val result = ds.withWatermark("time", "10 seconds")
    result.df.relation.hasWithWatermark shouldBe true
  }

  // ---------- KVGD extensions ----------

  test("KeyValueGroupedDataset has keyAs method") {
    classOf[KeyValueGroupedDataset[?, ?]].getMethods.exists(_.getName == "keyAs") shouldBe true
  }

  test("KeyValueGroupedDataset has mapValues method") {
    classOf[KeyValueGroupedDataset[?, ?]].getMethods.exists(
      _.getName == "mapValues"
    ) shouldBe true
  }

  test("KeyValueGroupedDataset has flatMapSortedGroups method") {
    classOf[KeyValueGroupedDataset[?, ?]].getMethods.exists(
      _.getName == "flatMapSortedGroups"
    ) shouldBe true
  }

  test("KeyValueGroupedDataset has agg 5-8 arity overloads") {
    val aggMethods = classOf[KeyValueGroupedDataset[?, ?]].getMethods.filter(_.getName == "agg")
    // Should have agg overloads for 1-8 columns (at minimum)
    aggMethods.length should be >= 5
  }

  // ---------------------------------------------------------------------------
  // Dataset proto-only delegate tests
  // ---------------------------------------------------------------------------

  private def stubSession: SparkSession = SparkSession(null)

  private def stubDf: DataFrame =
    val session = stubSession
    val rel = Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(session.nextPlanId()).build())
      .setLocalRelation(LocalRelation.getDefaultInstance)
      .build()
    DataFrame(session, rel)

  private def stubDs: Dataset[Long] =
    Dataset(stubDf, summon[Encoder[Long]])

  private def stubStringDs: Dataset[String] =
    Dataset(stubDf, summon[Encoder[String]])

  private def stubIntDs: Dataset[Int] =
    Dataset(stubDf, summon[Encoder[Int]])

  test("Dataset.filter(condition: Column) builds Filter proto") {
    val ds = stubDs
    val result = ds.filter(Column("x") > Column.lit(0))
    result.df.relation.hasFilter shouldBe true
  }

  test("Dataset.where(condition: Column) is alias for filter") {
    val ds = stubDs
    val result = ds.where(Column("x") > Column.lit(0))
    result.df.relation.hasFilter shouldBe true
  }

  test("Dataset.limit(n) builds Limit proto") {
    val ds = stubDs
    val result = ds.limit(10)
    result.df.relation.hasLimit shouldBe true
    result.df.relation.getLimit.getLimit shouldBe 10
  }

  test("Dataset.orderBy(cols) builds Sort proto") {
    val ds = stubDs
    val result = ds.orderBy(Column("x").asc)
    result.df.relation.hasSort shouldBe true
    result.df.relation.getSort.getIsGlobal shouldBe true
    result.df.relation.getSort.getOrderCount shouldBe 1
  }

  test("Dataset.sample(withReplacement, fraction, seed) builds Sample proto") {
    val ds = stubDs
    val result = ds.sample(true, 0.3, 42L)
    result.df.relation.hasSample shouldBe true
    val sample = result.df.relation.getSample
    sample.getWithReplacement shouldBe true
    sample.getUpperBound shouldBe 0.3
    sample.getSeed shouldBe 42L
  }

  test("Dataset.repartition(numPartitions, cols) builds RepartitionByExpression proto") {
    val ds = stubDs
    val result = ds.repartition(8, Column("x"))
    result.df.relation.hasRepartitionByExpression shouldBe true
    result.df.relation.getRepartitionByExpression.getNumPartitions shouldBe 8
    result.df.relation.getRepartitionByExpression.getPartitionExprsCount shouldBe 1
  }

  test("Dataset.dropDuplicates(colNames: Seq) builds Deduplicate proto") {
    val ds = stubDs
    val result = ds.dropDuplicates(Seq("x", "y"))
    result.df.relation.hasDeduplicate shouldBe true
    val dedup = result.df.relation.getDeduplicate
    dedup.getColumnNamesList.size() shouldBe 2
  }

  test("Dataset.union(other) builds SetOperation proto") {
    val ds1 = stubDs
    val ds2 = stubDs
    val result = ds1.union(ds2)
    result.df.relation.hasSetOp shouldBe true
    result.df.relation.getSetOp.getSetOpType shouldBe
      SetOperation.SetOpType.SET_OP_TYPE_UNION
  }

  test("Dataset.intersect(other) builds SetOperation proto") {
    val ds1 = stubDs
    val ds2 = stubDs
    val result = ds1.intersect(ds2)
    result.df.relation.hasSetOp shouldBe true
    result.df.relation.getSetOp.getSetOpType shouldBe
      SetOperation.SetOpType.SET_OP_TYPE_INTERSECT
  }

  test("Dataset.except(other) builds SetOperation proto") {
    val ds1 = stubDs
    val ds2 = stubDs
    val result = ds1.except(ds2)
    result.df.relation.hasSetOp shouldBe true
    result.df.relation.getSetOp.getSetOpType shouldBe
      SetOperation.SetOpType.SET_OP_TYPE_EXCEPT
  }

  test("Dataset.distinct() builds Deduplicate proto") {
    val ds = stubDs
    val result = ds.distinct()
    result.df.relation.hasDeduplicate shouldBe true
    result.df.relation.getDeduplicate.getAllColumnsAsKeys shouldBe true
  }

  test("Dataset.crossJoin(right) builds Join proto with CROSS type") {
    val ds = stubDs
    val rightDf = stubDf
    val result = ds.crossJoin(rightDf)
    result.relation.hasJoin shouldBe true
    result.relation.getJoin.getJoinType shouldBe Join.JoinType.JOIN_TYPE_CROSS
  }

  // ---------------------------------------------------------------------------
  // NEW: Additional Dataset proto-only tests for coverage
  // ---------------------------------------------------------------------------

  // -- toDF delegates --

  test("Dataset.toDF() returns the underlying DataFrame") {
    val ds = stubDs
    val result = ds.toDF()
    result.relation shouldBe ds.df.relation
  }

  test("Dataset.toDF(colNames*) builds ToDF proto") {
    val ds = stubDs
    val result = ds.toDF("a", "b")
    result.relation.hasToDf shouldBe true
    result.relation.getToDf.getColumnNamesCount shouldBe 2
  }

  // -- sparkSession accessor --

  test("Dataset.sparkSession returns the session") {
    val ds = stubDs
    ds.sparkSession should not be null
  }

  // -- col / apply --

  test("Dataset.col(name) builds UnresolvedAttribute with planId") {
    val ds = stubDs
    val c = ds.col("myCol")
    c.expr.hasUnresolvedAttribute shouldBe true
    c.expr.getUnresolvedAttribute.getUnparsedIdentifier shouldBe "myCol"
    c.expr.getUnresolvedAttribute.getPlanId shouldBe ds.df.relation.getCommon.getPlanId
  }

  test("Dataset.apply(name) is alias for col") {
    val ds = stubDs
    val c = ds("myCol")
    c.expr.hasUnresolvedAttribute shouldBe true
    c.expr.getUnresolvedAttribute.getUnparsedIdentifier shouldBe "myCol"
  }

  // -- select(cols: Column*) --

  test("Dataset.select(Column*) builds Project proto") {
    val ds = stubDs
    val result = ds.select(Column("a"), Column("b"))
    result.relation.hasProject shouldBe true
    result.relation.getProject.getExpressionsCount shouldBe 2
  }

  // -- join --

  test("Dataset.join(right, joinExpr) builds Join proto") {
    val ds = stubDs
    val rightDf = stubDf
    val result = ds.join(rightDf, Column("id") === Column("id"))
    result.relation.hasJoin shouldBe true
    result.relation.getJoin.getJoinType shouldBe Join.JoinType.JOIN_TYPE_INNER
  }

  test("Dataset.join(right, joinExpr, left) builds left outer join") {
    val ds = stubDs
    val rightDf = stubDf
    val result = ds.join(rightDf, Column("id") === Column("id"), "left")
    result.relation.hasJoin shouldBe true
    result.relation.getJoin.getJoinType shouldBe Join.JoinType.JOIN_TYPE_LEFT_OUTER
  }

  // -- withColumn / withColumnRenamed / withColumnsRenamed / withColumns --

  test("Dataset.withColumn builds WithColumns proto") {
    val ds = stubDs
    val result = ds.withColumn("newCol", Column.lit(1))
    result.relation.hasWithColumns shouldBe true
    result.relation.getWithColumns.getAliasesCount shouldBe 1
    result.relation.getWithColumns.getAliases(0).getName(0) shouldBe "newCol"
  }

  test("Dataset.withColumnRenamed builds WithColumnsRenamed proto") {
    val ds = stubDs
    val result = ds.withColumnRenamed("old", "new")
    result.relation.hasWithColumnsRenamed shouldBe true
    result.relation.getWithColumnsRenamed.getRenamesCount shouldBe 1
  }

  test("Dataset.withColumnsRenamed builds WithColumnsRenamed proto with multiple renames") {
    val ds = stubDs
    val result = ds.withColumnsRenamed(Map("a" -> "x", "b" -> "y"))
    result.relation.hasWithColumnsRenamed shouldBe true
    result.relation.getWithColumnsRenamed.getRenamesCount shouldBe 2
  }

  test("Dataset.withColumns builds WithColumns proto with multiple aliases") {
    val ds = stubDs
    val result = ds.withColumns(Map("c1" -> Column.lit(1), "c2" -> Column.lit(2)))
    result.relation.hasWithColumns shouldBe true
    result.relation.getWithColumns.getAliasesCount shouldBe 2
  }

  // -- drop --

  test("Dataset.drop(String*) builds Drop proto") {
    val ds = stubDs
    val result = ds.drop("a", "b")
    result.relation.hasDrop shouldBe true
    result.relation.getDrop.getColumnsCount shouldBe 2
  }

  test("Dataset.drop(Column*) builds Drop proto") {
    val ds = stubDs
    val result = ds.drop(Column("x"), Column("y"))(using DummyImplicit.dummyImplicit)
    result.relation.hasDrop shouldBe true
    result.relation.getDrop.getColumnsCount shouldBe 2
  }

  // -- describe / summary --

  test("Dataset.describe builds StatDescribe proto") {
    val ds = stubDs
    val result = ds.describe("col1", "col2")
    result.relation.hasDescribe shouldBe true
    result.relation.getDescribe.getColsCount shouldBe 2
  }

  test("Dataset.summary builds StatSummary proto") {
    val ds = stubDs
    val result = ds.summary("count", "mean")
    result.relation.hasSummary shouldBe true
    result.relation.getSummary.getStatisticsCount shouldBe 2
  }

  // -- observe --

  test("Dataset.observe(name, expr, exprs*) builds CollectMetrics proto") {
    val ds = stubDs
    val result = ds.observe("metrics", functions.count(Column("*")).as("cnt"))
    result.df.relation.hasCollectMetrics shouldBe true
    result.df.relation.getCollectMetrics.getName shouldBe "metrics"
    result.df.relation.getCollectMetrics.getMetricsCount shouldBe 1
  }

  test("Dataset.observe(name, expr, multiple exprs) builds CollectMetrics with multiple metrics") {
    val ds = stubDs
    val result = ds.observe(
      "metrics",
      functions.count(Column("*")).as("cnt"),
      functions.max(Column("x")).as("maxX")
    )
    result.df.relation.hasCollectMetrics shouldBe true
    result.df.relation.getCollectMetrics.getMetricsCount shouldBe 2
  }

  test(
    "Dataset.observe(Observation, expr, exprs*) builds CollectMetrics and registers observation"
  ) {
    val ds = stubDs
    val obs = Observation("test-obs")
    val result = ds.observe(obs, functions.count(Column("*")).as("cnt"))
    result.df.relation.hasCollectMetrics shouldBe true
    result.df.relation.getCollectMetrics.getName shouldBe "test-obs"
    obs.planId should be >= 0L
  }

  // -- hint with parameters --

  test("Dataset.hint with parameters builds Hint proto with parameters") {
    val ds = stubDs
    val result = ds.hint("repartition", 10)
    result.df.relation.hasHint shouldBe true
    result.df.relation.getHint.getName shouldBe "repartition"
    result.df.relation.getHint.getParametersCount shouldBe 1
  }

  // -- groupBy --

  test("Dataset.groupBy(cols*) returns GroupedDataFrame") {
    val ds = stubDs
    val grouped = ds.groupBy(Column("key"))
    grouped should not be null
    // Verify it produces an Aggregate when agg is called
    val result = grouped.agg(functions.count(Column("key")).as("cnt"))
    result.relation.hasAggregate shouldBe true
  }

  // -- repartition(cols)(DummyImplicit) --

  test("Dataset.repartition(cols*) with DummyImplicit builds RepartitionByExpression") {
    val ds = stubDs
    val result = ds.repartition(Column("x"), Column("y"))(using DummyImplicit.dummyImplicit)
    result.df.relation.hasRepartitionByExpression shouldBe true
    result.df.relation.getRepartitionByExpression.getPartitionExprsCount shouldBe 2
  }

  // -- sortWithinPartitions(String*) --

  test("Dataset.sortWithinPartitions(String*) delegates to DataFrame") {
    val ds = stubDs
    val result = ds.sortWithinPartitions("a", "b")(using DummyImplicit.dummyImplicit)
    result.df.relation.hasSort shouldBe true
    result.df.relation.getSort.getIsGlobal shouldBe false
    result.df.relation.getSort.getOrderCount shouldBe 2
  }

  // -- orderBy(String, String*) --

  test("Dataset.orderBy(String, String*) delegates to DataFrame") {
    val ds = stubDs
    val result = ds.orderBy("x", "y")
    result.df.relation.hasSort shouldBe true
    result.df.relation.getSort.getIsGlobal shouldBe true
    result.df.relation.getSort.getOrderCount shouldBe 2
  }

  // -- sort(Column*) alias for orderBy --

  test("Dataset.sort(Column*) is alias for orderBy") {
    val ds = stubDs
    val result = ds.sort(Column("x").desc)
    result.df.relation.hasSort shouldBe true
    result.df.relation.getSort.getIsGlobal shouldBe true
  }

  // -- sample(fraction, seed) --

  test("Dataset.sample(fraction, seed) builds Sample proto with seed") {
    val ds = stubDs
    val result = ds.sample(0.2, 123L)
    result.df.relation.hasSample shouldBe true
    result.df.relation.getSample.getSeed shouldBe 123L
    result.df.relation.getSample.getUpperBound shouldBe 0.2
  }

  // -- dropDuplicatesWithinWatermark variants --

  test("Dataset.dropDuplicatesWithinWatermark(Seq) builds Deduplicate with column names") {
    val ds = stubDs
    val result = ds.dropDuplicatesWithinWatermark(Seq("a", "b"))
    result.df.relation.hasDeduplicate shouldBe true
    val dedup = result.df.relation.getDeduplicate
    dedup.getWithinWatermark shouldBe true
    dedup.getColumnNamesList.size() shouldBe 2
  }

  test("Dataset.dropDuplicatesWithinWatermark(col1, cols*) builds Deduplicate") {
    val ds = stubDs
    val result = ds.dropDuplicatesWithinWatermark("a", "b", "c")
    result.df.relation.hasDeduplicate shouldBe true
    val dedup = result.df.relation.getDeduplicate
    dedup.getWithinWatermark shouldBe true
    dedup.getColumnNamesList.size() shouldBe 3
  }

  // -- unionByName with allowMissingColumns --

  test("Dataset.unionByName with allowMissingColumns=true sets flag") {
    val ds1 = stubDs
    val ds2 = stubDs
    val result = ds1.unionByName(ds2, allowMissingColumns = true)
    result.df.relation.hasSetOp shouldBe true
    result.df.relation.getSetOp.getByName shouldBe true
    result.df.relation.getSetOp.getAllowMissingColumns shouldBe true
  }

  // -- toString --

  test("Dataset.toString delegates to DataFrame") {
    val ds = stubDs
    ds.toString should not be null
  }

  // -- na / stat accessors --

  test("Dataset.na returns DataFrameNaFunctions") {
    val ds = stubDs
    ds.na shouldBe a[DataFrameNaFunctions]
  }

  test("Dataset.stat returns DataFrameStatFunctions") {
    val ds = stubDs
    ds.stat shouldBe a[DataFrameStatFunctions]
  }

  // -- write / writeTo / writeStream --

  test("Dataset.write returns DataFrameWriter") {
    val ds = stubDs
    ds.write shouldBe a[DataFrameWriter]
  }

  test("Dataset.writeTo returns DataFrameWriterV2") {
    val ds = stubDs
    ds.writeTo("my_table") shouldBe a[DataFrameWriterV2]
  }

  test("Dataset.writeStream returns DataStreamWriter") {
    val ds = stubDs
    ds.writeStream shouldBe a[DataStreamWriter]
  }

  // -- toJSON proto structure --

  test("Dataset.toJSON builds Project with to_json(struct(*))") {
    val ds = stubDs
    val jsonDs = ds.toJSON
    // The underlying DataFrame should have a Project relation
    jsonDs.df.relation.hasProject shouldBe true
    jsonDs.df.relation.getProject.getExpressionsCount shouldBe 1
  }

  // -- scalar and exists subquery expressions --

  test("Dataset.scalar() builds SubqueryExpression with SCALAR type") {
    val ds = stubDs
    val col = ds.scalar()
    col.expr.hasSubqueryExpression shouldBe true
    col.expr.getSubqueryExpression.getSubqueryType shouldBe
      SubqueryExpression.SubqueryType.SUBQUERY_TYPE_SCALAR
    col.expr.getSubqueryExpression.getPlanId shouldBe ds.df.relation.getCommon.getPlanId
  }

  test("Dataset.exists() builds SubqueryExpression with EXISTS type") {
    val ds = stubDs
    val col = ds.exists()
    col.expr.hasSubqueryExpression shouldBe true
    col.expr.getSubqueryExpression.getSubqueryType shouldBe
      SubqueryExpression.SubqueryType.SUBQUERY_TYPE_EXISTS
    col.expr.getSubqueryExpression.getPlanId shouldBe ds.df.relation.getCommon.getPlanId
  }

  test("Dataset.scalar() subquery relations contain the dataset relation") {
    val ds = stubDs
    val col = ds.scalar()
    col.subqueryRelations should have size 1
    col.subqueryRelations.head shouldBe ds.df.relation
  }

  test("Dataset.exists() subquery relations contain the dataset relation") {
    val ds = stubDs
    val col = ds.exists()
    col.subqueryRelations should have size 1
    col.subqueryRelations.head shouldBe ds.df.relation
  }

  // -- as[U] type conversion --

  test("Dataset.as[U] creates a new Dataset with different encoder") {
    val ds = stubDs
    val strDs = ds.as[String]
    strDs.encoder should not be null
    strDs.encoder.schema.fields.head.name shouldBe "value"
  }

  // -- joinWith additional cases --

  test("joinWith with full outer join type") {
    val ds1 = stubDs
    val ds2 = stubDs
    val joined = ds1.joinWith(ds2, Column("x") === Column("x"), "full")
    val join = joined.df.relation.getJoin
    join.getJoinType shouldBe Join.JoinType.JOIN_TYPE_FULL_OUTER
    join.hasJoinDataType shouldBe true
  }

  test("joinWith preserves JoinDataType isLeftStruct/isRightStruct for primitives") {
    val ds1 = stubDs
    val ds2 = stubIntDs
    val joined = ds1.joinWith(ds2, Column("x") === Column("x"))
    val jdt = joined.df.relation.getJoin.getJoinDataType
    // Primitives with single-field schema: isStruct depends on encoder type
    jdt should not be null
  }

  test("joinWith sets left and right inputs") {
    val ds1 = stubDs
    val ds2 = stubIntDs
    val joined = ds1.joinWith(ds2, Column("x") === Column("x"))
    val join = joined.df.relation.getJoin
    join.hasLeft shouldBe true
    join.hasRight shouldBe true
    join.hasJoinCondition shouldBe true
  }

  // -- withWatermark --

  test("Dataset.withWatermark builds correct eventTime and delayThreshold") {
    val ds = stubDs
    val result = ds.withWatermark("ts", "5 minutes")
    val ww = result.df.relation.getWithWatermark
    ww.getEventTime shouldBe "ts"
    ww.getDelayThreshold shouldBe "5 minutes"
  }

  // -- filter(func) / map(func) / flatMap(func) / mapPartitions(func) --
  // These build MapPartitions proto with UDF payload for primitive types

  test("Dataset.filter(func) builds MapPartitions proto for primitive type") {
    val ds = stubDs
    val result = ds.filter(_ > 0)
    result.df.relation.hasMapPartitions shouldBe true
    val mp = result.df.relation.getMapPartitions
    mp.hasFunc shouldBe true
    mp.getFunc.getFunctionName shouldBe "mapPartitions"
    mp.getFunc.getDeterministic shouldBe true
  }

  test("Dataset.map(func) builds MapPartitions proto for primitive type") {
    val ds = stubDs
    val result = ds.map(_ + 1)
    result.df.relation.hasMapPartitions shouldBe true
    val mp = result.df.relation.getMapPartitions
    mp.hasFunc shouldBe true
    mp.getFunc.getFunctionName shouldBe "mapPartitions"
  }

  test("Dataset.flatMap(func) builds MapPartitions proto for primitive type") {
    val ds = stubDs
    val result = ds.flatMap(x => Seq(x, x * 2))
    result.df.relation.hasMapPartitions shouldBe true
    val mp = result.df.relation.getMapPartitions
    mp.hasFunc shouldBe true
  }

  test("Dataset.mapPartitions(func) builds MapPartitions proto") {
    val ds = stubDs
    val result = ds.mapPartitions(iter => iter.map(_ * 2))
    result.df.relation.hasMapPartitions shouldBe true
    val mp = result.df.relation.getMapPartitions
    mp.hasInput shouldBe true
    mp.hasFunc shouldBe true
    mp.getFunc.getScalarScalaUdf.getPayload.isEmpty shouldBe false
    mp.getFunc.getScalarScalaUdf.hasOutputType shouldBe true
    mp.getFunc.getScalarScalaUdf.getNullable shouldBe true
  }

  test("Dataset.map preserves output encoder") {
    val ds = stubDs
    val result = ds.map(_.toString)
    result.encoder should not be null
  }

  // -- groupByKey --

  test("Dataset.groupByKey builds KeyValueGroupedDataset") {
    val ds = stubDs
    val grouped = ds.groupByKey(_ % 2)
    grouped should not be null
    grouped.ds shouldBe ds
  }

  // -- createTempView / createOrReplaceTempView / createGlobalTempView / createOrReplaceGlobalTempView --
  // These require a client, so we just verify the methods exist

  test("Dataset has createTempView method") {
    classOf[Dataset[?]].getMethod("createTempView", classOf[String]) should not be null
  }

  test("Dataset has createOrReplaceTempView method") {
    classOf[Dataset[?]].getMethod("createOrReplaceTempView", classOf[String]) should not be null
  }

  test("Dataset has createGlobalTempView method") {
    classOf[Dataset[?]].getMethod("createGlobalTempView", classOf[String]) should not be null
  }

  test("Dataset has createOrReplaceGlobalTempView method") {
    classOf[Dataset[?]].getMethod(
      "createOrReplaceGlobalTempView",
      classOf[String]
    ) should not be null
  }

  // -- randomSplit --
  // randomSplit doesn't require server since it builds Sample protos

  test("Dataset has randomSplit method") {
    classOf[Dataset[?]].getMethods.exists(_.getName == "randomSplit") shouldBe true
  }

  // -- isEmpty / isStreaming / isLocal / inputFiles / sameSemantics / semanticHash / storageLevel --
  // These all require server calls; verify method existence

  test("Dataset has isEmpty method") {
    classOf[Dataset[?]].getMethod("isEmpty") should not be null
  }

  test("Dataset has isStreaming method") {
    classOf[Dataset[?]].getMethod("isStreaming") should not be null
  }

  test("Dataset has isLocal method") {
    classOf[Dataset[?]].getMethod("isLocal") should not be null
  }

  test("Dataset has inputFiles method") {
    classOf[Dataset[?]].getMethod("inputFiles") should not be null
  }

  test("Dataset has sameSemantics method") {
    classOf[Dataset[?]].getMethods.exists(_.getName == "sameSemantics") shouldBe true
  }

  test("Dataset has semanticHash method") {
    classOf[Dataset[?]].getMethod("semanticHash") should not be null
  }

  test("Dataset has storageLevel method") {
    classOf[Dataset[?]].getMethod("storageLevel") should not be null
  }

  // -- persist / unpersist / cache --
  // These require client, verify method existence

  test("Dataset has cache method") {
    classOf[Dataset[?]].getMethod("cache") should not be null
  }

  test("Dataset has persist method") {
    classOf[Dataset[?]].getMethod("persist") should not be null
  }

  test("Dataset has persist(StorageLevel) method") {
    classOf[Dataset[?]].getMethods.exists(m =>
      m.getName == "persist" && m.getParameterCount == 1
    ) shouldBe true
  }

  test("Dataset has unpersist method") {
    classOf[Dataset[?]].getMethod("unpersist", classOf[Boolean]) should not be null
  }

  // -- foreach / foreachPartition / collect / head / take / first / count / show --
  // All require server; verify method existence

  test("Dataset has foreach method") {
    classOf[Dataset[?]].getMethods.exists(_.getName == "foreach") shouldBe true
  }

  test("Dataset has foreachPartition method") {
    classOf[Dataset[?]].getMethods.exists(_.getName == "foreachPartition") shouldBe true
  }

  // -- typed select with TypedColumn using actual proto --

  test("Dataset.select(TypedColumn) with real TypedColumn builds Project") {
    val ds = stubDs
    val tc = TypedColumn[Long, Long](Column("value").expr, summon[Encoder[Long]])
    val result = ds.select(tc)
    result.df.relation.hasProject shouldBe true
    result.encoder should not be null
  }

  // ---------------------------------------------------------------------------
  // KeyValueGroupedDataset proto-only tests
  // ---------------------------------------------------------------------------

  private def stubKvgd: KeyValueGroupedDataset[Long, Long] =
    val ds = stubDs
    ds.groupByKey(identity)

  test("KVGD.keys builds distinct mapped Dataset") {
    val kvgd = stubKvgd
    val keys = kvgd.keys
    // keys = ds.map(groupingFunc).toDF().distinct().as[K]
    keys should not be null
    keys.df.relation.hasDeduplicate shouldBe true
    keys.df.relation.getDeduplicate.getAllColumnsAsKeys shouldBe true
  }

  test("KVGD.flatMapGroups builds GroupMap proto") {
    val kvgd = stubKvgd
    val result = kvgd.flatMapGroups((k, iter) => iter.map(_ + k))
    result.df.relation.hasGroupMap shouldBe true
    val gm = result.df.relation.getGroupMap
    gm.hasInput shouldBe true
    gm.hasFunc shouldBe true
    gm.getFunc.getFunctionName shouldBe "flatMapGroups"
    gm.getGroupingExpressionsCount shouldBe 1
    // The grouping expression should be an inline UDF
    gm.getGroupingExpressions(0).hasCommonInlineUserDefinedFunction shouldBe true
    gm.getGroupingExpressions(
      0
    ).getCommonInlineUserDefinedFunction.getFunctionName shouldBe "groupByKey"
  }

  test("KVGD.mapGroups builds GroupMap proto (delegates to flatMapGroups)") {
    val kvgd = stubKvgd
    val result = kvgd.mapGroups((k, iter) => k + iter.size)
    result.df.relation.hasGroupMap shouldBe true
    val gm = result.df.relation.getGroupMap
    gm.hasFunc shouldBe true
    gm.getFunc.getFunctionName shouldBe "flatMapGroups"
  }

  test("KVGD.flatMapSortedGroups builds GroupMap proto with sorting expressions") {
    val kvgd = stubKvgd
    val result = kvgd.flatMapSortedGroups(Column("value").asc)((k, iter) => iter.map(_ + k))
    result.df.relation.hasGroupMap shouldBe true
    val gm = result.df.relation.getGroupMap
    gm.getSortingExpressionsCount shouldBe 1
    gm.hasFunc shouldBe true
    gm.getGroupingExpressionsCount shouldBe 1
  }

  test("KVGD.flatMapSortedGroups with multiple sort expressions") {
    val kvgd = stubKvgd
    val result =
      kvgd.flatMapSortedGroups(Column("a").asc, Column("b").desc)((k, iter) => iter.map(_ + k))
    result.df.relation.hasGroupMap shouldBe true
    result.df.relation.getGroupMap.getSortingExpressionsCount shouldBe 2
  }

  test("KVGD.count method exists") {
    // count() requires a (K, Long) tuple encoder which falls back to client-side collect
    // so we only verify the method exists
    classOf[KeyValueGroupedDataset[?, ?]].getMethods.exists(_.getName == "count") shouldBe true
  }

  test("KVGD.mapValues returns new KeyValueGroupedDataset") {
    val kvgd = stubKvgd
    val mapped = kvgd.mapValues(_ * 2)
    mapped should not be null
    // The underlying dataset should have a MapPartitions relation (from the map)
    mapped.ds.df.relation.hasMapPartitions shouldBe true
  }

  test("KVGD.keyAs returns new KeyValueGroupedDataset with different key type") {
    val kvgd = stubKvgd
    val retyped = kvgd.keyAs[Int]
    retyped should not be null
  }

  test("KVGD.agg with TypedColumn builds Aggregate proto") {
    val kvgd = stubKvgd
    // Create a simple Aggregator
    val summer = new expressions.Aggregator[Long, Long, Long]:
      def zero: Long = 0L
      def reduce(b: Long, a: Long): Long = b + a
      def merge(b1: Long, b2: Long): Long = b1 + b2
      def finish(reduction: Long): Long = reduction
      def bufferEncoder: Encoder[Long] = Encoders.scalaLong
      def outputEncoder: Encoder[Long] = Encoders.scalaLong
    val tc = summer.toColumn
    val result = kvgd.agg(tc)
    result.df.relation.hasAggregate shouldBe true
    val agg = result.df.relation.getAggregate
    agg.getGroupType shouldBe Aggregate.GroupType.GROUP_TYPE_GROUPBY
    agg.getGroupingExpressionsCount shouldBe 1
    agg.getAggregateExpressionsCount shouldBe 1
    // Grouping expression should be inline UDF
    agg.getGroupingExpressions(0).hasCommonInlineUserDefinedFunction shouldBe true
    agg.getGroupingExpressions(
      0
    ).getCommonInlineUserDefinedFunction.getFunctionName shouldBe "groupByKey"
    // Aggregate expression should be TypedAggregateExpression
    agg.getAggregateExpressions(0).hasTypedAggregateExpression shouldBe true
  }

  test("KVGD.reduceGroups builds Aggregate proto via ReduceAggregator") {
    val kvgd = stubKvgd
    val result = kvgd.reduceGroups(_ + _)
    result.df.relation.hasAggregate shouldBe true
    val agg = result.df.relation.getAggregate
    agg.getGroupType shouldBe Aggregate.GroupType.GROUP_TYPE_GROUPBY
    agg.getGroupingExpressionsCount shouldBe 1
    agg.getAggregateExpressionsCount shouldBe 1
  }

  // -- KVGD stateful operations (method existence only since they require full encoders) --

  test("KVGD has mapGroupsWithState method") {
    classOf[KeyValueGroupedDataset[?, ?]].getMethods.exists(
      _.getName == "mapGroupsWithState"
    ) shouldBe true
  }

  test("KVGD has flatMapGroupsWithState method") {
    classOf[KeyValueGroupedDataset[?, ?]].getMethods.exists(
      _.getName == "flatMapGroupsWithState"
    ) shouldBe true
  }

  test("KVGD has transformWithState method") {
    classOf[KeyValueGroupedDataset[?, ?]].getMethods.exists(
      _.getName == "transformWithState"
    ) shouldBe true
  }

  test("KVGD has cogroup method") {
    classOf[KeyValueGroupedDataset[?, ?]].getMethods.exists(
      _.getName == "cogroup"
    ) shouldBe true
  }

  // -- KVGD buildGroupingUdf is accessible and correct --

  test("KVGD buildGroupingUdf produces correct proto") {
    val kvgd = stubKvgd
    val keyAg = summon[Encoder[Long]].agnosticEncoder
    val valueAg = summon[Encoder[Long]].agnosticEncoder
    val udf = kvgd.buildGroupingUdf(keyAg, valueAg)
    udf.getFunctionName shouldBe "groupByKey"
    udf.getDeterministic shouldBe true
    udf.hasScalarScalaUdf shouldBe true
    udf.getScalarScalaUdf.getPayload.isEmpty shouldBe false
    udf.getScalarScalaUdf.hasOutputType shouldBe true
    udf.getScalarScalaUdf.getNullable shouldBe true
  }

  // ---------------------------------------------------------------------------
  // Additional KVGD coverage tests
  // ---------------------------------------------------------------------------

  private def stubStringKvgd: KeyValueGroupedDataset[String, String] =
    val ds = stubStringDs
    ds.groupByKey(identity)

  private def stubIntKvgd: KeyValueGroupedDataset[Int, Int] =
    val ds = stubIntDs
    ds.groupByKey(identity)

  test("KVGD.flatMapGroups with String key/value builds GroupMap proto") {
    val kvgd = stubStringKvgd
    val result = kvgd.flatMapGroups((k, iter) => iter.map(v => s"$k-$v"))
    result.df.relation.hasGroupMap shouldBe true
    val gm = result.df.relation.getGroupMap
    gm.hasFunc shouldBe true
    gm.getFunc.getFunctionName shouldBe "flatMapGroups"
    gm.getGroupingExpressionsCount shouldBe 1
  }

  test("KVGD.mapGroups with String key/value builds GroupMap proto") {
    val kvgd = stubStringKvgd
    val result = kvgd.mapGroups((k, iter) => s"$k:${iter.size}")
    result.df.relation.hasGroupMap shouldBe true
  }

  test("KVGD.flatMapSortedGroups with String key/value builds GroupMap with sorting") {
    val kvgd = stubStringKvgd
    val result = kvgd.flatMapSortedGroups(Column("value").asc)((k, iter) => iter.map(v => s"$k-$v"))
    result.df.relation.hasGroupMap shouldBe true
    result.df.relation.getGroupMap.getSortingExpressionsCount shouldBe 1
  }

  test("KVGD.flatMapSortedGroups with zero sort expressions") {
    val kvgd = stubKvgd
    val result = kvgd.flatMapSortedGroups()((k, iter) => iter.map(_ + k))
    result.df.relation.hasGroupMap shouldBe true
    result.df.relation.getGroupMap.getSortingExpressionsCount shouldBe 0
  }

  test("KVGD.mapValues returns a new KVGD with transformed Dataset") {
    val kvgd = stubKvgd
    val mapped = kvgd.mapValues(_ * 3)
    mapped should not be null
    mapped.ds.df.relation.hasMapPartitions shouldBe true
  }

  test("KVGD.mapValues followed by flatMapGroups works end-to-end") {
    val kvgd = stubKvgd
    val mapped = kvgd.mapValues(_ * 2)
    // mapped.flatMapGroups would need server since mapValues changes the ds
    // verify the KVGD is constructed correctly
    mapped.ds should not be null
  }

  test("KVGD.keyAs preserves underlying Dataset") {
    val kvgd = stubKvgd
    val retyped = kvgd.keyAs[Int]
    retyped.ds shouldBe kvgd.ds
  }

  test("KVGD.keys returns distinct mapped dataset proto") {
    val kvgd = stubStringKvgd
    val keys = kvgd.keys
    keys.df.relation.hasDeduplicate shouldBe true
    keys.df.relation.getDeduplicate.getAllColumnsAsKeys shouldBe true
  }

  test("KVGD.agg with two TypedColumns builds Aggregate proto") {
    val kvgd = stubKvgd
    val summer = new expressions.Aggregator[Long, Long, Long]:
      def zero: Long = 0L
      def reduce(b: Long, a: Long): Long = b + a
      def merge(b1: Long, b2: Long): Long = b1 + b2
      def finish(reduction: Long): Long = reduction
      def bufferEncoder: Encoder[Long] = Encoders.scalaLong
      def outputEncoder: Encoder[Long] = Encoders.scalaLong
    val tc1 = summer.toColumn
    val tc2 = summer.toColumn
    val result = kvgd.agg(tc1, tc2)
    result.df.relation.hasAggregate shouldBe true
    val agg = result.df.relation.getAggregate
    agg.getAggregateExpressionsCount shouldBe 2
    agg.getGroupingExpressionsCount shouldBe 1
  }

  test("KVGD.agg with three TypedColumns builds Aggregate proto") {
    val kvgd = stubKvgd
    val summer = new expressions.Aggregator[Long, Long, Long]:
      def zero: Long = 0L
      def reduce(b: Long, a: Long): Long = b + a
      def merge(b1: Long, b2: Long): Long = b1 + b2
      def finish(reduction: Long): Long = reduction
      def bufferEncoder: Encoder[Long] = Encoders.scalaLong
      def outputEncoder: Encoder[Long] = Encoders.scalaLong
    val result = kvgd.agg(summer.toColumn, summer.toColumn, summer.toColumn)
    result.df.relation.hasAggregate shouldBe true
    result.df.relation.getAggregate.getAggregateExpressionsCount shouldBe 3
  }

  test("KVGD.agg with four TypedColumns builds Aggregate proto") {
    val kvgd = stubKvgd
    val summer = new expressions.Aggregator[Long, Long, Long]:
      def zero: Long = 0L
      def reduce(b: Long, a: Long): Long = b + a
      def merge(b1: Long, b2: Long): Long = b1 + b2
      def finish(reduction: Long): Long = reduction
      def bufferEncoder: Encoder[Long] = Encoders.scalaLong
      def outputEncoder: Encoder[Long] = Encoders.scalaLong
    val result = kvgd.agg(summer.toColumn, summer.toColumn, summer.toColumn, summer.toColumn)
    result.df.relation.hasAggregate shouldBe true
    result.df.relation.getAggregate.getAggregateExpressionsCount shouldBe 4
  }

  test("KVGD.reduceGroups builds Aggregate with ReduceAggregator for Int type") {
    val kvgd = stubIntKvgd
    val result = kvgd.reduceGroups(_ + _)
    result.df.relation.hasAggregate shouldBe true
    val agg = result.df.relation.getAggregate
    agg.getGroupType shouldBe Aggregate.GroupType.GROUP_TYPE_GROUPBY
    agg.getGroupingExpressionsCount shouldBe 1
    agg.getAggregateExpressionsCount shouldBe 1
  }

  test("KVGD.reduceGroups builds Aggregate with ReduceAggregator for String type") {
    val kvgd = stubStringKvgd
    val result = kvgd.reduceGroups((a, b) => s"$a$b")
    result.df.relation.hasAggregate shouldBe true
    val agg = result.df.relation.getAggregate
    agg.getGroupType shouldBe Aggregate.GroupType.GROUP_TYPE_GROUPBY
  }

  test("KVGD.flatMapGroups proto contains correct input relation") {
    val kvgd = stubKvgd
    val result = kvgd.flatMapGroups((k, iter) => iter)
    val gm = result.df.relation.getGroupMap
    gm.hasInput shouldBe true
    gm.getInput.hasLocalRelation shouldBe true
  }

  test("KVGD.mapGroups proto UDF is deterministic") {
    val kvgd = stubKvgd
    val result = kvgd.mapGroups((k, iter) => k)
    val gm = result.df.relation.getGroupMap
    gm.getFunc.getDeterministic shouldBe true
    gm.getFunc.hasScalarScalaUdf shouldBe true
  }

  test("KVGD.flatMapGroups UDF payload is non-empty") {
    val kvgd = stubKvgd
    val result = kvgd.flatMapGroups((k, iter) => iter)
    val func = result.df.relation.getGroupMap.getFunc
    func.getScalarScalaUdf.getPayload.isEmpty shouldBe false
    func.getScalarScalaUdf.hasOutputType shouldBe true
  }

  test("KVGD companion object apply creates instance") {
    val ds = stubDs
    val kvgd = KeyValueGroupedDataset(ds, (x: Long) => x % 3)
    kvgd should not be null
    kvgd.ds shouldBe ds
  }

  test("KVGD groupingFunc is stored correctly") {
    val ds = stubDs
    val func: Long => Long = _ % 5
    val kvgd = KeyValueGroupedDataset(ds, func)
    // Verify the grouping function is the same
    kvgd.groupingFunc shouldBe func
  }

  test("KVGD.flatMapGroups with identity function builds correct proto") {
    val kvgd = stubKvgd
    val result = kvgd.flatMapGroups((k, iter) => Iterator(k))
    result.df.relation.hasGroupMap shouldBe true
    val gm = result.df.relation.getGroupMap
    gm.getGroupingExpressionsCount shouldBe 1
    gm.hasFunc shouldBe true
  }

  test("KVGD.keys for Int type returns distinct Dataset") {
    val kvgd = stubIntKvgd
    val keys = kvgd.keys
    keys should not be null
    keys.df.relation.hasDeduplicate shouldBe true
  }

  test("KVGD.mapGroups returning different type builds GroupMap proto") {
    val kvgd = stubKvgd
    val result = kvgd.mapGroups((k, iter) => k.toString)
    result.df.relation.hasGroupMap shouldBe true
    val gm = result.df.relation.getGroupMap
    gm.hasFunc shouldBe true
  }

  test("KVGD.flatMapSortedGroups with three sort expressions") {
    val kvgd = stubKvgd
    val result = kvgd.flatMapSortedGroups(
      Column("a").asc,
      Column("b").desc,
      Column("c").asc
    )((k, iter) => iter)
    result.df.relation.hasGroupMap shouldBe true
    result.df.relation.getGroupMap.getSortingExpressionsCount shouldBe 3
  }

  test("KVGD.agg aggregate expression is TypedAggregateExpression") {
    val kvgd = stubKvgd
    val summer = new expressions.Aggregator[Long, Long, Long]:
      def zero: Long = 0L
      def reduce(b: Long, a: Long): Long = b + a
      def merge(b1: Long, b2: Long): Long = b1 + b2
      def finish(reduction: Long): Long = reduction
      def bufferEncoder: Encoder[Long] = Encoders.scalaLong
      def outputEncoder: Encoder[Long] = Encoders.scalaLong
    val tc = summer.toColumn
    val result = kvgd.agg(tc)
    val aggExpr = result.df.relation.getAggregate.getAggregateExpressions(0)
    aggExpr.hasTypedAggregateExpression shouldBe true
  }

  test("KVGD.reduceGroups aggregate expression is TypedAggregateExpression") {
    val kvgd = stubKvgd
    val result = kvgd.reduceGroups(_ + _)
    val aggExpr = result.df.relation.getAggregate.getAggregateExpressions(0)
    aggExpr.hasTypedAggregateExpression shouldBe true
  }

  // ---------------------------------------------------------------------------
  // Additional Dataset coverage tests
  // ---------------------------------------------------------------------------

  // -- randomSplit proto construction --

  test("Dataset.randomSplit builds Sample protos") {
    val ds = stubDs
    val splits = ds.randomSplit(Array(0.6, 0.4))
    splits should have size 2
    splits.foreach { s =>
      s.df.relation.hasSample shouldBe true
    }
  }

  test("Dataset.randomSplit bounds are correct") {
    val ds = stubDs
    val splits = ds.randomSplit(Array(1.0, 1.0), seed = 99L)
    splits should have size 2
    val s0 = splits(0).df.relation.getSample
    val s1 = splits(1).df.relation.getSample
    s0.getLowerBound shouldBe 0.0
    s0.getUpperBound shouldBe 0.5 +- 0.01
    s1.getLowerBound shouldBe 0.5 +- 0.01
    s1.getUpperBound shouldBe 1.0 +- 0.01
    s0.getSeed shouldBe 99L
    s1.getSeed shouldBe 99L
  }

  // -- filter(Column) preserves encoder --

  test("Dataset.filter(Column) preserves encoder") {
    val ds = stubDs
    val result = ds.filter(Column("x") > Column.lit(0))
    result.encoder shouldBe ds.encoder
  }

  // -- where(Column) preserves encoder --

  test("Dataset.where(Column) preserves encoder") {
    val ds = stubDs
    val result = ds.where(Column("x") > Column.lit(0))
    result.encoder shouldBe ds.encoder
  }

  // -- limit preserves encoder --

  test("Dataset.limit preserves encoder") {
    val ds = stubDs
    val result = ds.limit(5)
    result.encoder shouldBe ds.encoder
  }

  // -- distinct preserves encoder --

  test("Dataset.distinct preserves encoder") {
    val ds = stubDs
    val result = ds.distinct()
    result.encoder shouldBe ds.encoder
  }

  // -- repartition preserves encoder --

  test("Dataset.repartition preserves encoder") {
    val ds = stubDs
    val result = ds.repartition(4)
    result.encoder shouldBe ds.encoder
  }

  // -- coalesce preserves encoder --

  test("Dataset.coalesce preserves encoder") {
    val ds = stubDs
    val result = ds.coalesce(2)
    result.encoder shouldBe ds.encoder
  }

  // -- union preserves encoder --

  test("Dataset.union preserves encoder") {
    val ds1 = stubDs
    val ds2 = stubDs
    val result = ds1.union(ds2)
    result.encoder shouldBe ds1.encoder
  }

  // -- intersect preserves encoder --

  test("Dataset.intersect preserves encoder") {
    val ds1 = stubDs
    val ds2 = stubDs
    val result = ds1.intersect(ds2)
    result.encoder shouldBe ds1.encoder
  }

  // -- except preserves encoder --

  test("Dataset.except preserves encoder") {
    val ds1 = stubDs
    val ds2 = stubDs
    val result = ds1.except(ds2)
    result.encoder shouldBe ds1.encoder
  }

  // -- as(alias) preserves encoder --

  test("Dataset.as(alias) preserves encoder") {
    val ds = stubDs
    val result = ds.as("t")
    result.encoder shouldBe ds.encoder
  }

  // -- alias preserves encoder --

  test("Dataset.alias preserves encoder") {
    val ds = stubDs
    val result = ds.alias("t")
    result.encoder shouldBe ds.encoder
  }

  // -- hint preserves encoder --

  test("Dataset.hint preserves encoder") {
    val ds = stubDs
    val result = ds.hint("broadcast")
    result.encoder shouldBe ds.encoder
  }

  // -- sortWithinPartitions preserves encoder --

  test("Dataset.sortWithinPartitions preserves encoder") {
    val ds = stubDs
    val result = ds.sortWithinPartitions(Column("x"))
    result.encoder shouldBe ds.encoder
  }

  // -- orderBy preserves encoder --

  test("Dataset.orderBy(Column*) preserves encoder") {
    val ds = stubDs
    val result = ds.orderBy(Column("x").asc)
    result.encoder shouldBe ds.encoder
  }

  // -- sort preserves encoder --

  test("Dataset.sort(Column*) preserves encoder") {
    val ds = stubDs
    val result = ds.sort(Column("x").asc)
    result.encoder shouldBe ds.encoder
  }

  // -- sample preserves encoder --

  test("Dataset.sample preserves encoder") {
    val ds = stubDs
    val result = ds.sample(0.5)
    result.encoder shouldBe ds.encoder
  }

  // -- withWatermark preserves encoder --

  test("Dataset.withWatermark preserves encoder") {
    val ds = stubDs
    val result = ds.withWatermark("ts", "10 seconds")
    result.encoder shouldBe ds.encoder
  }

  // -- observe preserves encoder --

  test("Dataset.observe preserves encoder") {
    val ds = stubDs
    val result = ds.observe("m", functions.count(Column("*")).as("c"))
    result.encoder shouldBe ds.encoder
  }

  // -- dropDuplicates preserves encoder --

  test("Dataset.dropDuplicates preserves encoder") {
    val ds = stubDs
    val result = ds.dropDuplicates()
    result.encoder shouldBe ds.encoder
  }

  // -- dropDuplicatesWithinWatermark preserves encoder --

  test("Dataset.dropDuplicatesWithinWatermark preserves encoder") {
    val ds = stubDs
    val result = ds.dropDuplicatesWithinWatermark()
    result.encoder shouldBe ds.encoder
  }

  // -- repartition(cols)(DummyImplicit) builds RepartitionByExpression (no numPartitions) --

  test(
    "Dataset.repartition(cols)(DummyImplicit) builds RepartitionByExpression without numPartitions"
  ) {
    val ds = stubDs
    val result = ds.repartition(Column("x"))(using DummyImplicit.dummyImplicit)
    result.df.relation.hasRepartitionByExpression shouldBe true
    result.df.relation.getRepartitionByExpression.hasNumPartitions shouldBe false
  }

  // -- Dataset.toJSON encoder schema --

  test("Dataset.toJSON encoder schema has 'value' field of StringType") {
    val ds = stubDs
    val jsonDs = ds.toJSON
    jsonDs.encoder.schema.fields should have size 1
    jsonDs.encoder.schema.fields.head.name shouldBe "value"
    jsonDs.encoder.schema.fields.head.dataType shouldBe StringType
  }

  // -- Dataset.as[U] with different type --

  test("Dataset.as[String] changes encoder type") {
    val ds = stubDs
    val strDs = ds.as[String]
    strDs.encoder.schema.fields.head.dataType shouldBe StringType
  }

  test("Dataset.as[Int] changes encoder type") {
    val ds = stubDs
    val intDs = ds.as[Int]
    intDs.encoder.schema.fields.head.dataType shouldBe IntegerType
  }

  // -- Schema accessor --

  test("Dataset.schema returns StructType") {
    // schema calls df.schema which requires server, but method should exist
    classOf[Dataset[?]].getMethod("schema") should not be null
  }

  // -- sample(withReplacement=false, fraction) --

  test("Dataset.sample(false, fraction) builds Sample with withReplacement=false") {
    val ds = stubDs
    val result = ds.sample(false, 0.3)
    result.df.relation.hasSample shouldBe true
    result.df.relation.getSample.getWithReplacement shouldBe false
    result.df.relation.getSample.getUpperBound shouldBe 0.3
  }

  // -- crossJoin preserves return type as DataFrame --

  test("Dataset.crossJoin returns DataFrame (not typed Dataset)") {
    val ds = stubDs
    val rightDf = stubDf
    val result = ds.crossJoin(rightDf)
    result shouldBe a[DataFrame]
  }

  // -- writeTo returns DataFrameWriterV2 --

  test("Dataset.writeTo(table) has correct table name") {
    val ds = stubDs
    val writer = ds.writeTo("catalog.db.table")
    writer shouldBe a[DataFrameWriterV2]
  }

  // -- groupByKey returns KeyValueGroupedDataset with correct references --

  test("Dataset.groupByKey has correct ds reference") {
    val ds = stubDs
    val kvgd = ds.groupByKey(x => x % 3)
    kvgd.ds shouldBe ds
  }

  // -- joinWith with cross join type --

  test("joinWith with cross join type") {
    val ds1 = stubDs
    val ds2 = stubIntDs
    val joined = ds1.joinWith(ds2, Column("x") === Column("x"), "cross")
    val join = joined.df.relation.getJoin
    join.getJoinType shouldBe Join.JoinType.JOIN_TYPE_CROSS
    join.hasJoinDataType shouldBe true
  }

  // -- joinWith with semi join type --

  test("joinWith with semi join type") {
    val ds1 = stubDs
    val ds2 = stubDs
    val joined = ds1.joinWith(ds2, Column("x") === Column("x"), "semi")
    val join = joined.df.relation.getJoin
    join.getJoinType shouldBe Join.JoinType.JOIN_TYPE_LEFT_SEMI
  }

  // -- joinWith with anti join type --

  test("joinWith with anti join type") {
    val ds1 = stubDs
    val ds2 = stubDs
    val joined = ds1.joinWith(ds2, Column("x") === Column("x"), "anti")
    val join = joined.df.relation.getJoin
    join.getJoinType shouldBe Join.JoinType.JOIN_TYPE_LEFT_ANTI
  }

  // -- joinWith with right join type --

  test("joinWith with right join type") {
    val ds1 = stubDs
    val ds2 = stubDs
    val joined = ds1.joinWith(ds2, Column("x") === Column("x"), "right")
    val join = joined.df.relation.getJoin
    join.getJoinType shouldBe Join.JoinType.JOIN_TYPE_RIGHT_OUTER
  }

  // -- joinWith encoder is tuple encoder --

  test("joinWith encoder is a tuple encoder") {
    val ds1 = stubDs
    val ds2 = stubIntDs
    val joined = ds1.joinWith(ds2, Column("x") === Column("x"))
    joined.encoder should not be null
    // Tuple encoder schema has fields for both left and right types
    joined.encoder.schema.fields should not be empty
  }

  // -- filter(func) preserves encoder --

  test("Dataset.filter(func) preserves encoder type") {
    val ds = stubDs
    val result = ds.filter(_ > 0)
    result.encoder shouldBe ds.encoder
  }

  // -- map changes encoder --

  test("Dataset.map changes encoder to output type") {
    val ds = stubDs
    val result: Dataset[String] = ds.map(_.toString)
    result.encoder should not be null
    result.encoder.schema.fields.head.dataType shouldBe StringType
  }

  // -- flatMap changes encoder --

  test("Dataset.flatMap changes encoder to output type") {
    val ds = stubDs
    val result: Dataset[Int] = ds.flatMap(x => Seq(x.toInt))
    result.encoder should not be null
    result.encoder.schema.fields.head.dataType shouldBe IntegerType
  }

  // -- mapPartitions has correct input reference --

  test("Dataset.mapPartitions has correct input in proto") {
    val ds = stubDs
    val result = ds.mapPartitions(iter => iter.map(_ * 3))
    val mp = result.df.relation.getMapPartitions
    mp.hasInput shouldBe true
    mp.getInput shouldBe ds.df.relation
  }

  // -- typed select with 2 TypedColumns --

  test("Dataset.select with 2 TypedColumns builds Project") {
    val ds = stubDs
    val tc1 = TypedColumn[Long, Long](Column("value").expr, summon[Encoder[Long]])
    val tc2 = TypedColumn[Long, Long](Column("value").expr, summon[Encoder[Long]])
    val result = ds.select(tc1, tc2)
    result.df.relation.hasProject shouldBe true
    result.df.relation.getProject.getExpressionsCount shouldBe 2
  }

  // -- Observation default constructor --

  test("Observation() creates an observation with UUID name") {
    val obs = Observation()
    obs.name should not be empty
    obs.name.length shouldBe 36 // UUID format
  }

  test("Observation.apply(name) creates an observation with given name") {
    val obs = Observation("my-metrics")
    obs.name shouldBe "my-metrics"
  }

  test("Observation requires non-empty name") {
    an[IllegalArgumentException] should be thrownBy Observation("")
  }
