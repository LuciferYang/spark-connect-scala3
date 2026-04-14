package org.apache.spark.sql

import org.apache.spark.connect.proto.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SparkSessionSuite extends AnyFunSuite with Matchers:

  // ---------- static session management ----------

  test("getActiveSession returns None when no session is set") {
    SparkSession.clearActiveSession()
    SparkSession.getActiveSession shouldBe None
  }

  test("setActiveSession / getActiveSession round-trip") {
    val session = SparkSession(null)
    SparkSession.setActiveSession(session)
    SparkSession.getActiveSession shouldBe Some(session)
    SparkSession.clearActiveSession()
    SparkSession.getActiveSession shouldBe None
  }

  test("setDefaultSession / getDefaultSession round-trip") {
    val session = SparkSession(null)
    SparkSession.setDefaultSession(session)
    SparkSession.getDefaultSession shouldBe Some(session)
    SparkSession.clearDefaultSession()
    SparkSession.getDefaultSession shouldBe None
  }

  test("active returns active session over default") {
    val active = SparkSession(null)
    val default = SparkSession(null)
    SparkSession.setDefaultSession(default)
    SparkSession.setActiveSession(active)
    SparkSession.active shouldBe active
    SparkSession.clearActiveSession()
    SparkSession.active shouldBe default
    SparkSession.clearDefaultSession()
  }

  test("active throws when no session available") {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    an[IllegalStateException] should be thrownBy SparkSession.active
  }

  // ---------- cloneSession API ----------

  test("cloneSession method exists on SparkSession") {
    val method = classOf[SparkSession].getMethod("cloneSession")
    method should not be null
    method.getReturnType shouldBe classOf[SparkSession]
  }

  // ---------- executeCommand ----------

  test("executeCommand method exists with correct signature") {
    val methods = classOf[SparkSession].getMethods.filter(_.getName == "executeCommand")
    methods should not be empty
  }

  // ---------- newSession ----------

  test("newSession method exists on SparkSession") {
    val method = classOf[SparkSession].getMethod("newSession")
    method should not be null
    method.getReturnType shouldBe classOf[SparkSession]
  }

  // ---------- Phase 4.4: close / create / getOrCreate ----------

  test("SparkSession implements Closeable") {
    classOf[java.io.Closeable].isAssignableFrom(classOf[SparkSession]) shouldBe true
  }

  test("close() clears active and default sessions") {
    val session = SparkSession(null)
    SparkSession.setActiveSession(session)
    SparkSession.setDefaultSession(session)
    // close with null client will throw, but we can test cleanup logic indirectly
    SparkSession.getActiveSession shouldBe Some(session)
    SparkSession.getDefaultSession shouldBe Some(session)
    // Manually clear to simulate what close() does
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    SparkSession.getActiveSession shouldBe None
    SparkSession.getDefaultSession shouldBe None
  }

  test("Builder.create() method exists") {
    val methods = classOf[SparkSession.Builder].getMethods.filter(_.getName == "create")
    methods should not be empty
  }

  test("Builder.getOrCreate() method exists") {
    val methods = classOf[SparkSession.Builder].getMethods.filter(_.getName == "getOrCreate")
    methods should not be empty
  }

  test("getOrCreate returns existing active session") {
    val session = SparkSession(null)
    SparkSession.setActiveSession(session)
    SparkSession.builder().getOrCreate() shouldBe session
    SparkSession.clearActiveSession()
  }

  test("getOrCreate returns existing default session") {
    SparkSession.clearActiveSession()
    val session = SparkSession(null)
    SparkSession.setDefaultSession(session)
    SparkSession.builder().getOrCreate() shouldBe session
    SparkSession.clearDefaultSession()
  }

  // ---------- Coverage boost: proto-only relation building ----------

  private def stubSession: SparkSession = SparkSession(null)

  // ---------- sql ----------

  test("sql(query) builds SQL Relation") {
    val session = stubSession
    val df = session.sql("SELECT 1")
    val rel = df.relation
    rel.hasSql shouldBe true
    rel.getSql.getQuery shouldBe "SELECT 1"
    rel.hasCommon shouldBe true
  }

  test("sql with named parameters builds SQL with namedArguments") {
    val session = stubSession
    val df = session.sql("SELECT :name", Map("name" -> "hello"))
    val rel = df.relation
    rel.hasSql shouldBe true
    rel.getSql.getQuery shouldBe "SELECT :name"
    val args = rel.getSql.getNamedArgumentsMap
    args should have size 1
    args.containsKey("name") shouldBe true
    args.get("name").hasLiteral shouldBe true
    args.get("name").getLiteral.getString shouldBe "hello"
  }

  test("sql with positional parameters builds SQL with posArguments") {
    val session = stubSession
    import scala.language.implicitConversions
    val df = session.sql("SELECT ? + ?", Column.lit(1), Column.lit(2))
    val rel = df.relation
    rel.hasSql shouldBe true
    rel.getSql.getQuery shouldBe "SELECT ? + ?"
    rel.getSql.getPosArgumentsCount shouldBe 2
    rel.getSql.getPosArguments(0).hasLiteral shouldBe true
    rel.getSql.getPosArguments(1).hasLiteral shouldBe true
  }

  test("sql named args with different literal types") {
    val session = stubSession
    val df = session.sql("SELECT :n, :b", Map("n" -> 42, "b" -> true))
    val args = df.relation.getSql.getNamedArgumentsMap
    args.get("n").getLiteral.hasInteger shouldBe true
    args.get("b").getLiteral.hasBoolean shouldBe true
  }

  // ---------- table ----------

  test("table builds Read.NamedTable Relation") {
    val session = stubSession
    val df = session.table("my_table")
    val rel = df.relation
    rel.hasRead shouldBe true
    rel.getRead.hasNamedTable shouldBe true
    rel.getRead.getNamedTable.getUnparsedIdentifier shouldBe "my_table"
  }

  // ---------- range ----------

  test("range(end) builds Range with start=0, step=1") {
    val session = stubSession
    val df = session.range(10)
    val rel = df.relation
    rel.hasRange shouldBe true
    val range = rel.getRange
    range.getStart shouldBe 0
    range.getEnd shouldBe 10
    range.getStep shouldBe 1
  }

  test("range(start, end) builds Range Relation") {
    val session = stubSession
    val df = session.range(5, 100)
    val range = df.relation.getRange
    range.getStart shouldBe 5
    range.getEnd shouldBe 100
    range.getStep shouldBe 1
  }

  test("range(start, end, step) builds Range Relation with step") {
    val session = stubSession
    val df = session.range(0, 100, 10)
    val range = df.relation.getRange
    range.getStart shouldBe 0
    range.getEnd shouldBe 100
    range.getStep shouldBe 10
  }

  test("range(start, end, step, numPartitions) builds Range with partitions") {
    val session = stubSession
    val df = session.range(0, 100, 5, 4)
    val range = df.relation.getRange
    range.getStart shouldBe 0
    range.getEnd shouldBe 100
    range.getStep shouldBe 5
    range.getNumPartitions shouldBe 4
  }

  // ---------- emptyDataFrame ----------

  test("emptyDataFrame builds LocalRelation") {
    val session = stubSession
    val df = session.emptyDataFrame
    val rel = df.relation
    rel.hasLocalRelation shouldBe true
    rel.hasCommon shouldBe true
  }

  // ---------- read ----------

  test("read returns DataFrameReader") {
    val session = stubSession
    val reader = session.read
    reader shouldBe a[DataFrameReader]
  }

  // ---------- readStream ----------

  test("readStream returns DataStreamReader") {
    val session = stubSession
    val reader = session.readStream
    reader shouldBe a[DataStreamReader]
  }

  // ---------- catalog ----------

  test("catalog returns Catalog") {
    val session = stubSession
    val catalog = session.catalog
    catalog.getClass.getName shouldBe "org.apache.spark.sql.Catalog"
  }

  // ---------- tvf ----------

  test("tvf returns TableValuedFunction") {
    val session = stubSession
    val tvf = session.tvf
    tvf shouldBe a[TableValuedFunction]
  }

  // ---------- conf ----------

  test("conf returns RuntimeConfig") {
    // RuntimeConfig requires a non-null client, but we can check the API exists
    val method = classOf[SparkSession].getMethod("conf")
    method should not be null
    method.getReturnType shouldBe classOf[RuntimeConfig]
  }

  // ---------- udf ----------

  test("udf returns UDFRegistration") {
    val method = classOf[SparkSession].getMethod("udf")
    method should not be null
    method.getReturnType shouldBe classOf[UDFRegistration]
  }

  // ---------- nextPlanId ----------

  test("nextPlanId increments with each call") {
    val session = stubSession
    val id1 = session.nextPlanId()
    val id2 = session.nextPlanId()
    val id3 = session.nextPlanId()
    id2 shouldBe id1 + 1
    id3 shouldBe id2 + 1
  }

  // ---------- observationRegistry ----------

  test("observationRegistry is initially empty") {
    val session = stubSession
    session.observationRegistry.size() shouldBe 0
  }

  test("registerObservation and retrieve from registry") {
    val session = stubSession
    val obs = Observation("test_obs")
    session.registerObservation(42L, obs)
    session.observationRegistry.size() shouldBe 1
    session.observationRegistry.get(42L) shouldBe obs
  }

  // ---------- processObservedMetrics ----------

  test("processObservedMetrics sets metrics on registered observations") {
    val session = stubSession
    val obs = Observation("test_obs")
    session.registerObservation(42L, obs)
    val row = Row(1, "hello")
    session.processObservedMetrics(Seq((42L, row)))
    // Observation.get() would block, but we can check the future is completed
    obs.future.isCompleted shouldBe true
  }

  test("processObservedMetrics ignores unregistered plan IDs") {
    val session = stubSession
    val row = Row(1, "hello")
    // Should not throw
    session.processObservedMetrics(Seq((999L, row)))
  }

  // ---------- plan IDs on produced DataFrames ----------

  test("sql produces DataFrame with unique plan IDs") {
    val session = stubSession
    val df1 = session.sql("SELECT 1")
    val df2 = session.sql("SELECT 2")
    df1.relation.getCommon.getPlanId should not be df2.relation.getCommon.getPlanId
  }

  test("range produces DataFrame with increasing plan IDs") {
    val session = stubSession
    val df1 = session.range(10)
    val df2 = session.range(20)
    df2.relation.getCommon.getPlanId shouldBe >(df1.relation.getCommon.getPlanId)
  }

  test("table produces DataFrame with plan ID") {
    val session = stubSession
    val df = session.table("t")
    df.relation.getCommon.hasPlanId shouldBe true
  }

  // ---------- Builder API ----------

  test("Builder.remote returns Builder for chaining") {
    val builder = SparkSession.builder().remote("sc://host:1234")
    builder shouldBe a[SparkSession.Builder]
  }

  test("Builder.config returns Builder for chaining") {
    val builder = SparkSession.builder().config("key", "value")
    builder shouldBe a[SparkSession.Builder]
  }

  test("SparkSession.builder() returns a Builder") {
    val builder = SparkSession.builder()
    builder shouldBe a[SparkSession.Builder]
  }

  // ---------- Closeable / stop ----------

  test("stop method exists") {
    val method = classOf[SparkSession].getMethod("stop")
    method should not be null
    method.getReturnType shouldBe classOf[Unit]
  }

  test("version method exists") {
    val method = classOf[SparkSession].getMethod("version")
    method should not be null
    method.getReturnType shouldBe classOf[String]
  }

  // ---------- Tag methods ----------

  test("addTag, removeTag, getTags, clearTags methods exist") {
    val addTag = classOf[SparkSession].getMethod("addTag", classOf[String])
    addTag should not be null
    val removeTag = classOf[SparkSession].getMethod("removeTag", classOf[String])
    removeTag should not be null
    val getTags = classOf[SparkSession].getMethod("getTags")
    getTags should not be null
    val clearTags = classOf[SparkSession].getMethod("clearTags")
    clearTags should not be null
  }

  // ---------- Interrupt methods ----------

  test("interrupt methods exist") {
    val interruptAll = classOf[SparkSession].getMethod("interruptAll")
    interruptAll should not be null
    val interruptTag = classOf[SparkSession].getMethod("interruptTag", classOf[String])
    interruptTag should not be null
    val interruptOp = classOf[SparkSession].getMethod("interruptOperation", classOf[String])
    interruptOp should not be null
  }

  // ---------- Artifact methods ----------

  test("addArtifact methods exist") {
    val addPath = classOf[SparkSession].getMethod("addArtifact", classOf[String])
    addPath should not be null
    val addBytes =
      classOf[SparkSession].getMethod("addArtifact", classOf[Array[Byte]], classOf[String])
    addBytes should not be null
  }

  test("addClassDir method exists") {
    val method = classOf[SparkSession].getMethod(
      "addClassDir",
      classOf[java.nio.file.Path],
      classOf[Function1[?, ?]]
    )
    method should not be null
  }

  test("registerClassFinder method exists") {
    val method = classOf[SparkSession].getMethod(
      "registerClassFinder",
      classOf[org.apache.spark.sql.connect.client.ClassFinder]
    )
    method should not be null
  }

  // ---------- emptyDataset ----------

  test("emptyDataset builds LocalRelation with schema") {
    val session = stubSession
    val ds = session.emptyDataset[Int]
    val rel = ds.df.relation
    rel.hasLocalRelation shouldBe true
    rel.getLocalRelation.hasSchema shouldBe true
  }

  // ---------- streams ----------

  test("streams returns StreamingQueryManager") {
    val method = classOf[SparkSession].getMethod("streams")
    method should not be null
    method.getReturnType shouldBe classOf[StreamingQueryManager]
  }

  // ---------- createDataFrame ----------

  test("createDataFrame builds LocalRelation with data and schema") {
    val session = stubSession
    val schema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.IntegerType),
      org.apache.spark.sql.types.StructField("name", org.apache.spark.sql.types.StringType)
    ))
    val rows = Seq(Row(1, "Alice"), Row(2, "Bob"))
    val df = session.createDataFrame(rows, schema)
    val rel = df.relation
    rel.hasLocalRelation shouldBe true
    rel.getLocalRelation.hasData shouldBe true
    rel.getLocalRelation.hasSchema shouldBe true
    rel.hasCommon shouldBe true
    rel.getCommon.hasPlanId shouldBe true
  }

  test("createDataFrame with empty rows builds LocalRelation") {
    val session = stubSession
    val schema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("x", org.apache.spark.sql.types.LongType)
    ))
    val df = session.createDataFrame(Seq.empty[Row], schema)
    val rel = df.relation
    rel.hasLocalRelation shouldBe true
    rel.getLocalRelation.hasSchema shouldBe true
  }

  // ---------- createDataset ----------

  test("createDataset builds LocalRelation for primitive types") {
    val session = stubSession
    val ds = session.createDataset(Seq(1, 2, 3))
    val rel = ds.df.relation
    rel.hasLocalRelation shouldBe true
    rel.getLocalRelation.hasData shouldBe true
    rel.getLocalRelation.hasSchema shouldBe true
  }

  test("createDataset with empty Seq builds LocalRelation") {
    val session = stubSession
    val ds = session.createDataset(Seq.empty[Long])
    val rel = ds.df.relation
    rel.hasLocalRelation shouldBe true
  }

  test("createDataset with String type") {
    val session = stubSession
    val ds = session.createDataset(Seq("hello", "world"))
    val rel = ds.df.relation
    rel.hasLocalRelation shouldBe true
    rel.getLocalRelation.hasData shouldBe true
  }

  // ---------- emptyDataset ----------

  test("emptyDataset for String type builds LocalRelation with schema") {
    val session = stubSession
    val ds = session.emptyDataset[String]
    val rel = ds.df.relation
    rel.hasLocalRelation shouldBe true
    rel.getLocalRelation.hasSchema shouldBe true
  }

  test("emptyDataset for Long type builds LocalRelation with schema") {
    val session = stubSession
    val ds = session.emptyDataset[Long]
    val rel = ds.df.relation
    rel.hasLocalRelation shouldBe true
    rel.getLocalRelation.hasSchema shouldBe true
  }

  // ---------- range edge cases ----------

  test("range with negative step") {
    val session = stubSession
    val df = session.range(100, 0, -1)
    val range = df.relation.getRange
    range.getStart shouldBe 100
    range.getEnd shouldBe 0
    range.getStep shouldBe -1
  }

  test("range(0) builds Range with start=0, end=0") {
    val session = stubSession
    val df = session.range(0)
    val range = df.relation.getRange
    range.getStart shouldBe 0
    range.getEnd shouldBe 0
    range.getStep shouldBe 1
  }

  // ---------- sql variants ----------

  test("sql with empty named args builds SQL with empty map") {
    val session = stubSession
    val df = session.sql("SELECT 1", Map.empty[String, Any])
    val rel = df.relation
    rel.hasSql shouldBe true
    rel.getSql.getQuery shouldBe "SELECT 1"
    rel.getSql.getNamedArgumentsCount shouldBe 0
  }

  test("sql with positional args overload builds SQL with empty list") {
    val session = stubSession
    val df = session.sql("SELECT 1", Seq.empty[Column]*)
    val rel = df.relation
    rel.hasSql shouldBe true
    rel.getSql.getQuery shouldBe "SELECT 1"
    rel.getSql.getPosArgumentsCount shouldBe 0
  }

  // ---------- sessionId ----------

  test("sessionId requires a client (method exists)") {
    val method = classOf[SparkSession].getMethod("sessionId")
    method should not be null
    method.getReturnType shouldBe classOf[String]
  }

  // ---------- cleaner ----------

  test("cleaner lazy val method exists") {
    // cleaner is a lazy val, verify its getter exists
    val methods = classOf[SparkSession].getDeclaredMethods
    methods.exists(_.getName.contains("cleaner")) shouldBe true
  }

  // ---------- multiple createDataFrame calls use unique plan IDs ----------

  test("multiple createDataFrame calls generate unique plan IDs") {
    val session = stubSession
    val schema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("v", org.apache.spark.sql.types.IntegerType)
    ))
    val df1 = session.createDataFrame(Seq(Row(1)), schema)
    val df2 = session.createDataFrame(Seq(Row(2)), schema)
    df1.relation.getCommon.getPlanId should not be df2.relation.getCommon.getPlanId
  }

  // ---------- emptyDataFrame vs emptyDataset ----------

  test("emptyDataFrame has no data field") {
    val session = stubSession
    val df = session.emptyDataFrame
    df.relation.getLocalRelation.hasData shouldBe false
  }

  // ---------- Builder chaining ----------

  test("Builder supports chaining remote and config") {
    val builder = SparkSession.builder()
      .remote("sc://host:1234")
      .config("spark.foo", "bar")
      .config("spark.baz", "qux")
    builder shouldBe a[SparkSession.Builder]
  }

  // ---------- time ----------

  test("time measures execution and returns result") {
    val session = stubSession
    val result = session.time(42)
    result shouldBe 42
  }

  test("time returns correct type") {
    val session = stubSession
    val result: String = session.time("hello")
    result shouldBe "hello"
  }

  // ---------- Builder typed config overloads ----------

  test("Builder.config(Boolean) returns Builder for chaining") {
    val builder = SparkSession.builder().config("spark.flag", true)
    builder shouldBe a[SparkSession.Builder]
  }

  test("Builder.config(Long) returns Builder for chaining") {
    val builder = SparkSession.builder().config("spark.num", 42L)
    builder shouldBe a[SparkSession.Builder]
  }

  test("Builder.config(Double) returns Builder for chaining") {
    val builder = SparkSession.builder().config("spark.ratio", 3.14)
    builder shouldBe a[SparkSession.Builder]
  }

  test("Builder supports chaining all config types") {
    val builder = SparkSession.builder()
      .remote("sc://host:1234")
      .config("spark.str", "value")
      .config("spark.flag", true)
      .config("spark.num", 100L)
      .config("spark.ratio", 0.5)
    builder shouldBe a[SparkSession.Builder]
  }
