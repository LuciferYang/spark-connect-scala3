package org.apache.spark.sql

import org.apache.spark.sql.streaming.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class KeyValueGroupedDatasetStatefulSuite extends AnyFunSuite with Matchers:

  private def session: SparkSession =
    SparkSession.builder().remote("sc://localhost:15002").build()

  private def dummyStreamDs: Dataset[Long] =
    val spark = session
    val df = spark.readStream.format("rate").load()
    Dataset(df, summon[Encoder[Long]])

  // ---------------------------------------------------------------------------
  // flatMapGroupsWithState proto construction
  // ---------------------------------------------------------------------------

  test("flatMapGroupsWithState builds GroupMap with correct state fields") {
    val ds = dummyStreamDs
    val grouped = ds.groupByKey(identity)
    val func: (Long, Iterator[Long], GroupState[String]) => Iterator[String] =
      (k, iter, state) => Iterator(s"$k:${iter.size}")

    val result = grouped.flatMapGroupsWithState[String, String](
      OutputMode.Update,
      GroupStateTimeout.ProcessingTimeTimeout
    )(func)

    // Verify the proto was built with state fields
    val relation = result.df.relation
    relation.hasGroupMap shouldBe true
    val gm = relation.getGroupMap
    gm.getIsMapGroupsWithState shouldBe false
    gm.getOutputMode shouldBe "Update"
    gm.getTimeoutConf shouldBe "ProcessingTimeTimeout"
    gm.hasStateSchema shouldBe true
    gm.hasTransformWithStateInfo shouldBe false
  }

  test("mapGroupsWithState builds GroupMap with isMapGroupsWithState=true") {
    val ds = dummyStreamDs
    val grouped = ds.groupByKey(identity)
    val func: (Long, Iterator[Long], GroupState[Int]) => String =
      (k, iter, state) => s"$k:${iter.size}"

    val result = grouped.mapGroupsWithState[Int, String](
      GroupStateTimeout.NoTimeout
    )(func)

    val gm = result.df.relation.getGroupMap
    gm.getIsMapGroupsWithState shouldBe true
    gm.getOutputMode shouldBe "Update"
    gm.getTimeoutConf shouldBe "NoTimeout"
    gm.hasStateSchema shouldBe true
  }

  // ---------------------------------------------------------------------------
  // transformWithState proto construction
  // ---------------------------------------------------------------------------

  test("transformWithState builds GroupMap with TransformWithStateInfo") {
    val ds = dummyStreamDs
    val grouped = ds.groupByKey(identity)
    val processor = new StatefulProcessor[Long, Long, String]:
      def handleInputRows(
          key: Long,
          inputRows: Iterator[Long],
          timerValues: TimerValues
      ): Iterator[String] =
        inputRows.map(v => s"$key:$v")

    val result = grouped.transformWithState[String](
      processor,
      TimeMode.ProcessingTime,
      OutputMode.Update
    )

    val relation = result.df.relation
    relation.hasGroupMap shouldBe true
    val gm = relation.getGroupMap
    gm.hasTransformWithStateInfo shouldBe true
    gm.getTransformWithStateInfo.getTimeMode shouldBe "ProcessingTime"
    gm.getOutputMode shouldBe "Update"
    gm.hasIsMapGroupsWithState shouldBe false
  }

  test("transformWithState with eventTimeColumnName sets event_time_column_name") {
    val ds = dummyStreamDs
    val grouped = ds.groupByKey(identity)
    val processor = new StatefulProcessor[Long, Long, String]:
      def handleInputRows(
          key: Long,
          inputRows: Iterator[Long],
          timerValues: TimerValues
      ): Iterator[String] = Iterator.empty

    val result = grouped.transformWithState[String](
      processor,
      "event_time",
      OutputMode.Append
    )

    val gm = result.df.relation.getGroupMap
    gm.hasTransformWithStateInfo shouldBe true
    gm.getTransformWithStateInfo.getTimeMode shouldBe "EventTime"
    gm.getTransformWithStateInfo.getEventTimeColumnName shouldBe "event_time"
    gm.getOutputMode shouldBe "Append"
  }

  test("flatMapGroupsWithState with initial state sets initial_input") {
    val ds = dummyStreamDs
    val grouped = ds.groupByKey(identity)
    val initialDs = dummyStreamDs
    val initialGrouped = initialDs.groupByKey(identity)
    val func: (Long, Iterator[Long], GroupState[Long]) => Iterator[String] =
      (k, iter, state) => Iterator(s"$k")

    val result = grouped.flatMapGroupsWithState[Long, String](
      OutputMode.Append,
      GroupStateTimeout.EventTimeTimeout,
      initialGrouped
    )(func)

    val gm = result.df.relation.getGroupMap
    gm.hasInitialInput shouldBe true
    gm.getInitialGroupingExpressionsCount should be > 0
    gm.getTimeoutConf shouldBe "EventTimeTimeout"
  }
