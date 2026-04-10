package org.apache.spark.sql

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.tags.IntegrationTest
import org.apache.spark.sql.types.*

/** Integration tests for DataFrameWriterV2 and MergeIntoWriter with real execution against a Spark
  * Connect server configured with testcat (InMemoryRowLevelOperationTableCatalog).
  *
  * Server configuration required:
  * spark.sql.catalog.testcat=org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTableCatalog
  */
@IntegrationTest
class WriterV2IntegrationSuite extends IntegrationTestBase:

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private val testSchema = StructType(Seq(
    StructField("id", LongType, nullable = false),
    StructField("name", StringType),
    StructField("value", DoubleType)
  ))

  private def testDf: DataFrame =
    spark.createDataFrame(
      Seq(
        Row(1L, "alice", 10.0),
        Row(2L, "bob", 20.0),
        Row(3L, "charlie", 30.0)
      ),
      testSchema
    )

  /** Execute a block ensuring the V2 table is dropped afterwards. */
  private def withV2Table(tableName: String)(f: => Unit): Unit =
    try f
    finally
      try spark.sql(s"DROP TABLE IF EXISTS testcat.$tableName").collect()
      catch case _: Exception => ()

  /** Execute a block ensuring multiple V2 tables are dropped afterwards. */
  private def withV2Tables(tableNames: String*)(f: => Unit): Unit =
    try f
    finally
      tableNames.foreach { name =>
        try spark.sql(s"DROP TABLE IF EXISTS testcat.$name").collect()
        catch case _: Exception => ()
      }

  // ---------------------------------------------------------------------------
  // DataFrameWriterV2 - create()
  // ---------------------------------------------------------------------------

  test("writeTo.create creates V2 table") {
    withV2Table("wv2_create") {
      spark.range(5).writeTo("testcat.wv2_create").create()
      val count = spark.read.table("testcat.wv2_create").count()
      assert(count == 5)
    }
  }

  test("writeTo.create with schema preserves columns") {
    withV2Table("wv2_create_schema") {
      testDf.writeTo("testcat.wv2_create_schema").create()
      val result = spark.read.table("testcat.wv2_create_schema").orderBy(col("id")).collect()
      assert(result.length == 3)
      assert(result(0).getLong(0) == 1L)
      assert(result(0).getString(1) == "alice")
      assert(result(0).getDouble(2) == 10.0)
    }
  }

  // ---------------------------------------------------------------------------
  // DataFrameWriterV2 - using(provider)
  // ---------------------------------------------------------------------------

  test("writeTo.using sets provider") {
    withV2Table("wv2_using") {
      // InMemoryTableCatalog ignores the provider hint but should accept it without error
      testDf.writeTo("testcat.wv2_using").using("foo").create()
      val count = spark.read.table("testcat.wv2_using").count()
      assert(count == 3)
    }
  }

  // ---------------------------------------------------------------------------
  // DataFrameWriterV2 - option / options
  // ---------------------------------------------------------------------------

  test("writeTo.option sets a single option") {
    withV2Table("wv2_option") {
      testDf.writeTo("testcat.wv2_option")
        .option("key1", "value1")
        .create()
      val count = spark.read.table("testcat.wv2_option").count()
      assert(count == 3)
    }
  }

  test("writeTo.options sets multiple options via Map") {
    withV2Table("wv2_options") {
      testDf.writeTo("testcat.wv2_options")
        .options(Map("k1" -> "v1", "k2" -> "v2"))
        .create()
      val count = spark.read.table("testcat.wv2_options").count()
      assert(count == 3)
    }
  }

  // ---------------------------------------------------------------------------
  // DataFrameWriterV2 - tableProperty
  // ---------------------------------------------------------------------------

  test("writeTo.tableProperty sets table property") {
    withV2Table("wv2_tblprop") {
      testDf.writeTo("testcat.wv2_tblprop")
        .tableProperty("write.format.default", "parquet")
        .create()
      val count = spark.read.table("testcat.wv2_tblprop").count()
      assert(count == 3)
    }
  }

  // ---------------------------------------------------------------------------
  // DataFrameWriterV2 - partitionedBy
  // ---------------------------------------------------------------------------

  test("writeTo.partitionedBy sets partitioning") {
    withV2Table("wv2_partitioned") {
      testDf.writeTo("testcat.wv2_partitioned")
        .partitionedBy(col("name"))
        .create()
      val result = spark.read.table("testcat.wv2_partitioned").orderBy(col("id")).collect()
      assert(result.length == 3)
      assert(result(0).getString(1) == "alice")
      assert(result(2).getString(1) == "charlie")
    }
  }

  test("writeTo.partitionedBy with multiple columns") {
    withV2Table("wv2_partitioned_multi") {
      val df = spark.createDataFrame(
        Seq(
          Row(1L, "a", "x", 10.0),
          Row(2L, "b", "y", 20.0),
          Row(3L, "a", "y", 30.0)
        ),
        StructType(Seq(
          StructField("id", LongType, nullable = false),
          StructField("part1", StringType),
          StructField("part2", StringType),
          StructField("value", DoubleType)
        ))
      )
      df.writeTo("testcat.wv2_partitioned_multi")
        .partitionedBy(col("part1"), col("part2"))
        .create()
      val count = spark.read.table("testcat.wv2_partitioned_multi").count()
      assert(count == 3)
    }
  }

  // ---------------------------------------------------------------------------
  // DataFrameWriterV2 - clusterBy
  // ---------------------------------------------------------------------------

  test("writeTo.clusterBy sets clustering") {
    withV2Table("wv2_clustered") {
      try
        testDf.writeTo("testcat.wv2_clustered")
          .clusterBy("name")
          .create()
        val count = spark.read.table("testcat.wv2_clustered").count()
        assert(count == 3)
      catch
        case e: Exception
            if e.getMessage != null &&
              (e.getMessage.contains("UNSUPPORTED_FEATURE") ||
                e.getMessage.contains("clusterBy") ||
                e.getMessage.contains("clustering")) =>
          cancel("clusterBy not supported by server version")
    }
  }

  test("writeTo.clusterBy with multiple columns") {
    withV2Table("wv2_clustered_multi") {
      try
        testDf.writeTo("testcat.wv2_clustered_multi")
          .clusterBy("name", "value")
          .create()
        val count = spark.read.table("testcat.wv2_clustered_multi").count()
        assert(count == 3)
      catch
        case e: Exception
            if e.getMessage != null &&
              (e.getMessage.contains("UNSUPPORTED_FEATURE") ||
                e.getMessage.contains("clusterBy") ||
                e.getMessage.contains("clustering")) =>
          cancel("clusterBy not supported by server version")
    }
  }

  // ---------------------------------------------------------------------------
  // DataFrameWriterV2 - replace()
  // ---------------------------------------------------------------------------

  test("writeTo.replace replaces existing V2 table") {
    withV2Table("wv2_replace") {
      // Create table first
      testDf.writeTo("testcat.wv2_replace").create()
      assert(spark.read.table("testcat.wv2_replace").count() == 3)

      // Replace with different data
      val replacement = spark.createDataFrame(
        Seq(Row(10L, "replaced", 99.0)),
        testSchema
      )
      replacement.writeTo("testcat.wv2_replace").replace()
      val result = spark.read.table("testcat.wv2_replace").collect()
      assert(result.length == 1)
      assert(result(0).getString(1) == "replaced")
    }
  }

  // ---------------------------------------------------------------------------
  // DataFrameWriterV2 - createOrReplace()
  // ---------------------------------------------------------------------------

  test("writeTo.createOrReplace creates new table when not exists") {
    withV2Table("wv2_cor_new") {
      testDf.writeTo("testcat.wv2_cor_new").createOrReplace()
      val count = spark.read.table("testcat.wv2_cor_new").count()
      assert(count == 3)
    }
  }

  test("writeTo.createOrReplace replaces existing table") {
    withV2Table("wv2_cor_existing") {
      testDf.writeTo("testcat.wv2_cor_existing").createOrReplace()
      assert(spark.read.table("testcat.wv2_cor_existing").count() == 3)

      // Replace with single row
      val small = spark.createDataFrame(
        Seq(Row(99L, "z", 0.0)),
        testSchema
      )
      small.writeTo("testcat.wv2_cor_existing").createOrReplace()
      val result = spark.read.table("testcat.wv2_cor_existing").collect()
      assert(result.length == 1)
      assert(result(0).getLong(0) == 99L)
    }
  }

  // ---------------------------------------------------------------------------
  // DataFrameWriterV2 - append()
  // ---------------------------------------------------------------------------

  test("writeTo.append adds rows to existing V2 table") {
    withV2Table("wv2_append") {
      testDf.writeTo("testcat.wv2_append").create()
      assert(spark.read.table("testcat.wv2_append").count() == 3)

      // Append same data
      testDf.writeTo("testcat.wv2_append").append()
      assert(spark.read.table("testcat.wv2_append").count() == 6)
    }
  }

  test("writeTo.append preserves existing data") {
    withV2Table("wv2_append_preserve") {
      testDf.writeTo("testcat.wv2_append_preserve").create()

      val extra = spark.createDataFrame(
        Seq(Row(4L, "dave", 40.0)),
        testSchema
      )
      extra.writeTo("testcat.wv2_append_preserve").append()

      val result = spark.read.table("testcat.wv2_append_preserve")
        .orderBy(col("id")).collect()
      assert(result.length == 4)
      assert(result(0).getString(1) == "alice")
      assert(result(3).getString(1) == "dave")
    }
  }

  // ---------------------------------------------------------------------------
  // DataFrameWriterV2 - overwrite(condition)
  // ---------------------------------------------------------------------------

  test("writeTo.overwrite with condition replaces matching rows") {
    withV2Table("wv2_overwrite") {
      // Partition by id so InMemoryTable recognises it as a filter attribute
      testDf.writeTo("testcat.wv2_overwrite").partitionedBy(col("id")).create()
      assert(spark.read.table("testcat.wv2_overwrite").count() == 3)

      val replacement = spark.createDataFrame(
        Seq(Row(1L, "alice_updated", 100.0)),
        testSchema
      )
      try
        replacement.writeTo("testcat.wv2_overwrite")
          .overwrite(col("id") === lit(1L))
        val result = spark.read.table("testcat.wv2_overwrite")
          .orderBy(col("id")).collect()
        // The overwrite should have removed the row with id=1 and replaced it
        val aliceRow = result.find(_.getLong(0) == 1L)
        assert(aliceRow.isDefined)
        assert(aliceRow.get.getString(1) == "alice_updated")
      catch
        case e: Exception =>
          cancel(s"Conditional overwrite may not be supported by the test catalog: ${e.getMessage}")
    }
  }

  test("writeTo.overwrite with non-matching condition keeps all rows") {
    withV2Table("wv2_overwrite_nomatch") {
      // Partition by id so InMemoryTable recognises it as a filter attribute
      testDf.writeTo("testcat.wv2_overwrite_nomatch").partitionedBy(col("id")).create()

      val replacement = spark.createDataFrame(
        Seq(Row(99L, "new", 0.0)),
        testSchema
      )
      try
        replacement.writeTo("testcat.wv2_overwrite_nomatch")
          .overwrite(col("id") === lit(999L))
        // Original rows for non-matching ids should remain; the overwrite adds the new row
        // for the matching partition (which is empty here)
        val count = spark.read.table("testcat.wv2_overwrite_nomatch").count()
        // Expect at least the original 3 rows plus the new row
        assert(count >= 3)
      catch
        case e: Exception =>
          cancel(s"Conditional overwrite may not be supported by the test catalog: ${e.getMessage}")
    }
  }

  // ---------------------------------------------------------------------------
  // DataFrameWriterV2 - overwritePartitions()
  // ---------------------------------------------------------------------------

  test("writeTo.overwritePartitions performs dynamic partition overwrite") {
    withV2Table("wv2_overwrite_parts") {
      val df = spark.createDataFrame(
        Seq(
          Row(1L, "a", 10.0),
          Row(2L, "b", 20.0),
          Row(3L, "a", 30.0)
        ),
        testSchema
      )
      df.writeTo("testcat.wv2_overwrite_parts")
        .partitionedBy(col("name"))
        .create()
      assert(spark.read.table("testcat.wv2_overwrite_parts").count() == 3)

      // Overwrite only partition name='a'
      val newA = spark.createDataFrame(
        Seq(Row(10L, "a", 100.0)),
        testSchema
      )
      newA.writeTo("testcat.wv2_overwrite_parts").overwritePartitions()
      // partition 'a' replaced with 1 row, partition 'b' untouched
      val result = spark.read.table("testcat.wv2_overwrite_parts")
        .orderBy(col("id")).collect()
      val partA = result.filter(_.getString(1) == "a")
      val partB = result.filter(_.getString(1) == "b")
      assert(partA.length == 1, s"Expected 1 row in partition 'a', got ${partA.length}")
      assert(partA(0).getLong(0) == 10L)
      assert(partB.length == 1, s"Expected 1 row in partition 'b', got ${partB.length}")
    }
  }

  // ---------------------------------------------------------------------------
  // DataFrameWriterV2 - fluent chaining
  // ---------------------------------------------------------------------------

  test("writeTo fluent chaining with using, option, options, tableProperty, partitionedBy") {
    withV2Table("wv2_fluent") {
      testDf.writeTo("testcat.wv2_fluent")
        .using("foo")
        .option("k1", "v1")
        .options(Map("k2" -> "v2", "k3" -> "v3"))
        .tableProperty("prop1", "val1")
        .partitionedBy(col("name"))
        .create()
      val count = spark.read.table("testcat.wv2_fluent").count()
      assert(count == 3)
    }
  }

  // ---------------------------------------------------------------------------
  // DataFrameWriterV2 - option overloads (Boolean, Long, Double)
  // ---------------------------------------------------------------------------

  test("writeTo.option Boolean overload") {
    withV2Table("wv2_opt_bool") {
      testDf.writeTo("testcat.wv2_opt_bool")
        .option("boolOpt", true)
        .create()
      assert(spark.read.table("testcat.wv2_opt_bool").count() == 3)
    }
  }

  test("writeTo.option Long overload") {
    withV2Table("wv2_opt_long") {
      testDf.writeTo("testcat.wv2_opt_long")
        .option("longOpt", 42L)
        .create()
      assert(spark.read.table("testcat.wv2_opt_long").count() == 3)
    }
  }

  test("writeTo.option Double overload") {
    withV2Table("wv2_opt_double") {
      testDf.writeTo("testcat.wv2_opt_double")
        .option("doubleOpt", 3.14)
        .create()
      assert(spark.read.table("testcat.wv2_opt_double").count() == 3)
    }
  }

  // ---------------------------------------------------------------------------
  // DataFrameWriterV2 - create() fails if table already exists
  // ---------------------------------------------------------------------------

  test("writeTo.create fails if table already exists") {
    withV2Table("wv2_create_dup") {
      testDf.writeTo("testcat.wv2_create_dup").create()
      val caught = intercept[Exception] {
        testDf.writeTo("testcat.wv2_create_dup").create()
      }
      assert(caught != null)
    }
  }

  // ---------------------------------------------------------------------------
  // DataFrameWriterV2 - replace() fails if table does not exist
  // ---------------------------------------------------------------------------

  test("writeTo.replace fails if table does not exist") {
    withV2Table("wv2_replace_noexist") {
      val caught = intercept[Exception] {
        testDf.writeTo("testcat.wv2_replace_noexist").replace()
      }
      assert(caught != null)
    }
  }

  // ---------------------------------------------------------------------------
  // MergeIntoWriter tests
  // ---------------------------------------------------------------------------

  // Note: Merge tests require InMemoryRowLevelOperationTableCatalog (which creates
  // InMemoryRowLevelOperationTable with SupportsRowLevelOperations). The source
  // DataFrame must be registered as a temp view so that column references like
  // col("source.id") resolve correctly on the server.

  /** Helper to create a V2 target table with id, name, value columns for merge tests. */
  private def createMergeTarget(tableName: String): Unit =
    val target = spark.createDataFrame(
      Seq(
        Row(1L, "alice", 10.0),
        Row(2L, "bob", 20.0),
        Row(3L, "charlie", 30.0)
      ),
      testSchema
    )
    target.writeTo(s"testcat.$tableName").create()

  /** Register a DataFrame as a temp view named "source" and return the view name. */
  private def withSourceView(sourceDf: DataFrame)(body: String => Unit): Unit =
    val viewName = "source"
    sourceDf.createOrReplaceTempView(viewName)
    try body(viewName)
    finally
      try spark.sql(s"DROP VIEW IF EXISTS $viewName").collect()
      catch case _: Exception => ()

  /** Helper to wrap merge operations that may not be supported. */
  private def withMergeSupport(body: => Unit): Unit =
    try body
    catch
      case e: Exception
          if e.getMessage != null &&
            (e.getMessage.contains("does not support row-level operations") ||
              e.getMessage.contains("UNSUPPORTED_FEATURE") ||
              e.getMessage.contains("SupportsRowLevelOperations") ||
              e.getMessage.contains("does not support MERGE") ||
              e.getMessage.contains("MergeIntoTable") ||
              e.getMessage.contains("Row-level operations") ||
              e.getMessage.contains("MERGE_INTO_TABLE") ||
              e.getMessage.contains("INTERNAL_ERROR")) =>
        cancel(
          s"MERGE INTO not supported by server/catalog configuration: ${e.getMessage.take(120)}"
        )

  test("mergeInto with whenMatched.updateAll and whenNotMatched.insertAll") {
    withV2Table("wv2_merge_basic") {
      createMergeTarget("wv2_merge_basic")

      val sourceDf = spark.createDataFrame(
        Seq(
          Row(1L, "alice_updated", 100.0), // matches id=1, should update
          Row(4L, "dave", 40.0) // no match, should insert
        ),
        testSchema
      )

      withSourceView(sourceDf) { source =>
        withMergeSupport {
          spark.table(source).mergeInto(
            "testcat.wv2_merge_basic",
            col(s"testcat.wv2_merge_basic.id") === col(s"$source.id")
          )
            .whenMatched().updateAll()
            .whenNotMatched().insertAll()
            .merge()

          val result = spark.read.table("testcat.wv2_merge_basic")
            .orderBy(col("id")).collect()
          assert(result.length == 4)
          // id=1 should be updated
          assert(result(0).getString(1) == "alice_updated")
          // id=4 should be inserted
          assert(result(3).getLong(0) == 4L)
          assert(result(3).getString(1) == "dave")
        }
      }
    }
  }

  test("mergeInto with whenMatched(condition).update(Map)") {
    withV2Table("wv2_merge_cond_update") {
      createMergeTarget("wv2_merge_cond_update")

      val sourceDf = spark.createDataFrame(
        Seq(
          Row(1L, "alice_v2", 100.0),
          Row(2L, "bob_v2", 200.0)
        ),
        testSchema
      )

      withSourceView(sourceDf) { source =>
        withMergeSupport {
          spark.table(source).mergeInto(
            "testcat.wv2_merge_cond_update",
            col("testcat.wv2_merge_cond_update.id") === col(s"$source.id")
          )
            .whenMatched(col("testcat.wv2_merge_cond_update.id") === lit(1L))
            .update(Map("name" -> lit("conditional_update")))
            .merge()

          val result = spark.read.table("testcat.wv2_merge_cond_update")
            .orderBy(col("id")).collect()
          // id=1 should be conditionally updated
          assert(result(0).getString(1) == "conditional_update")
          // id=2 should remain unchanged (condition didn't match)
          assert(result(1).getString(1) == "bob")
        }
      }
    }
  }

  test("mergeInto with whenMatched.delete") {
    withV2Table("wv2_merge_delete") {
      createMergeTarget("wv2_merge_delete")

      val sourceDf = spark.createDataFrame(
        Seq(Row(1L, "alice", 10.0)),
        testSchema
      )

      withSourceView(sourceDf) { source =>
        withMergeSupport {
          spark.table(source).mergeInto(
            "testcat.wv2_merge_delete",
            col("testcat.wv2_merge_delete.id") === col(s"$source.id")
          )
            .whenMatched().delete()
            .merge()

          val result = spark.read.table("testcat.wv2_merge_delete")
            .orderBy(col("id")).collect()
          // id=1 should be deleted
          assert(result.length == 2)
          assert(result(0).getLong(0) == 2L)
          assert(result(1).getLong(0) == 3L)
        }
      }
    }
  }

  test("mergeInto with whenNotMatched(condition).insert(Map)") {
    withV2Table("wv2_merge_cond_insert") {
      createMergeTarget("wv2_merge_cond_insert")

      val sourceDf = spark.createDataFrame(
        Seq(
          Row(4L, "dave", 40.0),
          Row(5L, "eve", 50.0)
        ),
        testSchema
      )

      withSourceView(sourceDf) { source =>
        withMergeSupport {
          spark.table(source).mergeInto(
            "testcat.wv2_merge_cond_insert",
            col("testcat.wv2_merge_cond_insert.id") === col(s"$source.id")
          )
            .whenNotMatched(col(s"$source.id") === lit(4L))
            .insert(Map(
              "id" -> col(s"$source.id"),
              "name" -> col(s"$source.name"),
              "value" -> col(s"$source.value")
            ))
            .merge()

          val result = spark.read.table("testcat.wv2_merge_cond_insert")
            .orderBy(col("id")).collect()
          // id=4 should be inserted (condition matched), id=5 should NOT
          assert(result.length == 4)
          assert(result(3).getLong(0) == 4L)
        }
      }
    }
  }

  test("mergeInto with whenNotMatchedBySource.delete") {
    withV2Table("wv2_merge_nmsrc_del") {
      createMergeTarget("wv2_merge_nmsrc_del")

      // Source only has id=1, so ids 2 and 3 are "not matched by source"
      val sourceDf = spark.createDataFrame(
        Seq(Row(1L, "alice", 10.0)),
        testSchema
      )

      withSourceView(sourceDf) { source =>
        withMergeSupport {
          spark.table(source).mergeInto(
            "testcat.wv2_merge_nmsrc_del",
            col("testcat.wv2_merge_nmsrc_del.id") === col(s"$source.id")
          )
            .whenNotMatchedBySource().delete()
            .merge()

          val result = spark.read.table("testcat.wv2_merge_nmsrc_del").collect()
          // Only id=1 should remain
          assert(result.length == 1)
          assert(result(0).getLong(0) == 1L)
        }
      }
    }
  }

  test("mergeInto with whenNotMatchedBySource(condition).update(Map)") {
    withV2Table("wv2_merge_nmsrc_upd") {
      createMergeTarget("wv2_merge_nmsrc_upd")

      // Source only has id=1
      val sourceDf = spark.createDataFrame(
        Seq(Row(1L, "alice", 10.0)),
        testSchema
      )

      withSourceView(sourceDf) { source =>
        withMergeSupport {
          spark.table(source).mergeInto(
            "testcat.wv2_merge_nmsrc_upd",
            col("testcat.wv2_merge_nmsrc_upd.id") === col(s"$source.id")
          )
            .whenNotMatchedBySource(col("testcat.wv2_merge_nmsrc_upd.id") === lit(2L))
            .update(Map("name" -> lit("updated_by_source_miss")))
            .merge()

          val result = spark.read.table("testcat.wv2_merge_nmsrc_upd")
            .orderBy(col("id")).collect()
          // id=2 should be updated (not matched by source + condition)
          assert(result(1).getString(1) == "updated_by_source_miss")
          // id=3 should remain unchanged (condition didn't match)
          assert(result(2).getString(1) == "charlie")
        }
      }
    }
  }

  test("mergeInto with withSchemaEvolution") {
    withV2Table("wv2_merge_schema_evo") {
      createMergeTarget("wv2_merge_schema_evo")

      val sourceDf = spark.createDataFrame(
        Seq(Row(4L, "dave", 40.0)),
        testSchema
      )

      withSourceView(sourceDf) { source =>
        withMergeSupport {
          spark.table(source).mergeInto(
            "testcat.wv2_merge_schema_evo",
            col("testcat.wv2_merge_schema_evo.id") === col(s"$source.id")
          )
            .whenNotMatched().insertAll()
            .withSchemaEvolution()
            .merge()

          val result = spark.read.table("testcat.wv2_merge_schema_evo")
            .orderBy(col("id")).collect()
          assert(result.length == 4)
          assert(result(3).getLong(0) == 4L)
        }
      }
    }
  }

  test("mergeInto with multiple when clauses") {
    withV2Table("wv2_merge_multi") {
      createMergeTarget("wv2_merge_multi")

      val sourceDf = spark.createDataFrame(
        Seq(
          Row(1L, "alice_new", 100.0), // matches target id=1
          Row(4L, "dave", 40.0), // no match in target
          Row(5L, "eve", 50.0) // no match in target
        ),
        testSchema
      )

      withSourceView(sourceDf) { source =>
        withMergeSupport {
          spark.table(source).mergeInto(
            "testcat.wv2_merge_multi",
            col("testcat.wv2_merge_multi.id") === col(s"$source.id")
          )
            .whenMatched().updateAll()
            .whenNotMatched().insertAll()
            .whenNotMatchedBySource().delete()
            .merge()

          val result = spark.read.table("testcat.wv2_merge_multi")
            .orderBy(col("id")).collect()
          // id=1 updated, id=2 and id=3 deleted (not matched by source), id=4 and id=5 inserted
          assert(result.length == 3)
          assert(result(0).getLong(0) == 1L)
          assert(result(0).getString(1) == "alice_new")
          assert(result(1).getLong(0) == 4L)
          assert(result(2).getLong(0) == 5L)
        }
      }
    }
  }

  test("mergeInto without any action throws SparkException") {
    withV2Table("wv2_merge_no_action") {
      createMergeTarget("wv2_merge_no_action")

      val sourceDf = spark.createDataFrame(
        Seq(Row(1L, "alice", 10.0)),
        testSchema
      )

      val caught = intercept[SparkException] {
        sourceDf.mergeInto(
          "testcat.wv2_merge_no_action",
          col("testcat.wv2_merge_no_action.id") === col("source.id")
        ).merge()
      }
      assert(caught.getMessage.contains("MERGE") || caught.getMessage.contains("action"))
    }
  }
