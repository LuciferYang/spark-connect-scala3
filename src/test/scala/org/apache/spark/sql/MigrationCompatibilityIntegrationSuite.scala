package org.apache.spark.sql

import org.apache.spark.sql.functions.*
import org.apache.spark.sql.tags.IntegrationTest
import org.apache.spark.sql.types.*

@IntegrationTest
class MigrationCompatibilityIntegrationSuite extends IntegrationTestBase:

  test("spark.implicits Seq.toDS and Seq.toDF execute against Connect server") {
    val session = spark
    import session.implicits.*

    val ds = Seq(1, 2, 3).toDS
    assert(ds.count() == 3L)

    val rows = Seq(4, 5, 6).toDF("id").orderBy(col("id")).collect()
    assert(rows.map(_.getInt(0)).toSeq == Seq(4, 5, 6))
  }

  test("SQLContext facade supports common delegated migration calls") {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits.*

    val df = Seq(1, 2, 3).toDF("id")
    assert(df.filter(col("id") > lit(1)).count() == 2L)

    val queryRows = sqlContext.sql("SELECT 7 AS id").collect()
    assert(queryRows.length == 1)
    assert(queryRows.head.getInt(0) == 7)
  }

  test("expressions.Window package works in a real query") {
    val rows = Seq(
      Row("A", 1, 10),
      Row("A", 2, 20),
      Row("B", 1, 30)
    )
    val schema = StructType(Seq(
      StructField("grp", StringType),
      StructField("seq", IntegerType),
      StructField("value", IntegerType)
    ))
    val df = spark.createDataFrame(rows, schema)
    val w = org.apache.spark.sql.expressions.Window
      .partitionBy("grp")
      .orderBy("seq")
      .rowsBetween(
        org.apache.spark.sql.expressions.Window.unboundedPreceding,
        org.apache.spark.sql.expressions.Window.currentRow
      )

    val result = df
      .select(col("grp"), col("seq"), sum(col("value")).over(w).as("running"))
      .orderBy(col("grp"), col("seq"))
      .collect()

    assert(result.map(_.getLong(2)).toSeq == Seq(10L, 30L, 30L))
  }
