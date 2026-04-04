package org.apache.spark.sql.examples

import org.apache.spark.sql.{SparkSession, Row, functions as F}
import org.apache.spark.sql.types.*

/** Quick-start example for the Scala 3 Spark Connect client.
  *
  * Prerequisites:
  *   1. Start a Spark Connect server (Spark 4.0+):
  *      {{{
  *        ./sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.13:4.0.0
  *      }}}
  *      Or with an existing Spark installation:
  *      {{{
  *        $SPARK_HOME/sbin/start-connect-server.sh
  *      }}}
  *   2. Run this example:
  *      {{{
  *        sbt "runMain org.apache.spark.sql.examples.QuickStart"
  *      }}}
  *   3. Override the connection URL via SPARK_CONNECT_URL env var:
  *      {{{
  *        SPARK_CONNECT_URL=sc://remote-host:15002 sbt "runMain ..."
  *      }}}
  */
@main def quickStart(): Unit =
  val url = sys.env.getOrElse("SPARK_CONNECT_URL", "sc://localhost:15002")
  println(s"Connecting to Spark Connect at: $url")
  println("=" * 60)

  val spark = SparkSession.builder()
    .remote(url)
    .build()

  try
    // 1. Server version
    println(s"\n[1] Spark version: ${spark.version}")

    // 2. SQL query
    println("\n[2] SQL: SELECT 1 as one, 'hello' as greeting")
    spark.sql("SELECT 1 as one, 'hello' as greeting").show()

    // 3. Range DataFrame
    println("\n[3] spark.range(10)")
    spark.range(10).show()

    // 4. Transformations: filter, select, withColumn
    println("\n[4] range(20).filter(id > 10).withColumn(doubled, id * 2)")
    spark.range(20)
      .filter(F.col("id") > F.lit(10))
      .withColumn("doubled", F.col("id") * F.lit(2))
      .show()

    // 5. Aggregation
    println("\n[5] range(100).groupBy(id % 5).agg(count, sum)")
    spark.range(100)
      .withColumn("group", F.col("id") % F.lit(5))
      .groupBy(F.col("group"))
      .agg(F.count(F.col("id")).as("cnt"), F.sum(F.col("id")).as("total"))
      .orderBy(F.col("group"))
      .show()

    // 6. createDataFrame with Arrow
    println("\n[6] createDataFrame with Arrow serialization")
    val schema = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("score", DoubleType)
    ))
    val rows = Seq(
      Row("Alice", 30, 95.5),
      Row("Bob", 25, 88.0),
      Row("Carol", 35, 92.3)
    )
    val df = spark.createDataFrame(rows, schema)
    df.show()
    println("Schema:")
    df.printSchema()

    // 7. Join
    println("\n[7] Self-join example")
    val left = spark.sql("SELECT 1 as id, 'a' as val1 UNION ALL SELECT 2, 'b'")
    val right = spark.sql("SELECT 1 as id, 'x' as val2 UNION ALL SELECT 2, 'y'")
    left.join(right, Seq("id")).show()

    // 8. Config
    println("\n[8] Config: spark.sql.shuffle.partitions")
    println(s"  Current value: ${spark.conf.get("spark.sql.shuffle.partitions")}")

    // 9. Temp View + Catalog
    println("\n[9] createOrReplaceTempView + catalog")
    spark.range(5).createOrReplaceTempView("my_range")
    println(s"  tableExists('my_range'): ${spark.catalog.tableExists("my_range")}")
    println(s"  tableExists('no_such_table'): ${spark.catalog.tableExists("no_such_table")}")
    println(s"  currentDatabase: ${spark.catalog.currentDatabase}")
    println("  listTables:")
    spark.catalog.listTables().show()
    println("  Query temp view via SQL:")
    spark.sql("SELECT * FROM my_range").show()
    spark.catalog.dropTempView("my_range")
    println(s"  After drop, tableExists('my_range'): ${spark.catalog.tableExists("my_range")}")

    println("\n" + "=" * 60)
    println("All examples completed successfully!")

  finally
    spark.stop()
    println("Session stopped.")
