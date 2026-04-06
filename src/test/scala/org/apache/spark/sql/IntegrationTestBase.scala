package org.apache.spark.sql

import org.apache.spark.sql.tags.IntegrationTest

import java.nio.file.Paths

/** Base trait for integration tests that require a running Spark Connect server at
  * sc://localhost:15002.
  *
  * All concrete suites extending this trait must be annotated with `@IntegrationTest` so that
  * `sbt test` excludes them by tag.
  *
  * Start a Spark Connect server with: $SPARK_HOME/sbin/start-connect-server.sh
  */
trait IntegrationTestBase extends org.scalatest.funsuite.AnyFunSuite:

  lazy val spark: SparkSession =
    SparkSession.builder().remote("sc://localhost:15002").build()

  /** Upload test/main class files so the server can deserialize UDF lambdas. */
  protected lazy val classFilesUploaded: Boolean =
    val testClassDir = Paths.get("target/scala-3.3.7/test-classes")
    if testClassDir.toFile.isDirectory then spark.addClassDir(testClassDir)
    val mainClassDir = Paths.get("target/scala-3.3.7/classes")
    if mainClassDir.toFile.isDirectory then spark.addClassDir(mainClassDir)
    val cp = System.getProperty("java.class.path", "").split(java.io.File.pathSeparator)
    cp.filter { entry =>
      val name = entry.substring(entry.lastIndexOf(java.io.File.separatorChar) + 1)
      name.endsWith(".jar") &&
      (name.startsWith("scalatest") || name.startsWith("scalactic") ||
        name.startsWith("scala3-library") || name.startsWith("scala-library"))
    }.foreach { jar =>
      spark.addArtifact(jar)
    }
    true

  /** Helper to catch Scala 3/2.13 lambda serialization incompatibility.
    *
    * All operations that serialize Scala 3 lambdas to a Scala 2.13 Spark server will fail because
    * the server cannot find `$deserializeLambda$`. This wraps those operations and cancels the test
    * gracefully when the known incompatibility is detected.
    */
  protected def withLambdaCompat[T](body: => T): T =
    try body
    catch
      case e: SparkException
          if e.getMessage != null &&
            (e.getMessage.contains("deserializeLambda") ||
              e.getMessage.contains("Failed to unpack scala udf") ||
              e.getMessage.contains("Failed to load class correctly")) =>
        cancel("Scala 3 lambda serialization incompatible with Scala 2.13 server")
      case e: Exception
          if e.getMessage != null &&
            (e.getMessage.contains("deserializeLambda") ||
              e.getMessage.contains("Failed to unpack scala udf")) =>
        cancel("Scala 3 lambda serialization incompatible with Scala 2.13 server")
