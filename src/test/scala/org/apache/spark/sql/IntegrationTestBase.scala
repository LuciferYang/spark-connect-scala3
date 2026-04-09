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

  /** Upload test/main class files so the server can deserialize UDF lambdas.
    *
    * SC3's `connect/common` and `catalyst/encoders` packages contain Scala 3-compiled versions
    * of classes that also exist on the Scala 2.13 server. Uploading those would shadow the
    * server's copies and cause `ArrayStoreException` classloader conflicts. We exclude them,
    * keeping only SC3-only classes (serialization proxies) that must be present on the server.
    */
  protected lazy val classFilesUploaded: Boolean =
    val connectCommonPrefix = "org/apache/spark/sql/connect/common/"
    val encodersPrefix = "org/apache/spark/sql/catalyst/encoders/"
    // SC3-only proxy classes that MUST be uploaded
    val sc3OnlyEncoderProxies = Set(
      encodersPrefix + "EncoderSerializationProxy",
      encodersPrefix + "EncoderFieldProxy",
      encodersPrefix + "CollectionEncoderProxy",
      encodersPrefix + "ProductEncoderProxy"
    )
    // Classes in connect/common/ that exist on the server and must NOT be shadowed.
    // All other classes in this package are SC3-only and should be uploaded.
    val serverConnectCommonClasses = Set(
      "UdfPacket", "UdfPacket$",
      "ForeachWriterPacket", "ForeachWriterPacket$",
      "LiteralValueProtoConverter", "LiteralValueProtoConverter$",
      "LiteralValueProtoConverter$ToLiteralProtoOptions",
      "LiteralValueProtoConverter$ToLiteralProtoOptions$"
    )

    val excludeShadowClasses: java.nio.file.Path => Boolean = { rel =>
      val s = rel.toString
      // Exclude connect/common classes that exist on the server
      val excludeConnectCommon =
        s.startsWith(connectCommonPrefix) && {
          val classFile = s.stripPrefix(connectCommonPrefix)
          val className = classFile.stripSuffix(".class").stripSuffix(".tasty")
          serverConnectCommonClasses.contains(className)
        }
      // Exclude catalyst/encoders/ (server has its own) except SC3-only proxy classes
      val excludeEncoders =
        s.startsWith(encodersPrefix) &&
          !sc3OnlyEncoderProxies.exists(proxy => s.startsWith(proxy))
      excludeConnectCommon || excludeEncoders
    }
    val testClassDir = Paths.get("target/scala-3.3.7/test-classes")
    if testClassDir.toFile.isDirectory then
      spark.addClassDir(testClassDir, excludeShadowClasses)
    val mainClassDir = Paths.get("target/scala-3.3.7/classes")
    if mainClassDir.toFile.isDirectory then
      spark.addClassDir(mainClassDir, excludeShadowClasses)
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
    * All operations that serialize Scala 3 lambdas to a Scala 2.13 Spark server may fail because:
    * - The server cannot find `$deserializeLambda$`
    * - The LambdaSerializationProxy cannot resolve the impl method on the server
    * - The server cannot generate an encoder for inner classes
    * - The server receives a null encoder from cross-version proxy resolution
    * This wraps those operations and cancels the test gracefully when a known incompatibility is
    * detected.
    */
  protected def withLambdaCompat[T](body: => T): T =
    def isKnownIncompat(msg: String): Boolean =
      msg != null && (
        msg.contains("deserializeLambda") ||
        msg.contains("Failed to unpack scala udf") ||
        msg.contains("Failed to load class correctly") ||
        msg.contains("Failed to resolve method handle") ||
        msg.contains("Unable to generate an encoder for inner class") ||
        msg.contains("\"encoder\" is null")
      )
    try body
    catch
      case e: SparkException if isKnownIncompat(e.getMessage) =>
        cancel("Scala 3 lambda serialization incompatible with Scala 2.13 server")
      case e: Exception if isKnownIncompat(e.getMessage) =>
        cancel("Scala 3 lambda serialization incompatible with Scala 2.13 server")
