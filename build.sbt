ThisBuild / scalaVersion := "3.3.7"
ThisBuild / organization := "io.github.luciferyang"

// ---------------------------------------------------------------------------
// Publishing metadata (required by Maven Central)
// ---------------------------------------------------------------------------
ThisBuild / description := "A lightweight Apache Spark Connect client for Scala 3"
ThisBuild / homepage := Some(url("https://github.com/LuciferYang/spark-connect-scala3"))
ThisBuild / licenses := List("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / developers := List(
  Developer(
    id = "LuciferYang",
    name = "Yang Jie",
    email = "yangjie01@baidu.com",
    url = url("https://github.com/LuciferYang")
  )
)
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/LuciferYang/spark-connect-scala3"),
    "scm:git@github.com:LuciferYang/spark-connect-scala3.git"
  )
)

// Sonatype / Maven Central publishing
ThisBuild / sonatypeCredentialHost := "central.sonatype.com"
ThisBuild / publishTo := sonatypePublishToBundle.value

val grpcVersion = "1.80.0"
val protobufVersion = "4.34.1"
val ammoniteVersion = "3.0.9"

// Google Mirror of Maven Central, placed first so that it's used instead of flaky Maven Central.
// See https://storage-download.googleapis.com/maven-central/index.html
ThisBuild / resolvers += "gcs-maven-central-mirror" at
  "https://maven-central.storage-download.googleapis.com/maven2/"

lazy val root = (project in file("."))
  .settings(
    name := "spark-connect-scala3",

    scalacOptions ++= Seq(
      "-encoding", "utf8",
      "-feature",
      "-unchecked",
      "-deprecation"
    ),

    Compile / mainClass := Some("org.apache.spark.sql.application.ConnectRepl"),

    libraryDependencies ++= {
      val arrowVersion = "19.0.0"
      val scalaFullVersion = scalaVersion.value // e.g. "3.3.7"

      Seq(
        // gRPC and Protocol Buffers
        "com.google.protobuf" % "protobuf-java" % protobufVersion,
        "com.google.protobuf" % "protobuf-java" % protobufVersion % "protobuf",
        "io.grpc" % "grpc-netty-shaded" % grpcVersion,
        "io.grpc" % "grpc-protobuf" % grpcVersion,
        "io.grpc" % "grpc-stub" % grpcVersion,

        // Apache Arrow for data transfer
        "org.apache.arrow" % "arrow-vector" % arrowVersion,
        "org.apache.arrow" % "arrow-memory-netty" % arrowVersion,

        // Zstandard for plan compression
        "com.github.luben" % "zstd-jni" % "1.5.6-8",

        // ASM for closure-cleaning bytecode analysis (mirrors upstream Spark's
        // ClosureCleaner approach for stripping unused outer references from
        // Scala lambdas before sending them over the wire).
        "org.ow2.asm" % "asm" % "9.9.1",
        "org.ow2.asm" % "asm-tree" % "9.9.1",

        // Ammonite REPL (published per full Scala version, not binary)
        "com.lihaoyi" % s"ammonite_$scalaFullVersion" % ammoniteVersion cross CrossVersion.disabled,

        // Testing
        "org.scalatest" %% "scalatest" % "3.2.19" % Test
      )
    },

    // Java protobuf + gRPC Java code generation
    Compile / PB.targets := Seq(
      PB.gens.java -> (Compile / sourceManaged).value / "java",
      PB.gens.plugin("grpc-java") -> (Compile / sourceManaged).value / "java"
    ),

    Compile / PB.protoSources := Seq(
      baseDirectory.value / "src" / "main" / "protobuf"
    ),

    // gRPC Java codegen plugin
    libraryDependencies += "io.grpc" % "protoc-gen-grpc-java" % grpcVersion asProtocPlugin(),

    // Exclude @IntegrationTest-tagged suites from unit tests (requires live Spark Connect Server)
    Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest,
      "-l", "org.apache.spark.sql.tags.IntegrationTest"),

    // Exclude code that requires a live Spark Connect server (not unit-testable)
    coverageExcludedPackages := Seq(
      "org\\.apache\\.spark\\.sql\\.application\\..*",            // ConnectRepl
      "org\\.apache\\.spark\\.sql\\.connect\\.client\\..*",       // gRPC client internals
      "org\\.apache\\.spark\\.sql\\.StreamingQuery",              // requires live server
      "org\\.apache\\.spark\\.sql\\.StreamingQueryManager",       // requires live server
      "org\\.apache\\.spark\\.sql\\.streaming\\.StreamingQueryListenerBus"  // requires live server
    ).mkString(";"),

    // JVM options for Apache Arrow
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/java.nio=ALL-UNNAMED"
    ),
    Test / fork := true,

    // Run options for Arrow
    run / javaOptions ++= Seq(
      "--add-opens=java.base/java.nio=ALL-UNNAMED"
    ),
    run / fork := true,

    // Include LICENSE and NOTICE in the jar
    Compile / packageBin / mappings ++= Seq(
      baseDirectory.value / "LICENSE" -> "META-INF/LICENSE",
      baseDirectory.value / "NOTICE" -> "META-INF/NOTICE"
    ),
    Compile / packageSrc / mappings ++= Seq(
      baseDirectory.value / "LICENSE" -> "META-INF/LICENSE",
      baseDirectory.value / "NOTICE" -> "META-INF/NOTICE"
    )
  )
