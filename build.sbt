ThisBuild / scalaVersion := "3.3.7"
ThisBuild / organization := "io.github.luciferyang"

// ---------------------------------------------------------------------------
// Publishing metadata (required by Maven Central)
// ---------------------------------------------------------------------------
ThisBuild / description := s"A lightweight Apache Spark Connect client for Scala 3 (targeting Spark $sparkVersion)"
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

val grpcVersion = "1.81.0"
val protobufVersion = "4.34.1"
val arrowVersion = "19.0.0"
val ammoniteVersion = "3.0.9"
val zstdVersion = "1.5.7-8"
val asmVersion = "9.9.1"
val scalatestVersion = "3.2.19"
val jacksonVersion = "2.21.0"

// The Spark Connect server version this client is built and tested against.
// Encoded in the artifactId so users know which server version to target.
val sparkVersion = "4.1"

// Google Mirror of Maven Central, placed first so that it's used instead of flaky Maven Central.
// See https://storage-download.googleapis.com/maven-central/index.html
ThisBuild / resolvers += "gcs-maven-central-mirror" at
  "https://maven-central.storage-download.googleapis.com/maven2/"

lazy val root = (project in file("."))
  .enablePlugins(JmhPlugin)
  .settings(
    name := s"spark-connect-spark${sparkVersion.replace(".", "")}",

    scalacOptions ++= Seq(
      "-encoding", "utf8",
      "-feature",
      "-unchecked",
      "-deprecation"
    ),

    // Strict warnings-as-errors for main sources: unused symbols + deprecation → compile errors.
    // Test code is relaxed to avoid noise from unused variables in test fixtures.
    // LiteralValueProtoConverter uses proto APIs that are deprecated in some protobuf versions
    // but not others, so we silence deprecation warnings for that file specifically.
    Compile / scalacOptions ++= Seq(
      "-Wunused:all",
      "-Werror",
      "-Wconf:cat=deprecation&src=LiteralValueProtoConverter\\.scala:s"
    ),
    Test / scalacOptions --= Seq("-Wunused:all", "-Werror"),

    Compile / mainClass := Some("org.apache.spark.sql.application.ConnectRepl"),

    libraryDependencies ++= {
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

        // Jackson for streaming progress JSON parsing (already transitive via Arrow,
        // but pinned explicitly to avoid hidden coupling to Arrow's version).
        "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,

        // Zstandard for plan compression
        "com.github.luben" % "zstd-jni" % zstdVersion,

        // ASM for closure-cleaning bytecode analysis
        "org.ow2.asm" % "asm" % asmVersion,
        "org.ow2.asm" % "asm-tree" % asmVersion,

        // Ammonite REPL (published per full Scala version, not binary)
        "com.lihaoyi" % s"ammonite_$scalaFullVersion" % ammoniteVersion cross CrossVersion.disabled,

        // Testing
        "org.scalatest" %% "scalatest" % scalatestVersion % Test,
        "org.scalatestplus" %% "scalacheck-1-18" % "3.2.19.0" % Test
      )
    },

    // Java protobuf + gRPC Java code generation
    Compile / PB.targets := Seq(
      PB.gens.java -> (Compile / sourceManaged).value / "java",
      PB.gens.plugin("grpc-java") -> (Compile / sourceManaged).value / "java"
    ),

    Compile / PB.protoSources := Seq(
      baseDirectory.value / "spark-upstream" / "sql" / "connect" / "common" / "src" / "main" / "protobuf"
    ),

    // Include spark-sketch Java sources from the spark-upstream submodule (BloomFilter, CountMinSketch).
    Compile / unmanagedSourceDirectories +=
      baseDirectory.value / "spark-upstream" / "common" / "sketch" / "src" / "main" / "java",

    // Include Spark annotation and spatial value classes from the spark-upstream submodule.
    Compile / unmanagedSourceDirectories +=
      baseDirectory.value / "spark-upstream" / "common" / "tags" / "src" / "main" / "java",
    Compile / unmanagedSources ++= {
      val typesDir = baseDirectory.value / "spark-upstream" / "sql" / "api" / "src" / "main" / "java" /
        "org" / "apache" / "spark" / "sql" / "types"
      Seq(typesDir / "Geometry.java", typesDir / "Geography.java")
    },

    // gRPC Java codegen plugin
    libraryDependencies += "io.grpc" % "protoc-gen-grpc-java" % grpcVersion asProtocPlugin(),

    // Exclude @IntegrationTest-tagged suites from unit tests (requires live Spark Connect Server)
    Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest,
      "-l", "org.apache.spark.sql.tags.IntegrationTest"),

    // Exclude code that requires a live Spark Connect server (not unit-testable)
    // or is vendored upstream code with deep reflection (ClosureCleaner)
    coverageExcludedPackages := Seq(
      "org\\.apache\\.spark\\.sql\\.application\\..*",            // ConnectRepl
      "org\\.apache\\.spark\\.sql\\.connect\\.client\\..*",       // gRPC client internals
      "org\\.apache\\.spark\\.sql\\.connect\\.common\\..*",       // ClosureCleaner (vendored upstream)
      "org\\.apache\\.spark\\.sql\\.connect\\..*",                // any remaining connect internals
      "org\\.apache\\.spark\\.sql\\.catalyst\\.encoders\\..*",    // AgnosticEncoder serialization proxies
      "org\\.apache\\.spark\\.sql\\.StreamingQuery",              // requires live server
      "org\\.apache\\.spark\\.sql\\.StreamingQueryManager",       // requires live server
      "org\\.apache\\.spark\\.sql\\.streaming\\.StreamingQueryListenerBus",  // requires live server
      "org\\.apache\\.spark\\.sql\\.KeyValueGroupedDataset",      // cogroup/flatMapGroups need server
      "org\\.apache\\.spark\\.sql\\.Observation",                 // get() needs server metrics
      "org\\.apache\\.spark\\.sql\\.UDFRegistration"              // register() needs server
    ).mkString(";"),

    // Fail build if statement coverage drops below 80%.
    coverageMinimumStmtTotal := 80,
    coverageFailOnMinimum := true,

    // JVM options for Apache Arrow + spark-sketch (sun.misc.Unsafe)
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    ),
    Test / fork := true,

    // Run options for Arrow + spark-sketch
    run / javaOptions ++= Seq(
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
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
    ),

    // -------------------------------------------------------------------------
    // JMH Benchmark settings
    // -------------------------------------------------------------------------
    Jmh / sourceDirectory := (ThisBuild / baseDirectory).value / "src" / "jmh",
    // Relax strict compilation for benchmark code (no -Werror, no -Wunused)
    Jmh / scalacOptions --= Seq("-Wunused:all", "-Werror"),
    Jmh / javaOptions ++= Seq(
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    ),
    Jmh / fork := true
  )
