ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.7"
ThisBuild / organization := "io.github.spark-connect"

val grpcVersion = "1.80.0"
val protobufVersion = "4.34.1"

lazy val root = (project in file("."))
  .settings(
    name := "spark-connect-scala3",

    scalacOptions ++= Seq(
      "-encoding", "utf8",
      "-feature",
      "-unchecked",
      "-deprecation"
    ),

    libraryDependencies ++= {
      val arrowVersion = "19.0.0"

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

    // Exclude IntegrationSuite from unit tests (requires live Spark Connect Server)
    Test / testOptions += Tests.Filter(name => !name.contains("IntegrationSuite")),

    // JVM options for Apache Arrow
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/java.nio=ALL-UNNAMED"
    ),
    Test / fork := true,

    // Run options for Arrow
    run / javaOptions ++= Seq(
      "--add-opens=java.base/java.nio=ALL-UNNAMED"
    ),
    run / fork := true
  )
