ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.7"
ThisBuild / organization := "io.github.spark-connect"

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
      val grpcVersion = "1.80.0"
      val scalapbVersion = scalapb.compiler.Version.scalapbVersion
      val arrowVersion = "19.0.0"
      val protobufVersion = "4.34.1"

      Seq(
        // gRPC and Protocol Buffers
        "com.google.protobuf" % "protobuf-java" % protobufVersion,
        "io.grpc" % "grpc-netty-shaded" % grpcVersion,
        "io.grpc" % "grpc-protobuf" % grpcVersion,
        "io.grpc" % "grpc-stub" % grpcVersion,
        "com.thesamet.scalapb" %% "scalapb-runtime" % scalapbVersion % "protobuf",
        "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapbVersion,

        // Apache Arrow for data transfer
        "org.apache.arrow" % "arrow-vector" % arrowVersion,
        "org.apache.arrow" % "arrow-memory-netty" % arrowVersion,

        // Zstandard for plan compression
        "com.github.luben" % "zstd-jni" % "1.5.6-8",

        // Testing
        "org.scalatest" %% "scalatest" % "3.2.19" % Test
      )
    },

    // ScalaPB protobuf code generation
    Compile / PB.targets := Seq(
      scalapb.gen(grpc = true) -> (Compile / sourceManaged).value / "scalapb"
    ),

    Compile / PB.protoSources := Seq(
      baseDirectory.value / "src" / "main" / "protobuf"
    ),

    // JVM options for Apache Arrow
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
    ),
    Test / fork := true,

    // Run options for Arrow
    run / javaOptions ++= Seq(
      "--add-opens=java.base/java.nio=ALL-UNNAMED"
    ),
    run / fork := true
  )
