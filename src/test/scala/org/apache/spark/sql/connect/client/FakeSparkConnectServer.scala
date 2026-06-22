package org.apache.spark.sql.connect.client

import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import org.apache.spark.connect.proto.SparkConnectServiceGrpc

/** An in-process fake Spark Connect server for unit tests.
  *
  * Binds a real gRPC server on an ephemeral localhost port so the production
  * [[SparkConnectClient]] — which only connects via `sc://host:port` — can talk to it without a live
  * Spark cluster. Each test supplies a service implementation overriding just the RPCs it needs;
  * un-overridden RPCs return `UNIMPLEMENTED`.
  *
  * {{{
  *   val service = new SparkConnectServiceGrpc.SparkConnectServiceImplBase { ... }
  *   val server = FakeSparkConnectServer(service)
  *   try { ... SparkSession.builder().remote(server.url).build() ... } finally server.stop()
  * }}}
  */
final class FakeSparkConnectServer(
    service: SparkConnectServiceGrpc.SparkConnectServiceImplBase
):
  private val server: Server =
    NettyServerBuilder.forPort(0).addService(service).build().start()

  /** The `sc://` URL the client should connect to. */
  def url: String = s"sc://localhost:${server.getPort}"

  def stop(): Unit =
    server.shutdownNow()
    server.awaitTermination()
