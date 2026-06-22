package org.apache.spark.sql.connect.client

import io.grpc.Server
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import org.apache.spark.connect.proto.SparkConnectServiceGrpc

/** In-process fake Spark Connect server on an ephemeral localhost port, for unit tests. */
final class FakeSparkConnectServer(service: SparkConnectServiceGrpc.SparkConnectServiceImplBase):

  private val server: Server = NettyServerBuilder.forPort(0).addService(service).build().start()

  /** The `sc://` URL the client should connect to. */
  def url: String = s"sc://localhost:${server.getPort}"

  def stop(): Unit =
    server.shutdownNow()
    server.awaitTermination()
