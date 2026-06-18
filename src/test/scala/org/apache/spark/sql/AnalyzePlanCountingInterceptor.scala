package org.apache.spark.sql

import io.grpc.{CallOptions, Channel, ClientCall, ClientInterceptor, MethodDescriptor}

import java.util.concurrent.atomic.AtomicInteger

/** Test gRPC interceptor that counts `AnalyzePlan` RPCs issued over the channel.
  *
  * Used by integration tests to assert that an action does not trigger a separate schema-resolution
  * round-trip (`AnalyzePlan`) on top of its `ExecutePlan`.
  */
final class AnalyzePlanCountingInterceptor extends ClientInterceptor:

  val analyzeCalls: AtomicInteger = AtomicInteger(0)

  override def interceptCall[Req, Resp](
      method: MethodDescriptor[Req, Resp],
      callOptions: CallOptions,
      next: Channel
  ): ClientCall[Req, Resp] =
    if method.getFullMethodName.contains("AnalyzePlan") then analyzeCalls.incrementAndGet()
    next.newCall(method, callOptions)
