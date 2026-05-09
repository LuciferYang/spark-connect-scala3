package org.apache.spark.sql.connect.client

import io.grpc.ManagedChannel
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.TimeUnit

class SharedChannelSuite extends AnyFunSuite with Matchers:

  /** Fake ManagedChannel that tracks shutdown calls without doing real I/O. */
  private class FakeManagedChannel extends ManagedChannel:
    var shutdownCalled = false
    var shutdownNowCalled = false
    var isShut = false

    def shutdown(): ManagedChannel =
      shutdownCalled = true
      isShut = true
      this
    def shutdownNow(): ManagedChannel =
      shutdownNowCalled = true
      isShut = true
      this
    def isShutdown: Boolean = isShut
    def isTerminated: Boolean = isShut
    def awaitTermination(timeout: Long, unit: TimeUnit): Boolean = true
    def newCall[ReqT, RespT](
        method: io.grpc.MethodDescriptor[ReqT, RespT],
        options: io.grpc.CallOptions
    ): io.grpc.ClientCall[ReqT, RespT] =
      throw UnsupportedOperationException("fake")
    def authority(): String = "fake"

  test("single owner: release shuts down channel") {
    val fake = FakeManagedChannel()
    val sc = SharedChannel(fake)
    sc.referenceCount shouldBe 1
    sc.release()
    fake.shutdownCalled shouldBe true
    sc.referenceCount shouldBe 0
  }

  test("retain increments ref count") {
    val fake = FakeManagedChannel()
    val sc = SharedChannel(fake)
    sc.retain()
    sc.referenceCount shouldBe 2
    sc.release()
    sc.release()
    fake.shutdownCalled shouldBe true
  }

  test("multiple retains: channel survives until last release") {
    val fake = FakeManagedChannel()
    val sc = SharedChannel(fake)
    sc.retain() // refCount = 2
    sc.retain() // refCount = 3
    sc.referenceCount shouldBe 3

    sc.release() // refCount = 2
    fake.shutdownCalled shouldBe false
    sc.release() // refCount = 1
    fake.shutdownCalled shouldBe false
    sc.release() // refCount = 0 → shutdown
    fake.shutdownCalled shouldBe true
  }

  test("retain after full release throws") {
    val fake = FakeManagedChannel()
    val sc = SharedChannel(fake)
    sc.release()
    assertThrows[IllegalStateException] {
      sc.retain()
    }
  }

  test("double release is safe (no negative refcount)") {
    val fake = FakeManagedChannel()
    val sc = SharedChannel(fake)
    sc.release()
    sc.release() // should not throw
    sc.referenceCount shouldBe 0
  }

  test("isShutdown delegates to underlying") {
    val fake = FakeManagedChannel()
    val sc = SharedChannel(fake)
    sc.isShutdown shouldBe false
    sc.release()
    sc.isShutdown shouldBe true
  }
