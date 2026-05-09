package org.apache.spark.sql.connect.client

import io.grpc.ManagedChannel

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.util.control.NonFatal

/** A reference-counted wrapper around a [[ManagedChannel]].
  *
  * Multiple [[SparkConnectClient]] instances can share the same underlying gRPC channel (e.g.
  * after `cloneSession`). The channel is only shut down when the last reference calls [[release]].
  *
  * This class is thread-safe.
  */
private[client] final class SharedChannel(private val channel: ManagedChannel):
  private val refCount = AtomicInteger(1)

  /** Access the underlying channel for creating stubs or iterators. */
  private[client] def underlying: ManagedChannel = channel

  /** Increment the reference count. Call when sharing this channel with a new client.
    *
    * Uses CAS loop to prevent incrementing a fully-released (refCount=0) channel.
    */
  def retain(): SharedChannel =
    var current = refCount.get()
    while current > 0 do
      if refCount.compareAndSet(current, current + 1) then return this
      current = refCount.get()
    throw IllegalStateException("Cannot retain a SharedChannel that has already been fully released")

  /** Decrement the reference count. When it reaches zero, the channel is shut down. */
  def release(): Unit =
    val now = refCount.decrementAndGet()
    if now == 0 then
      try
        channel.shutdown()
        if !channel.awaitTermination(5, TimeUnit.SECONDS) then channel.shutdownNow()
      catch case NonFatal(_) => channel.shutdownNow()
    else if now < 0 then
      // Guard against double-release (restore to 0 and ignore)
      refCount.compareAndSet(now, 0)

  def isShutdown: Boolean = channel.isShutdown

  /** Current reference count (for testing/diagnostics). */
  def referenceCount: Int = refCount.get()
