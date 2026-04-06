package org.apache.spark.sql.connect

import org.apache.spark.connect.proto.*
import org.apache.spark.sql.SparkSession

import scala.util.control.NonFatal

/** GC-based cleanup of `CachedRemoteRelation` references on the server.
  *
  * When a cached relation is garbage-collected on the client, the cleaner sends a
  * `RemoveCachedRemoteRelationCommand` to the server to release the corresponding resources.
  *
  * Uses `java.lang.ref.Cleaner` (JDK 9+). The cleaning action captures only the `relationId` string
  * (not the relation object itself) to avoid preventing GC.
  */
class SessionCleaner(session: SparkSession):

  private val cleaner = java.lang.ref.Cleaner.create()

  /** Register a `CachedRemoteRelation` for automatic cleanup when GC'd.
    *
    * The `CachedRemoteRelation` proto message itself is a lightweight value object; the heavy
    * server-side cache is keyed by `relation_id`. We capture only the ID so the proto can be GC'd.
    */
  def register(relation: CachedRemoteRelation): Unit =
    val relationId = relation.getRelationId
    cleaner.register(
      relation,
      (() => doCleanup(relationId)): Runnable
    )

  private def doCleanup(relationId: String): Unit =
    try
      val cmd = Command.newBuilder()
        .setRemoveCachedRemoteRelationCommand(
          RemoveCachedRemoteRelationCommand.newBuilder()
            .setRelation(
              CachedRemoteRelation.newBuilder().setRelationId(relationId).build()
            )
            .build()
        )
        .build()
      session.client.executeCommand(cmd)
    catch case NonFatal(_) => () // best-effort; session may already be closed
