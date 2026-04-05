package org.apache.spark.sql.connect

import org.apache.spark.connect.proto.{
  CachedRemoteRelation, Command, RemoveCachedRemoteRelationCommand
}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SessionCleanerSuite extends AnyFunSuite with Matchers:

  test("RemoveCachedRemoteRelationCommand proto builds correctly") {
    val relationId = "test-relation-id-123"
    val cmd = Command.newBuilder()
      .setRemoveCachedRemoteRelationCommand(
        RemoveCachedRemoteRelationCommand.newBuilder()
          .setRelation(
            CachedRemoteRelation.newBuilder().setRelationId(relationId).build()
          )
          .build()
      )
      .build()

    cmd.hasRemoveCachedRemoteRelationCommand shouldBe true
    cmd.getRemoveCachedRemoteRelationCommand.getRelation.getRelationId shouldBe relationId
  }

  test("CachedRemoteRelation proto round-trip") {
    val relationId = "cached-relation-456"
    val cached = CachedRemoteRelation.newBuilder()
      .setRelationId(relationId)
      .build()
    cached.getRelationId shouldBe relationId
  }
