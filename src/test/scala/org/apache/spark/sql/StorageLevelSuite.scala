package org.apache.spark.sql

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class StorageLevelSuite extends AnyFunSuite with Matchers:

  test("predefined storage levels") {
    StorageLevel.MEMORY_ONLY.useMemory shouldBe true
    StorageLevel.MEMORY_ONLY.useDisk shouldBe false

    StorageLevel.DISK_ONLY.useDisk shouldBe true
    StorageLevel.DISK_ONLY.useMemory shouldBe false

    StorageLevel.MEMORY_AND_DISK.useMemory shouldBe true
    StorageLevel.MEMORY_AND_DISK.useDisk shouldBe true

    StorageLevel.OFF_HEAP.useOffHeap shouldBe true

    StorageLevel.MEMORY_ONLY_2.replication shouldBe 2
  }

  test("toProto conversion") {
    val proto = StorageLevel.MEMORY_AND_DISK.toProto
    proto.getUseMemory shouldBe true
    proto.getUseDisk shouldBe true
    proto.getUseOffHeap shouldBe false
    proto.getDeserialized shouldBe true
    proto.getReplication shouldBe 1
  }
