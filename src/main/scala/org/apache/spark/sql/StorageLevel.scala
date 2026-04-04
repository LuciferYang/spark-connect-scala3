package org.apache.spark.sql

import org.apache.spark.connect.proto.StorageLevel as ProtoStorageLevel

/** Flags for controlling the storage of an RDD/DataFrame. Mirrors
  * org.apache.spark.storage.StorageLevel.
  */
final case class StorageLevel(
    useDisk: Boolean,
    useMemory: Boolean,
    useOffHeap: Boolean,
    deserialized: Boolean,
    replication: Int = 1
):
  private[sql] def toProto: ProtoStorageLevel =
    ProtoStorageLevel.newBuilder()
      .setUseDisk(useDisk)
      .setUseMemory(useMemory)
      .setUseOffHeap(useOffHeap)
      .setDeserialized(deserialized)
      .setReplication(replication)
      .build()

object StorageLevel:
  val NONE = StorageLevel(false, false, false, false, 1)
  val DISK_ONLY = StorageLevel(true, false, false, false, 1)
  val DISK_ONLY_2 = StorageLevel(true, false, false, false, 2)
  val MEMORY_ONLY = StorageLevel(false, true, false, true, 1)
  val MEMORY_ONLY_2 = StorageLevel(false, true, false, true, 2)
  val MEMORY_ONLY_SER = StorageLevel(false, true, false, false, 1)
  val MEMORY_AND_DISK = StorageLevel(true, true, false, true, 1)
  val MEMORY_AND_DISK_2 = StorageLevel(true, true, false, true, 2)
  val MEMORY_AND_DISK_SER = StorageLevel(true, true, false, false, 1)
  val OFF_HEAP = StorageLevel(true, true, true, false, 1)
