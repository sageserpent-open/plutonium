package com.sageserpent.plutonium

import java.util.UUID

import com.github.benmanes.caffeine.cache.Cache
import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob

object PithedOutBlobStorage {
  val cache: Cache[UUID, Timeline.BlobStorage] = caffeineBuilder().build()

  val empty: Timeline.BlobStorage = {
    val newImplementationId = UUID.randomUUID()

    PithedOutBlobStorage.cache
      .put(newImplementationId,
           BlobStorageInMemory[ItemStateUpdateTime,
                               ItemStateUpdateKey,
                               SnapshotBlob]())

    PithedOutBlobStorage(newImplementationId)
  }
}

case class PithedOutBlobStorage(implementationId: UUID)(
    override implicit val timeOrdering: Ordering[ItemStateUpdateTime])
    extends Timeline.BlobStorage {
  override def openRevision(): RevisionBuilder = {
    val revisionBuilder =
      Option(PithedOutBlobStorage.cache.getIfPresent(implementationId)).get
        .openRevision()

    new RevisionBuilder {
      override def record(
          key: ItemStateUpdateKey,
          when: ItemStateUpdateTime,
          snapshotBlobs: Map[UniqueItemSpecification, Option[SnapshotBlob]])
        : Unit = revisionBuilder.record(key, when, snapshotBlobs)

      override def build()
        : BlobStorage[ItemStateUpdateTime, ItemStateUpdateKey, SnapshotBlob] = {
        val newImplementationId = UUID.randomUUID()

        PithedOutBlobStorage.cache
          .put(newImplementationId, revisionBuilder.build())

        PithedOutBlobStorage(newImplementationId)
      }
    }
  }

  override def timeSlice(
      when: ItemStateUpdateTime,
      inclusive: Boolean): BlobStorage.Timeslice[SnapshotBlob] =
    Option(PithedOutBlobStorage.cache.getIfPresent(implementationId)).get
      .timeSlice(when, inclusive)

  override def retainUpTo(when: ItemStateUpdateTime)
    : BlobStorage[ItemStateUpdateTime, ItemStateUpdateKey, SnapshotBlob] = {
    val newImplementationId = UUID.randomUUID()

    PithedOutBlobStorage.cache.put(
      newImplementationId,
      Option(PithedOutBlobStorage.cache.getIfPresent(implementationId)).get
        .retainUpTo(when))

    PithedOutBlobStorage(newImplementationId)
  }
}
