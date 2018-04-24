package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob

import scala.collection.immutable.Map

class TimelineImplementation(
    lifecyclesState: LifecyclesState = noLifecyclesState(),
    blobStorage: BlobStorage[ItemStateUpdate.Key, SnapshotBlob] =
      BlobStorageInMemory[ItemStateUpdate.Key, SnapshotBlob]())
    extends Timeline {

  override def revise(events: Map[_ <: EventId, Option[Event]]) = {
    val (newLifecyclesState, blobStorageForNewTimeline) = lifecyclesState
      .revise(events, blobStorage)

    new TimelineImplementation(
      lifecyclesState = newLifecyclesState,
      blobStorage = blobStorageForNewTimeline
    )
  }

  override def retainUpTo(when: Unbounded[Instant]) = {
    new TimelineImplementation(
      lifecyclesState = this.lifecyclesState.retainUpTo(when),
      blobStorage = this.blobStorage.retainUpTo(when)
    )
  }

  override def itemCacheAt(when: Unbounded[Instant]) =
    new ItemCacheUsingBlobStorage(blobStorage, when)
}
