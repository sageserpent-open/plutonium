package com.sageserpent.plutonium

import java.time.Instant
import java.util.UUID

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob

import scala.collection.immutable.Map

object itemStateStorageUsingProxies extends ItemStateStorage {
  override protected type ItemSuperType = ItemExtensionApi
  override protected val clazzOfItemSuperType = classOf[ItemSuperType]

  override protected def uniqueItemSpecification(
      item: ItemSuperType): UniqueItemSpecification =
    item.uniqueItemSpecification

  override protected def lifecycleUUID(item: ItemSuperType): UUID =
    item.lifecycleUUID

  override protected def noteAnnihilationOnItem(item: ItemSuperType): Unit = {
    item
      .asInstanceOf[AnnihilationHook]
      .recordAnnihilation()
  }
}

class TimelineImplementation(lifecyclesState: LifecyclesState =
                               noLifecyclesState(),
                             blobStorage: BlobStorage[SnapshotBlob] =
                               BlobStorageInMemory[SnapshotBlob]())
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
