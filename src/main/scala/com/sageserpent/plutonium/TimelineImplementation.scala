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
    item.asInstanceOf[LifecycleUUIDApi].lifecycleUUID

  override protected def itemStateUpdateKey[EventId](
      item: ItemExtensionApi): ItemStateUpdate.Key[EventId] =
    item.asInstanceOf[ItemStateUpdateKeyTrackingApi[EventId]].itemStateUpdateKey

  override protected def noteAnnihilationOnItem(item: ItemSuperType): Unit = {
    item
      .asInstanceOf[AnnihilationHook]
      .recordAnnihilation()
  }
}

class TimelineImplementation[EventId](
    lifecyclesState: LifecyclesState[EventId] = noLifecyclesState[EventId](),
    blobStorage: BlobStorage[ItemStateUpdate.Key[EventId],
                             SnapshotBlob[EventId]] =
      BlobStorageInMemory[ItemStateUpdate.Key[EventId],
                          SnapshotBlob[EventId]]())
    extends Timeline[EventId] {

  override def revise(events: Map[EventId, Option[Event]]) = {
    val (newLifecyclesState, blobStorageForNewTimeline) = lifecyclesState
      .revise(events, blobStorage)

    new TimelineImplementation(
      lifecyclesState = newLifecyclesState,
      blobStorage = blobStorageForNewTimeline
    )
  }

  override def retainUpTo(when: Unbounded[Instant]) = {
    new TimelineImplementation[EventId](
      lifecyclesState = this.lifecyclesState.retainUpTo(when),
      blobStorage = this.blobStorage.retainUpTo(when)
    )
  }

  override def itemCacheAt(when: Unbounded[Instant]) =
    new ItemCacheUsingBlobStorage[EventId](blobStorage, when)
}
