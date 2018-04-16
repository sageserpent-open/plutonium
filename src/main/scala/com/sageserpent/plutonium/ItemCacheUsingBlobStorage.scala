package com.sageserpent.plutonium

import java.time.Instant
import java.util.UUID

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob

import scala.reflect.runtime.universe.{Super => _, This => _, _}

object ItemCacheUsingBlobStorage {
  object proxyFactory extends PersistentItemProxyFactory {
    override val proxySuffix: String = "itemCacheProxy"
    override type AcquiredState =
      PersistentItemProxyFactory.AcquiredState[_]
    override val acquiredStateClazz: Class[_ <: AcquiredState] =
      classOf[AcquiredState]
  }
}

class ItemCacheUsingBlobStorage[EventId](
    blobStorage: BlobStorage[ItemStateUpdate.Key[EventId],
                             SnapshotBlob[EventId]],
    when: Unbounded[Instant])
    extends ItemCacheImplementation
    with itemStateStorageUsingProxies.ReconstitutionContext[EventId] {
  override def itemsFor[Item: TypeTag](id: Any): Stream[Item] =
    for {
      uniqueItemSpecification <- blobStorageTimeslice.uniqueItemQueriesFor(id)
    } yield itemFor[Item](uniqueItemSpecification)

  override def allItems[Item: TypeTag](): Stream[Item] =
    for {
      uniqueItemSpecification <- blobStorageTimeslice
        .uniqueItemQueriesFor[Item]
    } yield itemFor[Item](uniqueItemSpecification)

  override val blobStorageTimeslice
    : BlobStorage.Timeslice[SnapshotBlob[EventId]] =
    blobStorage.timeSlice(when)

  override protected def fallbackItemFor[Item](
      uniqueItemSpecification: UniqueItemSpecification): Item =
    throw new RuntimeException(
      s"Snapshot does not exist for: $uniqueItemSpecification at: $when.")

  override protected def fallbackAnnihilatedItemFor[Item](
      uniqueItemSpecification: UniqueItemSpecification): Item = {
    val item =
      createItemFor[Item](uniqueItemSpecification, UUID.randomUUID(), None)
    item.asInstanceOf[AnnihilationHook].recordAnnihilation()
    item
  }

  // TODO - either fuse this back with the other code duplicate above or make it its own thing. Do we really need the 'itemIsLocked'? If we do, then let's fuse...
  override protected def createItemFor[Item](
      _uniqueItemSpecification: UniqueItemSpecification,
      lifecycleUUID: UUID,
      itemStateUpdateKey: Option[ItemStateUpdate.Key[EventId]]) = {
    import ItemCacheUsingBlobStorage.proxyFactory.AcquiredState

    val stateToBeAcquiredByProxy: AcquiredState =
      new PersistentItemProxyFactory.AcquiredState[EventId] {
        val uniqueItemSpecification: UniqueItemSpecification =
          _uniqueItemSpecification
        def itemIsLocked: Boolean                              = true
        def recordMutation(item: ItemExtensionApi): Unit       = {}
        def recordReadOnlyAccess(item: ItemExtensionApi): Unit = {}
      }

    implicit val typeTagForItem: TypeTag[Item] =
      _uniqueItemSpecification.typeTag.asInstanceOf[TypeTag[Item]]

    val item = ItemCacheUsingBlobStorage.proxyFactory
      .constructFrom[Item](stateToBeAcquiredByProxy)

    item
      .asInstanceOf[LifecycleUUIDApi]
      .setLifecycleUUID(lifecycleUUID)

    item
      .asInstanceOf[ItemStateUpdateKeyTrackingApi[EventId]]
      .setItemStateUpdateKey(itemStateUpdateKey)

    item
  }
}
