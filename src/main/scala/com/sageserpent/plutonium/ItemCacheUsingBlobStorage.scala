package com.sageserpent.plutonium

import java.util.UUID

import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob

object ItemCacheUsingBlobStorage {
  object proxyFactory extends PersistentItemProxyFactory {
    override val proxySuffix: String = "itemCacheProxy"
    override type AcquiredState =
      PersistentItemProxyFactory.AcquiredState
    override val acquiredStateClazz: Class[_ <: AcquiredState] =
      classOf[AcquiredState]
  }
}

class ItemCacheUsingBlobStorage[Time](
    blobStorage: BlobStorage[Time, SnapshotBlob],
    when: Time)
    extends ItemCacheImplementation
    with itemStateStorageUsingProxies.ReconstitutionContext {
  override def itemsFor[Item](
      uniqueItemSpecification: UniqueItemSpecification): Stream[Item] =
    for {
      uniqueItemSpecification <- blobStorageTimeslice.uniqueItemQueriesFor(
        uniqueItemSpecification)
    } yield itemFor[Item](uniqueItemSpecification)

  override def allItems[Item](clazz: Class[Item]): Stream[Item] =
    for {
      uniqueItemSpecification <- blobStorageTimeslice
        .uniqueItemQueriesFor[Item](clazz)
    } yield itemFor[Item](uniqueItemSpecification)

  override val blobStorageTimeslice: BlobStorage.Timeslice[SnapshotBlob] =
    blobStorage.timeSlice(when)

  override protected def fallbackItemFor[Item](
      uniqueItemSpecification: UniqueItemSpecification): Item =
    throw new RuntimeException(
      s"Snapshot does not exist for: $uniqueItemSpecification at: $when.")

  override protected def fallbackAnnihilatedItemFor[Item](
      uniqueItemSpecification: UniqueItemSpecification,
      lifecycleUUID: UUID): Item = {
    val item =
      createItemFor[Item](uniqueItemSpecification, lifecycleUUID, None)
    item.asInstanceOf[AnnihilationHook].recordAnnihilation()
    item
  }

  // TODO - either fuse this back with the other code duplicate above or make it its own thing. Do we really need the 'itemIsLocked'? If we do, then let's fuse...
  override protected def createItemFor[Item](
      _uniqueItemSpecification: UniqueItemSpecification,
      lifecycleUUID: UUID,
      itemStateUpdateKey: Option[ItemStateUpdateKey]) = {
    import ItemCacheUsingBlobStorage.proxyFactory.AcquiredState

    val stateToBeAcquiredByProxy: AcquiredState =
      new PersistentItemProxyFactory.AcquiredState {
        val uniqueItemSpecification: UniqueItemSpecification =
          _uniqueItemSpecification
        def itemIsLocked: Boolean                              = true
        def recordMutation(item: ItemExtensionApi): Unit       = {}
        def recordReadOnlyAccess(item: ItemExtensionApi): Unit = {}
      }

    val item = ItemCacheUsingBlobStorage.proxyFactory
      .constructFrom[Item](stateToBeAcquiredByProxy)

    item
      .asInstanceOf[LifecycleUUIDApi]
      .setLifecycleUUID(lifecycleUUID)

    item
      .asInstanceOf[ItemStateUpdateKeyTrackingApi]
      .setItemStateUpdateKey(itemStateUpdateKey)

    item
  }
}
