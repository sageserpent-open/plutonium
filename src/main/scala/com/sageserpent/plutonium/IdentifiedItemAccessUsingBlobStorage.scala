package com.sageserpent.plutonium

import java.util.UUID

import com.sageserpent.plutonium.BlobStorage.Timeslice
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob

import scala.collection.immutable.Map
import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag
import scala.util.DynamicVariable

trait IdentifiedItemAccessUsingBlobStorage
    extends IdentifiedItemAccess
    with itemStateStorageUsingProxies.ReconstitutionContext {
  override def reconstitute(uniqueItemSpecification: UniqueItemSpecification) =
    itemFor[Any](uniqueItemSpecification)

  protected val blobStorageTimeSlice: Timeslice[SnapshotBlob]

  val itemStateUpdateKeyOfPatchBeingApplied =
    new DynamicVariable[Option[ItemStateUpdate.Key]](None)

  private val itemsMutatedSinceLastHarvest =
    mutable.Map.empty[UniqueItemSpecification, ItemExtensionApi]

  private val itemStateUpdateReadDependenciesDiscoveredSinceLastHarvest
    : mutable.Set[ItemStateUpdate.Key] =
    mutable.Set.empty[ItemStateUpdate.Key]

  override def blobStorageTimeslice: BlobStorage.Timeslice[SnapshotBlob] =
    blobStorageTimeSlice

  override protected def createItemFor[Item](
      _uniqueItemSpecification: UniqueItemSpecification,
      lifecycleUUID: UUID,
      itemStateUpdateKey: Option[ItemStateUpdate.Key]) = {
    import LifecyclesStateImplementation.proxyFactory.AcquiredState

    val stateToBeAcquiredByProxy: AcquiredState =
      new PersistentItemProxyFactory.AcquiredState {
        val uniqueItemSpecification: UniqueItemSpecification =
          _uniqueItemSpecification

        def itemIsLocked: Boolean = false

        override def recordMutation(item: ItemExtensionApi): Unit = {
          itemsMutatedSinceLastHarvest.update(item.uniqueItemSpecification,
                                              item)
        }

        override def recordReadOnlyAccess(item: ItemExtensionApi): Unit = {
          val itemStateUpdateKeyThatLastUpdatedItem = item
            .asInstanceOf[ItemStateUpdateKeyTrackingApi]
            .itemStateUpdateKey

          assert(
            itemStateUpdateKeyOfPatchBeingApplied.value != itemStateUpdateKeyThatLastUpdatedItem)

          itemStateUpdateKeyThatLastUpdatedItem match {
            case Some(key) =>
              itemStateUpdateReadDependenciesDiscoveredSinceLastHarvest += key
            case None =>
          }

        }
      }

    implicit val typeTagForItem: TypeTag[Item] =
      _uniqueItemSpecification.typeTag.asInstanceOf[TypeTag[Item]]

    val item = LifecyclesStateImplementation.proxyFactory
      .constructFrom[Item](stateToBeAcquiredByProxy)

    item
      .asInstanceOf[LifecycleUUIDApi]
      .setLifecycleUUID(lifecycleUUID)

    item
      .asInstanceOf[ItemStateUpdateKeyTrackingApi]
      .setItemStateUpdateKey(itemStateUpdateKey)

    item
  }

  override protected def fallbackItemFor[Item](
      uniqueItemSpecification: UniqueItemSpecification): Item = {
    val item =
      createAndStoreItem[Item](uniqueItemSpecification, UUID.randomUUID(), None)
    itemsMutatedSinceLastHarvest.update(uniqueItemSpecification,
                                        item.asInstanceOf[ItemExtensionApi])
    item
  }

  override protected def fallbackAnnihilatedItemFor[Item](
      uniqueItemSpecification: UniqueItemSpecification): Item = {
    val item =
      createItemFor[Item](uniqueItemSpecification, UUID.randomUUID(), None)
    item.asInstanceOf[AnnihilationHook].recordAnnihilation()
    item
  }

  def apply(patch: AbstractPatch, itemStateUpdateKey: ItemStateUpdate.Key)
    : (Map[UniqueItemSpecification, SnapshotBlob], Set[ItemStateUpdate.Key]) = {
    itemStateUpdateKeyOfPatchBeingApplied
      .withValue(Some(itemStateUpdateKey)) {
        patch(this)
        patch.checkInvariants(this)
      }

    val readDependencies: Set[ItemStateUpdate.Key] =
      itemStateUpdateReadDependenciesDiscoveredSinceLastHarvest.toSet

    // NOTE: set this *after* getting the read dependencies, because mutations caused by the item state update being applied
    // can themselves discover read dependencies that would otherwise be clobbered by the following block...
    for (item <- itemsMutatedSinceLastHarvest.values) {
      item
        .asInstanceOf[ItemStateUpdateKeyTrackingApi]
        .setItemStateUpdateKey(Some(itemStateUpdateKey))
    }

    // ... but make sure this happens *before* the snapshots are obtained. Imperative code, got to love it, eh!
    val mutationSnapshots = itemsMutatedSinceLastHarvest map {
      case (uniqueItemSpecification, item) =>
        val snapshotBlob =
          itemStateStorageUsingProxies.snapshotFor(item)

        uniqueItemSpecification -> snapshotBlob
    } toMap

    itemsMutatedSinceLastHarvest.clear()
    itemStateUpdateReadDependenciesDiscoveredSinceLastHarvest.clear()

    mutationSnapshots -> readDependencies
  }
}
