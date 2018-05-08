package com.sageserpent.plutonium

import java.util.UUID

import com.sageserpent.plutonium.BlobStorage.SnapshotRetrievalApi
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob

import scala.collection.immutable.Map
import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag
import scala.util.DynamicVariable

object IdentifiedItemAccessUsingBlobStorage {
  object proxyFactory extends PersistentItemProxyFactory {
    override val proxySuffix: String = "lifecyclesStateProxy"
    override type AcquiredState =
      PersistentItemProxyFactory.AcquiredState
    override val acquiredStateClazz: Class[_ <: AcquiredState] =
      classOf[AcquiredState]
  }
}

trait IdentifiedItemAccessUsingBlobStorage
    extends IdentifiedItemAccess
    with itemStateStorageUsingProxies.ReconstitutionContext {
  override def reconstitute(uniqueItemSpecification: UniqueItemSpecification) =
    itemFor[Any](uniqueItemSpecification)

  protected val blobStorageTimeSlice: SnapshotRetrievalApi[SnapshotBlob]

  val itemStateUpdateKeyOfPatchBeingApplied =
    new DynamicVariable[Option[ItemStateUpdate.Key]](None)

  private var ancestorKeyOfAnnihilatedItem: Option[ItemStateUpdate.Key] = None

  private val itemsMutatedSinceLastHarvest =
    mutable.Map.empty[UniqueItemSpecification, ItemExtensionApi]

  private val itemStateUpdateReadDependenciesDiscoveredSinceLastHarvest
    : mutable.Set[ItemStateUpdate.Key] =
    mutable.Set.empty[ItemStateUpdate.Key]

  override def blobStorageTimeslice: SnapshotRetrievalApi[SnapshotBlob] =
    blobStorageTimeSlice

  override protected def createItemFor[Item](
      _uniqueItemSpecification: UniqueItemSpecification,
      lifecycleUUID: UUID,
      itemStateUpdateKey: Option[ItemStateUpdate.Key]) = {
    import IdentifiedItemAccessUsingBlobStorage.proxyFactory.AcquiredState

    val stateToBeAcquiredByProxy: AcquiredState =
      new PersistentItemProxyFactory.AcquiredState {
        val uniqueItemSpecification: UniqueItemSpecification =
          _uniqueItemSpecification

        def itemIsLocked: Boolean = false

        override def recordAnnihilation(): Unit = {
          ancestorKeyOfAnnihilatedItem = itemStateUpdateKey
          super.recordAnnihilation()
        }

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

    val item = IdentifiedItemAccessUsingBlobStorage.proxyFactory
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
      uniqueItemSpecification: UniqueItemSpecification,
      lifecycleUUID: UUID): Item = {
    val item =
      createItemFor[Item](uniqueItemSpecification, lifecycleUUID, None)
    item.asInstanceOf[AnnihilationHook].recordAnnihilation()
    item
  }

  def apply(patch: AbstractPatch, itemStateUpdateKey: ItemStateUpdate.Key)
    : (Map[UniqueItemSpecification,
           (SnapshotBlob, Option[ItemStateUpdate.Key])],
       Set[ItemStateUpdate.Key]) = {
    itemStateUpdateKeyOfPatchBeingApplied
      .withValue(Some(itemStateUpdateKey)) {
        patch(this)
        patch.checkInvariants(this)
      }

    val ancestorItemStateUpdateKeysOnMutatedItems
      : Map[UniqueItemSpecification, Option[ItemStateUpdate.Key]] =
      itemsMutatedSinceLastHarvest
        .mapValues(
          _.asInstanceOf[ItemStateUpdateKeyTrackingApi].itemStateUpdateKey
        )
        .toMap

    // NOTE: set this *after* performing the update, because mutations caused by the update can themselves
    // discover read dependencies that would otherwise be clobbered by the following block...
    for (item <- itemsMutatedSinceLastHarvest.values) {
      item
        .asInstanceOf[ItemStateUpdateKeyTrackingApi]
        .setItemStateUpdateKey(Some(itemStateUpdateKey))
    }

    // ... but make sure this happens *before* the snapshots are obtained. Imperative code, got to love it, eh!
    val mutationSnapshots: Map[UniqueItemSpecification, SnapshotBlob] =
      itemsMutatedSinceLastHarvest map {
        case (uniqueItemSpecification, item) =>
          val snapshotBlob =
            itemStateStorageUsingProxies.snapshotFor(item)

          uniqueItemSpecification -> snapshotBlob
      } toMap

    val mutatedItemResults: Map[
      UniqueItemSpecification,
      (SnapshotBlob, Option[ItemStateUpdate.Key])] = mutationSnapshots map {
      case (uniqueItemSpecification, snapshot) =>
        uniqueItemSpecification -> (snapshot, ancestorItemStateUpdateKeysOnMutatedItems(
          uniqueItemSpecification))
    }

    val readDependencies: Set[ItemStateUpdate.Key] =
      itemStateUpdateReadDependenciesDiscoveredSinceLastHarvest.toSet

    itemsMutatedSinceLastHarvest.clear()
    itemStateUpdateReadDependenciesDiscoveredSinceLastHarvest.clear()

    mutatedItemResults -> readDependencies
  }

  def apply(annihilation: Annihilation): ItemStateUpdate.Key = {
    annihilation(this)

    val ancestorKey: ItemStateUpdate.Key = ancestorKeyOfAnnihilatedItem.get

    ancestorKeyOfAnnihilatedItem = None

    ancestorKey
  }
}
