package com.sageserpent.plutonium

import java.time.Instant
import java.util.UUID

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.QueryCallbackStuff
import resource.makeManagedResource

import scala.collection.immutable.{Map, SortedMap}
import scala.collection.mutable
import scala.reflect.runtime.universe.{Super => _, This => _, _}

case class UpdatePlan[EventId](
    obsoleteItemStateUpdateKeys: Set[ItemStateUpdate.Key[EventId]],
    itemStateUpdates: SortedMap[ItemStateUpdate.Key[EventId], ItemStateUpdate]) {
  def apply(blobStorage: BlobStorage[ItemStateUpdate.Key[EventId],
                                     (Array[Byte], UUID)],
            whenFor: ItemStateUpdate.Key[EventId] => Unbounded[Instant])
    : BlobStorage[ItemStateUpdate.Key[EventId], SnapshotBlob] = {
    var microRevisedBlobStorage = {
      val initialMicroRevisionBuilder = blobStorage.openRevision()

      for (itemStateUpdateKey <- obsoleteItemStateUpdateKeys) {
        initialMicroRevisionBuilder.annul(itemStateUpdateKey)
      }
      initialMicroRevisionBuilder.build()
    }

    val itemStateUpdatesGroupedByTimeslice: collection.SortedMap[
      Unbounded[Instant],
      SortedMap[ItemStateUpdate.Key[EventId], ItemStateUpdate]] =
      SortedMap(
        itemStateUpdates groupBy { case (key, _) => whenFor(key) } toSeq: _*)

    for {
      (when, itemStateUpdates) <- itemStateUpdatesGroupedByTimeslice
    } {
      val identifiedItemAccess = new IdentifiedItemAccess
      with itemStateStorageUsingProxies.ReconstitutionContext {
        override def reconstitute(
            uniqueItemSpecification: UniqueItemSpecification) =
          itemFor[Any](uniqueItemSpecification)

        private val blobStorageTimeSlice =
          microRevisedBlobStorage.timeSlice(when, inclusive = false)

        var allItemsAreLocked = true

        private val itemsMutatedSinceLastSnapshotHarvest =
          mutable.Map.empty[UniqueItemSpecification, ItemExtensionApi]

        override def blobStorageTimeslice: BlobStorage.Timeslice[SnapshotBlob] =
          blobStorageTimeSlice

        override protected def createItemFor[Item](
            _uniqueItemSpecification: UniqueItemSpecification,
            lifecycleUUID: UUID) = {
          import QueryCallbackStuff.{AcquiredState, proxyFactory}

          val stateToBeAcquiredByProxy: AcquiredState =
            new AcquiredState {
              val uniqueItemSpecification: UniqueItemSpecification =
                _uniqueItemSpecification
              def itemIsLocked: Boolean = allItemsAreLocked
              def recordMutation(item: ItemExtensionApi): Unit = {
                itemsMutatedSinceLastSnapshotHarvest.update(
                  item.uniqueItemSpecification,
                  item)
              }
            }

          implicit val typeTagForItem: TypeTag[Item] =
            _uniqueItemSpecification.typeTag.asInstanceOf[TypeTag[Item]]

          val item = proxyFactory.constructFrom[Item](stateToBeAcquiredByProxy)

          item
            .asInstanceOf[AnnihilationHook]
            .setLifecycleUUID(lifecycleUUID)

          item
        }

        override protected def fallbackItemFor[Item](
            uniqueItemSpecification: UniqueItemSpecification): Item = {
          val item =
            createAndStoreItem[Item](uniqueItemSpecification, UUID.randomUUID())
          itemsMutatedSinceLastSnapshotHarvest.update(
            uniqueItemSpecification,
            item.asInstanceOf[ItemExtensionApi])
          item
        }

        override protected def fallbackAnnihilatedItemFor[Item](
            uniqueItemSpecification: UniqueItemSpecification): Item = {
          val item =
            createItemFor[Item](uniqueItemSpecification, UUID.randomUUID())
          item.asInstanceOf[AnnihilationHook].recordAnnihilation()
          item
        }

        def harvestSnapshots(): Map[UniqueItemSpecification, SnapshotBlob] = {
          val result = itemsMutatedSinceLastSnapshotHarvest map {
            case (uniqueItemSpecification, item) =>
              val snapshotBlob = itemStateStorageUsingProxies.snapshotFor(item)

              uniqueItemSpecification -> snapshotBlob
          } toMap

          itemsMutatedSinceLastSnapshotHarvest.clear()

          result
        }
      }

      {
        val microRevisionBuilder = microRevisedBlobStorage.openRevision()

        for {
          (itemStateUpdateKey, itemStateUpdate) <- itemStateUpdates
        } {
          val snapshotBlobs =
            mutable.Map
              .empty[UniqueItemSpecification, Option[SnapshotBlob]]

          itemStateUpdate match {
            case ItemStateAnnihilation(annihilation) =>
              annihilation(identifiedItemAccess)
              snapshotBlobs += (annihilation.uniqueItemSpecification -> None)
            case ItemStatePatch(patch) =>
              for (_ <- makeManagedResource {
                     identifiedItemAccess.allItemsAreLocked = false
                   } { _ =>
                     identifiedItemAccess.allItemsAreLocked = true
                   }(List.empty)) {
                patch(identifiedItemAccess)
              }

              patch.checkInvariants(identifiedItemAccess)

              snapshotBlobs ++= identifiedItemAccess
                .harvestSnapshots()
                .mapValues(Some.apply)
          }

          microRevisionBuilder.record(Set(itemStateUpdateKey),
                                      when,
                                      snapshotBlobs.toMap)
        }

        microRevisedBlobStorage = microRevisionBuilder.build()
      }
    }

    microRevisedBlobStorage
  }

}
