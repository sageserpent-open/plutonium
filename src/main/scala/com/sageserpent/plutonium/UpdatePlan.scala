package com.sageserpent.plutonium

import java.time.Instant
import java.util.UUID

import com.sageserpent.americium.{NegativeInfinity, Unbounded}
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.QueryCallbackStuff
import resource.makeManagedResource
import scala.reflect.runtime.universe.{Super => _, This => _, _}

import scala.collection.immutable.{Map, SortedMap}
import scala.collection.mutable

case class UpdatePlan[EventId](
    eventsMadeObsolete: Set[EventId],
    updates: SortedMap[Unbounded[Instant],
                       Seq[(Set[EventId], ItemStateUpdate)]]) {
  def apply(blobStorage: BlobStorage[EventId, (Array[Byte], UUID)])
    : BlobStorage[EventId, SnapshotBlob] = {
    val UpdatePlan(eventsMadeObsolete, updates) = this

    var microRevisedBlobStorage = {
      val initialMicroRevisionBuilder = blobStorage.openRevision()

      for (eventId <- eventsMadeObsolete) {
        initialMicroRevisionBuilder.annul(eventId)
      }
      initialMicroRevisionBuilder.build()
    }

    for {
      (when, itemStateUpdates: Seq[(Set[EventId], ItemStateUpdate)]) <- updates
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
          (eventIds, itemStateUpdate) <- itemStateUpdates
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

          microRevisionBuilder.record(eventIds, when, snapshotBlobs.toMap)
        }

        microRevisedBlobStorage = microRevisionBuilder.build()
      }
    }

    microRevisedBlobStorage
  }

}
