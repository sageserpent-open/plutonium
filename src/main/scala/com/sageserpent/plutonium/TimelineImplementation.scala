package com.sageserpent.plutonium

import java.time.Instant
import java.util.UUID

import com.sageserpent.americium.{NegativeInfinity, PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import com.sageserpent.plutonium.PatchRecorder.UpdateConsumer
import com.sageserpent.plutonium.World.{Revision, initialRevision}
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.{
  EventData,
  QueryCallbackStuff
}
import resource.makeManagedResource

import scala.collection.immutable.{Map, SortedMap, TreeMap}
import scala.collection.mutable
import scala.reflect.runtime.universe.{Super => _, This => _, _}
import scalaz.std.list._
import scalaz.syntax.monadPlus._
import scalaz.{-\/, \/-}

object itemStateStorageUsingProxies extends ItemStateStorage {
  override protected type ItemSuperType = ItemExtensionApi
  override protected val clazzOfItemSuperType = classOf[ItemSuperType]

  override protected def uniqueItemSpecification(
      item: ItemSuperType): UniqueItemSpecification =
    item.uniqueItemSpecification

  override protected def lifecycleUUID(item: ItemSuperType): UUID =
    item.lifecycleUUID
}

class TimelineImplementation[EventId](
    events: Map[EventId, EventData] = Map.empty[EventId, EventData],
    blobStorage: BlobStorage[EventId, ItemStateStorage.SnapshotBlob] =
      BlobStorageInMemory.apply[EventId, ItemStateStorage.SnapshotBlob](),
    nextRevision: Revision = initialRevision)
    extends Timeline[EventId] {
  import ItemStateStorage.SnapshotBlob

  sealed trait ItemStateUpdate

  case class ItemStatePatch(patch: AbstractPatch) extends ItemStateUpdate

  case class ItemStateAnnihilation(annihilation: Annihilation)
      extends ItemStateUpdate

  type UpdatePlan =
    SortedMap[Unbounded[Instant], Seq[(Set[EventId], ItemStateUpdate)]]

  override def revise(events: Map[EventId, Option[Event]]) = {
    val (annulledEvents, newEvents) =
      (events.toList map {
        case (eventId, Some(event)) => \/-(eventId -> event)
        case (eventId, None)        => -\/(eventId)
      }).separate

    val eventsForNewTimeline
      : Map[EventId, EventData] = this.events -- annulledEvents ++ newEvents.zipWithIndex.map {
      case ((eventId, event), tiebreakerIndex) =>
        eventId -> EventData(event, nextRevision, tiebreakerIndex)
    }.toMap

    val updatePlan = createUpdatePlan(eventsForNewTimeline)

    val blobStorageForNewTimeline =
      carryOutUpdatePlanInABlazeOfImperativeGlory(annulledEvents, updatePlan)

    new TimelineImplementation(
      events = eventsForNewTimeline,
      blobStorage = blobStorageForNewTimeline,
      nextRevision = 1 + nextRevision
    )
  }

  private def carryOutUpdatePlanInABlazeOfImperativeGlory(
      annulledEvents: List[EventId],
      updatePlan: UpdatePlan): BlobStorage[EventId, SnapshotBlob] = {
    var microRevisedBlobStorage = {
      val initialMicroRevisionBuilder = blobStorage.openRevision()

      val eventsBeingUpdated = updatePlan.values.flatMap(_ flatMap (_._1))
      for (eventId <- annulledEvents ++ eventsBeingUpdated) {
        initialMicroRevisionBuilder.annulEvent(eventId)
      }
      initialMicroRevisionBuilder.build()
    }

    val identifiedItemAccess = new IdentifiedItemAccess
    with itemStateStorageUsingProxies.ReconstitutionContext {
      override def reconstitute(
          uniqueItemSpecification: UniqueItemSpecification) =
        if (forgottenItemSpecifications.contains(uniqueItemSpecification)) {
          forgottenItemSpecifications -= uniqueItemSpecification
          fallbackItemFor[Any](uniqueItemSpecification)
        } else itemFor[Any](uniqueItemSpecification)

      private val forgottenItemSpecifications =
        mutable.Set.empty[UniqueItemSpecification]

      override def forget(
          uniqueItemSpecification: UniqueItemSpecification): Unit = {
        val item = reconstitute(uniqueItemSpecification)
          .asInstanceOf[AnnihilationHook]
          .recordAnnihilation()
        forgottenItemSpecifications += uniqueItemSpecification
      }

      private var blobStorageTimeSlice =
        microRevisedBlobStorage.timeSlice(NegativeInfinity())

      var allItemsAreLocked = true

      private val itemsMutatedSinceLastSnapshotHarvest =
        mutable.Map.empty[UniqueItemSpecification, ItemExtensionApi]

      def resetSourceTimesliceTo(when: Unbounded[Instant]) = {
        blobStorageTimeSlice = microRevisedBlobStorage.timeSlice(when)
      }

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

      override protected def fallbackRelatedItemFor[Item](
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

    for {
      (when, itemStateUpdates: Seq[(Set[EventId], ItemStateUpdate)]) <- updatePlan
    } {
      identifiedItemAccess.resetSourceTimesliceTo(when)

      val timesliceOfBlobStorageBeingRevisedOverall = blobStorage
        .timeSlice(when)

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
                .filter {
                  case (uniqueItemSpecification, snapshot) =>
                    !timesliceOfBlobStorageBeingRevisedOverall
                      .snapshotBlobFor(uniqueItemSpecification)
                      .contains(snapshot)
                }
                .mapValues(Some.apply)
          }

          microRevisionBuilder.recordSnapshotBlobsForEvent(eventIds,
                                                           when,
                                                           snapshotBlobs.toMap)
        }

        microRevisedBlobStorage = microRevisionBuilder.build()
      }
    }

    microRevisedBlobStorage
  }

  override def retainUpTo(when: Unbounded[Instant]) = {
    new TimelineImplementation[EventId](
      events = this.events filter (when >= _._2.serializableEvent.when),
      blobStorage = this.blobStorage.retainUpTo(when),
      nextRevision = this.nextRevision
    )
  }

  override def itemCacheAt(when: Unbounded[Instant]) =
    new ItemCache with itemStateStorageUsingProxies.ReconstitutionContext {
      override def itemsFor[Item: TypeTag](id: Any): Stream[Item] =
        for {
          uniqueItemSpecification <- blobStorageTimeslice.uniqueItemQueriesFor(
            id)
        } yield itemFor[Item](uniqueItemSpecification)

      override def allItems[Item: TypeTag](): Stream[Item] =
        for {
          uniqueItemSpecification <- blobStorageTimeslice
            .uniqueItemQueriesFor[Item]
        } yield itemFor[Item](uniqueItemSpecification)

      override val blobStorageTimeslice: BlobStorage.Timeslice[SnapshotBlob] =
        blobStorage.timeSlice(when)

      override protected def fallbackItemFor[Item](
          uniqueItemSpecification: UniqueItemSpecification): Item =
        throw new RuntimeException(
          s"Snapshot does not exist for: $uniqueItemSpecification at: $when.")

      override protected def fallbackRelatedItemFor[Item](
          uniqueItemSpecification: UniqueItemSpecification): Item = {
        val item =
          createItemFor[Item](uniqueItemSpecification, UUID.randomUUID())
        item.asInstanceOf[AnnihilationHook].recordAnnihilation()
        item
      }

      // TODO - either fuse this back with the other code duplicate above or make it its own thing. Do we really need the 'itemIsLocked'? If we do, then let's fuse...
      override protected def createItemFor[Item](
          _uniqueItemSpecification: UniqueItemSpecification,
          lifecycleUUID: UUID) = {
        import QueryCallbackStuff.{AcquiredState, proxyFactory}

        val stateToBeAcquiredByProxy: AcquiredState =
          new AcquiredState {
            val uniqueItemSpecification: UniqueItemSpecification =
              _uniqueItemSpecification
            def itemIsLocked: Boolean                        = true
            def recordMutation(item: ItemExtensionApi): Unit = {}
          }

        implicit val typeTagForItem: TypeTag[Item] =
          _uniqueItemSpecification.typeTag.asInstanceOf[TypeTag[Item]]

        val item = proxyFactory.constructFrom[Item](stateToBeAcquiredByProxy)

        item
          .asInstanceOf[AnnihilationHook]
          .setLifecycleUUID(lifecycleUUID)

        item
      }
    }

  private def createUpdatePlan(
      eventsForNewTimeline: Map[EventId, EventData]): UpdatePlan = {
    val eventTimeline = WorldImplementationCodeFactoring.eventTimelineFrom(
      eventsForNewTimeline.toSeq)

    val updatePlanBuffer = mutable.SortedMap
      .empty[Unbounded[Instant],
             mutable.MutableList[(Set[EventId], ItemStateUpdate)]]

    val patchRecorder: PatchRecorder[EventId] =
      new PatchRecorderImplementation[EventId](PositiveInfinity())
      with PatchRecorderContracts[EventId] with BestPatchSelectionImplementation
      with BestPatchSelectionContracts {
        override val updateConsumer: UpdateConsumer[EventId] =
          new UpdateConsumer[EventId] {
            private def itemStatesFor(when: Unbounded[Instant]) =
              updatePlanBuffer
                .getOrElseUpdate(when,
                                 mutable.MutableList
                                   .empty[(Set[EventId], ItemStateUpdate)])

            override def captureAnnihilation(
                eventId: EventId,
                annihilation: Annihilation): Unit = {
              itemStatesFor(annihilation.when) += Set(eventId) -> ItemStateAnnihilation(
                annihilation)
            }

            override def capturePatch(when: Unbounded[Instant],
                                      eventIds: Set[EventId],
                                      patch: AbstractPatch): Unit = {
              itemStatesFor(when) += eventIds -> ItemStatePatch(patch)
            }
          }
      }

    WorldImplementationCodeFactoring.recordPatches(eventTimeline, patchRecorder)

    val updatePlan: UpdatePlan =
      TreeMap[Unbounded[Instant], Seq[(Set[EventId], ItemStateUpdate)]](
        updatePlanBuffer.toSeq: _*)

    updatePlan
  }
}
