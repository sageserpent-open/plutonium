package com.sageserpent.plutonium

import java.time.Instant
import java.util.UUID

import com.sageserpent.americium.{NegativeInfinity, PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.BlobStorage.SnapshotBlob
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

class TimelineImplementation[EventId](
    events: Map[EventId, EventData] = Map.empty[EventId, EventData],
    blobStorage: BlobStorage[EventId] = BlobStorageInMemory.apply[EventId](),
    nextRevision: Revision = initialRevision)
    extends Timeline[EventId] {
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

    val stub =
      SortedMap.empty[Unbounded[Instant], Map[EventId, Seq[ItemStateUpdate]]]

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
      updatePlan: UpdatePlan): BlobStorage[EventId] = {
    val revisionBuilder = blobStorage.openRevision()

    val sourceBlobStorage = {
      val revisionBuilder = blobStorage.openRevision()

      val allEventsThatNeedToBeAnnulledOrRedefined
        : Seq[EventId] = annulledEvents ++ updatePlan.values
        .flatMap(_.flatMap(_._1).distinct)

      for (eventId <- allEventsThatNeedToBeAnnulledOrRedefined) {
        revisionBuilder.annulEvent(eventId)
      }

      revisionBuilder.build()
    }

    for (eventId <- annulledEvents) {
      revisionBuilder.annulEvent(eventId)
    }

    val identifiedItemAccess = new IdentifiedItemAccess
    with itemStateStorageUsingProxies.ReconstitutionContext {
      override def reconstitute(
          uniqueItemSpecification: UniqueItemSpecification) =
        itemFor[Any](uniqueItemSpecification)

      override def forget(
          uniqueItemSpecification: UniqueItemSpecification): Unit = {
        val item = reconstitute(uniqueItemSpecification)
          .asInstanceOf[AnnihilationHook]
          .recordAnnihilation()
        purgeItemFor(uniqueItemSpecification)
      }

      private var blobStorageTimeSlice =
        sourceBlobStorage.timeSlice(NegativeInfinity())

      var allItemsAreLocked = true

      private val itemsMutatedSinceLastSnapshotHarvest =
        mutable.Map.empty[UniqueItemSpecification, ItemExtensionApi]

      def resetSourceTimesliceTo(when: Unbounded[Instant]) = {
        blobStorageTimeSlice = sourceBlobStorage.timeSlice(when)
      }

      override def blobStorageTimeslice: BlobStorage.Timeslice =
        blobStorageTimeSlice

      override protected def createItemFor[Item](
          _uniqueItemSpecification: UniqueItemSpecification) = {
        import QueryCallbackStuff._

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

        proxyFactory.constructFrom[Item](stateToBeAcquiredByProxy)
      }

      def harvestSnapshots()
        : Map[UniqueItemSpecification, (SnapshotBlob, UUID)] = {
        val result = itemsMutatedSinceLastSnapshotHarvest map {
          case (uniqueItemSpecification, item) =>
            val snapshotBlob = itemStateStorageUsingProxies.snapshotFor(item)

            uniqueItemSpecification -> (snapshotBlob, item.lifecycleUUID)
        } toMap

        itemsMutatedSinceLastSnapshotHarvest.clear()

        result
      }

      override def fallbackItemFor[Item](
          uniqueItemSpecification: UniqueItemSpecification): Item =
        createAndStoreItem(uniqueItemSpecification)
    }

    for {
      (when, itemStateUpdates: Seq[(Set[EventId], ItemStateUpdate)]) <- updatePlan
    } {
      identifiedItemAccess.resetSourceTimesliceTo(when)

      for {
        (eventIds, itemStateUpdate) <- itemStateUpdates
      } {
        val snapshotBlobs =
          mutable.Map
            .empty[UniqueItemSpecification, Option[(SnapshotBlob, UUID)]]

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

        revisionBuilder.recordSnapshotBlobsForEvent(eventIds,
                                                    when,
                                                    snapshotBlobs.toMap)
      }
    }

    revisionBuilder.build()
  }

  override def retainUpTo(when: Unbounded[Instant]) =
    this // TODO - support experimental worlds.

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

      override val blobStorageTimeslice: BlobStorage.Timeslice =
        blobStorage.timeSlice(when)

      override def fallbackItemFor[Item](
          uniqueItemSpecification: UniqueItemSpecification): Item =
        throw new RuntimeException(
          s"Snapshot does not exist for: $uniqueItemSpecification at: $when.")

      // TODO - either fuse this back with the other code duplicate above or make it its own thing. Do we really need the 'itemIsLocked'? If we do, then let's fuse...
      override protected def createItemFor[Item](
          _uniqueItemSpecification: UniqueItemSpecification) = {
        import QueryCallbackStuff._

        val stateToBeAcquiredByProxy: AcquiredState =
          new AcquiredState {
            val uniqueItemSpecification: UniqueItemSpecification =
              _uniqueItemSpecification
            def itemIsLocked: Boolean                        = true
            def recordMutation(item: ItemExtensionApi): Unit = {}
          }

        implicit val typeTagForItem: TypeTag[Item] =
          _uniqueItemSpecification.typeTag.asInstanceOf[TypeTag[Item]]

        proxyFactory.constructFrom[Item](stateToBeAcquiredByProxy)
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
