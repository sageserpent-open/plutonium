package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{NegativeInfinity, PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.BlobStorage.SnapshotBlob
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import com.sageserpent.plutonium.PatchRecorder.UpdateConsumer
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.{
  EventData,
  EventOrderingTiebreakerIndex,
  QueryCallbackStuff
}
import World.{Revision, initialRevision}
import resource._

import scala.collection.immutable.{Map, SortedMap, TreeMap}
import scala.collection.mutable
import scala.reflect.runtime.universe.{Super => _, This => _, _}
import scalaz.std.list._
import scalaz.syntax.monadPlus._
import scalaz.{-\/, \/-}

trait Timeline[EventId] {
  def revise(events: Map[EventId, Option[Event]]): Timeline[EventId]

  def retainUpTo(when: Unbounded[Instant]): Timeline[EventId]

  def itemCacheAt(when: Unbounded[Instant]): ItemCache
}

// TODO - given that we have 'emptyItemCache', I'm not sure if we need this too - let's see how it pans out...
object emptyTimeline {
  def apply[EventId]() = new TimelineImplementation[EventId]
}

object itemStateStorageUsingProxies extends ItemStateStorage {
  override protected type ItemSuperType = ItemExtensionApi
  override protected val clazzOfItemSuperType = classOf[ItemSuperType]

  override protected def uniqueItemSpecification(
      item: ItemSuperType): UniqueItemSpecification =
    item.uniqueItemSpecification
}

class TimelineImplementation[EventId](
    events: Map[EventId, EventData] = Map.empty[EventId, EventData],
    blobStorage: BlobStorage[EventId] = BlobStorageInMemory.apply[EventId](),
    nextRevision: Revision = initialRevision)
    extends Timeline[EventId] {
  sealed trait ItemStateUpdate

  case class ItemStatePatch(patch: AbstractPatch) extends ItemStateUpdate

  case class ItemStateAnnihilation(
      uniqueItemSpecification: UniqueItemSpecification)
      extends ItemStateUpdate

  type UpdatePlan =
    SortedMap[Unbounded[Instant], Map[EventId, Seq[ItemStateUpdate]]]

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

    for (eventId <- annulledEvents) {
      revisionBuilder.annulEvent(eventId)
    }

    val identifiedItemAccess = new IdentifiedItemAccess
    with itemStateStorageUsingProxies.ReconstitutionContext {
      override def reconstitute(
          uniqueItemSpecification: UniqueItemSpecification) =
        itemFor[Any](uniqueItemSpecification)

      private var blobStorageTimeSlice =
        blobStorage.timeSlice(NegativeInfinity())

      var allItemsAreLocked = true

      private val itemsMutatedSinceLastSnapshotHarvest =
        mutable.Map.empty[UniqueItemSpecification, ItemExtensionApi]

      def resetTimesliceTo(when: Unbounded[Instant]) = {
        blobStorageTimeSlice = blobStorage.timeSlice(when)
      }

      override def blobStorageTimeslice: BlobStorage.Timeslice =
        blobStorageTimeSlice

      override protected def createItemFor[Item](
          _uniqueItemSpecification: UniqueItemSpecification) = {
        import QueryCallbackStuff._

        val stateToBeAcquiredByProxy: AcquiredState =
          new AcquiredState(
            _uniqueItemSpecification,
            new NonPersistedAcquiredState {
              def itemIsLocked: Boolean = allItemsAreLocked

              def recordMutation(item: ItemExtensionApi) =
                itemsMutatedSinceLastSnapshotHarvest
                  .update(item.uniqueItemSpecification, item)
            }
          )

        implicit val typeTagForItem: TypeTag[Item] =
          _uniqueItemSpecification.typeTag.asInstanceOf[TypeTag[Item]]

        proxyFactory.constructFrom[Item](stateToBeAcquiredByProxy)
      }

      def harvestSnapshots(): Map[UniqueItemSpecification, SnapshotBlob] = {
        val result = itemsMutatedSinceLastSnapshotHarvest map {
          case (uniqueItemSpecification, item) =>
            val snapshotBlob = itemStateStorageUsingProxies.snapshotFor(item)

            uniqueItemSpecification -> snapshotBlob
        } filter {
          case (uniqueItemSpecification, snapshot) =>
            blobStorageTimeSlice.snapshotBlobFor(uniqueItemSpecification) match {
              case Some(snapshotFromLastRevision) =>
                snapshot != snapshotFromLastRevision
              case None => true
            }
        } toMap

        itemsMutatedSinceLastSnapshotHarvest.clear()

        result
      }

      override def fallbackItemFor[Item](
          uniqueItemSpecification: UniqueItemSpecification): Item =
        createAndStoreItem(uniqueItemSpecification)
    }

    for {
      (when, itemStateUpdates) <- updatePlan
    } {
      identifiedItemAccess.resetTimesliceTo(when)

      for {
        (eventId, itemStateUpdatesForEvent) <- itemStateUpdates
      } {
        val snapshotBlobs =
          mutable.Map.empty[UniqueItemSpecification, Option[SnapshotBlob]]
        for {
          itemStateUpdate <- itemStateUpdatesForEvent
        } {

          itemStateUpdate match {
            case ItemStateAnnihilation(uniqueItemSpecification) =>
              snapshotBlobs += (uniqueItemSpecification -> None)
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
        }

        revisionBuilder.recordSnapshotBlobsForEvent(eventId,
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
          new AcquiredState(
            _uniqueItemSpecification,
            new NonPersistedAcquiredState {
              override def recordMutation(item: ItemExtensionApi): Unit = {}

              override def itemIsLocked: Boolean = true
            })

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
             mutable.Map[EventId, mutable.MutableList[ItemStateUpdate]]]

    val patchRecorder: PatchRecorder[EventId] =
      new PatchRecorderImplementation[EventId](PositiveInfinity())
      with PatchRecorderContracts[EventId] with BestPatchSelectionImplementation
      with BestPatchSelectionContracts {
        override val updateConsumer: UpdateConsumer[EventId] =
          new UpdateConsumer[EventId] {
            private def itemStatesFor(when: Unbounded[Instant],
                                      eventId: EventId) =
              updatePlanBuffer
                .getOrElseUpdate(
                  when,
                  mutable.Map
                    .empty[EventId, mutable.MutableList[ItemStateUpdate]])
                .getOrElseUpdate(eventId,
                                 mutable.MutableList
                                   .empty[ItemStateUpdate])

            override def captureAnnihilation(
                when: Unbounded[Instant],
                eventId: EventId,
                uniqueItemSpecification: UniqueItemSpecification): Unit = {
              itemStatesFor(when, eventId) += ItemStateAnnihilation(
                uniqueItemSpecification)
            }

            override def capturePatch(when: Unbounded[Instant],
                                      eventId: EventId,
                                      patch: AbstractPatch): Unit = {
              itemStatesFor(when, eventId) += ItemStatePatch(patch)
            }
          }
      }

    WorldImplementationCodeFactoring.recordPatches(eventTimeline, patchRecorder)

    require(
      updatePlanBuffer.values flatMap (_.keys) groupBy identity forall (1 == _._2.size),
      "Each event id should only occur once in the update plan.")

    val updatePlan: UpdatePlan =
      TreeMap[Unbounded[Instant], Map[EventId, Seq[ItemStateUpdate]]](
        updatePlanBuffer
          .mapValues(map => Map(map.toSeq: _*))
          .toSeq: _*) // Yuck!

    updatePlan
  }
}
