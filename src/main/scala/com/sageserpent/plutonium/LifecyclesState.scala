package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob
import com.sageserpent.plutonium.PatchRecorder.UpdateConsumer
import com.sageserpent.plutonium.World.{Revision, initialRevision}
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.{
  EventData,
  eventDataOrdering
}
import scalaz.std.list._
import scalaz.syntax.monadPlus._
import scalaz.{-\/, \/-}

import scala.collection.immutable.{Map, TreeMap}
import scala.collection.mutable

trait LifecyclesState[EventId] {

  // NOTE: one might be tempted to think that as a revision of a 'LifecyclesState' also simultaneously revises
  // a 'BlobStorage', then the two things should be fused into one. It seems obvious, doesn't it? - Especially
  // when one looks at 'TimelineImplementation'. Think twice - 'BlobStorage' is revised in several micro-revisions
  // when the 'LifecyclesState' is revised just once. Having said that, I still think that perhaps persistent storage
  // of a 'BlobStorage' can be fused with the persistent storage of a 'LifecyclesState', so perhaps the latter should
  // encapsulate the former in some way - it could hive off a local 'BlobStorageInMemory' as it revises itself, then
  // reabsorb the new blob storage instance back into its own revision. If so, then perhaps 'TimelineImplementation' *is*
  // in fact the cutover form of 'LifecyclesState'. Food for thought...
  def revise(
      events: Map[EventId, Option[Event]],
      blobStorage: BlobStorage[ItemStateUpdate.Key[EventId], SnapshotBlob])
    : (LifecyclesState[EventId],
       BlobStorage[ItemStateUpdate.Key[EventId], SnapshotBlob])

  def retainUpTo(when: Unbounded[Instant]): LifecyclesState[EventId]
}

object noLifecyclesState {
  def apply[EventId](): LifecyclesState[EventId] =
    new LifecyclesStateImplementation[EventId]
}

class LifecyclesStateImplementation[EventId](
    events: Map[EventId, EventData] = Map.empty[EventId, EventData],
    itemStateUpdateKeysByEvent: Map[EventId,
                                    Set[ItemStateUpdate.Key[EventId]]] =
      Map.empty[EventId, Set[ItemStateUpdate.Key[EventId]]],
    nextRevision: Revision = initialRevision)
    extends LifecyclesState[EventId] {
  override def revise(
      events: Map[EventId, Option[Event]],
      blobStorage: BlobStorage[ItemStateUpdate.Key[EventId], SnapshotBlob])
    : (LifecyclesState[EventId],
       BlobStorage[ItemStateUpdate.Key[EventId], SnapshotBlob]) = {
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

    val (itemStateUpdates: Seq[(ItemStateUpdate.Key[EventId], ItemStateUpdate)],
         eventsRelatedByCandidatePatchesToEvent: Map[EventId, Set[EventId]]) =
      createItemStateUpdates(eventsForNewTimeline)

    val eventsMadeObsolete = this.events.keySet intersect events.keySet

    val eventsRelatedByCandidatePatchesToNewEvents =
      (newEvents map (_._1) flatMap eventsRelatedByCandidatePatchesToEvent.get).flatten

    val itemStateUpdateKeysThatNeedToBeRevoked
      : Set[ItemStateUpdate.Key[EventId]] =
      (eventsMadeObsolete ++ eventsRelatedByCandidatePatchesToNewEvents) flatMap itemStateUpdateKeysByEvent.get flatten

    implicit val itemStateUpdateKeyOrdering
      : Ordering[ItemStateUpdate.Key[EventId]] =
      Ordering.by { key: ItemStateUpdate.Key[EventId] =>
        val eventData = eventsForNewTimeline(key.eventId)
        (eventData, key.intraEventIndex)
      }

    val updatePlan =
      UpdatePlan(itemStateUpdateKeysThatNeedToBeRevoked,
                 TreeMap(itemStateUpdates: _*))

    val itemStateUpdateKeysByEventForNewTimeline
      : Map[EventId, Set[ItemStateUpdate.Key[EventId]]] =
      itemStateUpdates flatMap {
        case (itemStateUpdateKey, _) =>
          eventsRelatedByCandidatePatchesToEvent(itemStateUpdateKey.eventId) map (_ -> itemStateUpdateKey)
      } groupBy (_._1) mapValues (_.map(_._2).toSet)

    new LifecyclesStateImplementation[EventId](
      events = eventsForNewTimeline,
      itemStateUpdateKeysByEvent = itemStateUpdateKeysByEventForNewTimeline,
      nextRevision = 1 + nextRevision) -> updatePlan(
      blobStorage,
      whenFor(eventsForNewTimeline))
  }

  private def createItemStateUpdates(
      eventsForNewTimeline: Map[EventId, EventData])
    : (Seq[(ItemStateUpdate.Key[EventId], ItemStateUpdate)],
       Map[EventId, Set[EventId]]) = {
    val eventTimeline = WorldImplementationCodeFactoring.eventTimelineFrom(
      eventsForNewTimeline.toSeq)

    val itemStateUpdatesBuffer
      : mutable.MutableList[(ItemStateUpdate, EventId, Set[EventId])] =
      mutable.MutableList.empty[(ItemStateUpdate, EventId, Set[EventId])]

    val patchRecorder: PatchRecorder[EventId] =
      new PatchRecorderImplementation[EventId](PositiveInfinity())
      with PatchRecorderContracts[EventId] with BestPatchSelectionImplementation
      with BestPatchSelectionContracts {
        override val updateConsumer: UpdateConsumer[EventId] =
          new UpdateConsumer[EventId] {
            override def captureAnnihilation(
                eventId: EventId,
                annihilation: Annihilation): Unit = {
              val itemStateUpdate = ItemStateAnnihilation(annihilation)
              itemStateUpdatesBuffer += ((itemStateUpdate,
                                          eventId,
                                          Set(eventId)))
            }

            override def capturePatch(
                when: Unbounded[Instant],
                eventId: EventId,
                eventIdsFromCandidatePatches: Set[EventId],
                patch: AbstractPatch): Unit = {
              val itemStateUpdate = ItemStatePatch(patch)
              itemStateUpdatesBuffer += ((itemStateUpdate,
                                          eventId,
                                          eventIdsFromCandidatePatches))
            }
          }
      }

    WorldImplementationCodeFactoring.recordPatches(eventTimeline, patchRecorder)

    val itemStateUpdatesGroupedByEventIdPreservingOriginalOrder = (itemStateUpdatesBuffer.zipWithIndex groupBy {
      case ((_, eventId, _), _) => eventId
    }).values.toSeq sortBy (_.head._2) map (_.map(_._1))

    val (itemStateUpdates: Seq[(ItemStateUpdate.Key[EventId], ItemStateUpdate)],
         eventsRelatedByCandidatePatchesToEvent: Seq[(EventId, Set[EventId])]) =
      (itemStateUpdatesGroupedByEventIdPreservingOriginalOrder flatMap (_.zipWithIndex) map {
        case (((itemStateUpdate, eventId, eventIds), intraEventIndex)) =>
          val itemStateUpdateKey =
            ItemStateUpdate.Key(eventId, intraEventIndex)
          (itemStateUpdateKey -> itemStateUpdate) -> (eventId -> eventIds)
      }).unzip

    itemStateUpdates -> eventsRelatedByCandidatePatchesToEvent.toMap
  }

  private def whenFor(eventDataFor: EventId => EventData)(
      key: ItemStateUpdate.Key[EventId]) =
    eventDataFor(key.eventId).serializableEvent.when

  override def retainUpTo(when: Unbounded[Instant]): LifecyclesState[EventId] =
    new LifecyclesStateImplementation[EventId](
      events = this.events filter (when >= _._2.serializableEvent.when),
      itemStateUpdateKeysByEvent = itemStateUpdateKeysByEvent mapValues (_ filter (
          key => when >= whenFor(this.events.apply)(key))) filter {
        case (_, keys) => keys.nonEmpty
      },
      nextRevision = this.nextRevision
    )
}
