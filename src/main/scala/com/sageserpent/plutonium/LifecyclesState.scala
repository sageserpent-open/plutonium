package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob
import com.sageserpent.plutonium.ItemStateUpdate.IntraEventIndex
import com.sageserpent.plutonium.PatchRecorder.UpdateConsumer
import com.sageserpent.plutonium.World.{Revision, initialRevision}
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.{
  EventData,
  EventOrderingTiebreakerIndex,
  eventDataOrdering
}

import scala.collection.immutable.{Map, SortedMap, TreeMap}
import scala.collection.{immutable, mutable}
import scalaz.std.list._
import scalaz.syntax.monadPlus._
import scalaz.{-\/, \/-}

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
         foo: Map[ItemStateUpdate.Key[EventId], Set[EventId]]) =
      createUpdates(eventsForNewTimeline)

    val itemStateUpdateKeysByEventForNewTimeline
      : Map[EventId, Set[ItemStateUpdate.Key[EventId]]] = (foo flatMap {
      case (itemStateUpdateKey, eventIds) =>
        eventIds.toSeq map (_ -> itemStateUpdateKey)
    } groupBy (_._1)).mapValues(_.map(_._2).toSet)

    val eventsMadeObsolete = this.events.keySet intersect events.keySet

    val obsoleteItemStateUpdateKeys: Set[ItemStateUpdate.Key[EventId]] =
      eventsMadeObsolete flatMap itemStateUpdateKeysByEvent.get flatten

    val relatedEventIdsByEvent: Map[EventId, Set[EventId]] =
      foo.toSeq groupBy (_._1.eventId) map {
        case (eventId, group) => eventId -> group.flatMap(_._2).toSet
      }

    val perturbedItemStateUpdateKeys
      : immutable.Seq[ItemStateUpdate.Key[EventId]] =
      (newEvents map (_._1) flatMap relatedEventIdsByEvent.get).flatten flatMap itemStateUpdateKeysByEvent.get flatten

    implicit val itemStateUpdateKeyOrdering
      : Ordering[ItemStateUpdate.Key[EventId]] =
      Ordering.by { key: ItemStateUpdate.Key[EventId] =>
        val eventData = eventsForNewTimeline(key.eventId)
        (eventData, key.intraEventIndex)
      }

    val updatePlan =
      UpdatePlan(obsoleteItemStateUpdateKeys ++ perturbedItemStateUpdateKeys,
                 TreeMap(itemStateUpdates: _*))

    new LifecyclesStateImplementation[EventId](
      events = eventsForNewTimeline,
      itemStateUpdateKeysByEvent = itemStateUpdateKeysByEventForNewTimeline,
      nextRevision = 1 + nextRevision) -> updatePlan(
      blobStorage,
      whenFor(eventsForNewTimeline))
  }

  private def createUpdates(eventsForNewTimeline: Map[EventId, EventData])
    : (Seq[(ItemStateUpdate.Key[EventId], ItemStateUpdate)],
       Map[ItemStateUpdate.Key[EventId], Set[EventId]]) = {
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

            override def capturePatch(when: Unbounded[Instant],
                                      eventId: EventId,
                                      candidateEventIds: Set[EventId],
                                      patch: AbstractPatch): Unit = {
              val itemStateUpdate = ItemStatePatch(patch)
              itemStateUpdatesBuffer += ((itemStateUpdate,
                                          eventId,
                                          candidateEventIds))
            }
          }
      }

    WorldImplementationCodeFactoring.recordPatches(eventTimeline, patchRecorder)

    val rubbish: Seq[
      (((ItemStateUpdate, EventId, Set[EventId]), Int),
       ItemStateUpdate.IntraEventIndex)] = ((itemStateUpdatesBuffer.zipWithIndex groupBy {
      case ((_, eventId, _), _) => eventId
    }).values flatMap (_.zipWithIndex)).toSeq sortBy (_._1._2)

    val (itemStateUpdates: Seq[(ItemStateUpdate.Key[EventId], ItemStateUpdate)],
         foo: Seq[(ItemStateUpdate.Key[EventId], Set[EventId])]) =
      (rubbish map {
        case ((((itemStateUpdate, eventId, eventIds), _), intraEventIndex)) =>
          val itemStateUpdateKey =
            ItemStateUpdate.Key(eventId, intraEventIndex)
          (itemStateUpdateKey -> itemStateUpdate) -> (itemStateUpdateKey -> eventIds)
      }).unzip

    itemStateUpdates -> foo.toMap
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
