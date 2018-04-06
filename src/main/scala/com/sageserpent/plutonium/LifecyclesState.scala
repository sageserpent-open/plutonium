package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob
import com.sageserpent.plutonium.ItemStateUpdate.IntraEventIndex
import com.sageserpent.plutonium.PatchRecorder.UpdateConsumer
import com.sageserpent.plutonium.World.{Revision, initialRevision}
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.EventData

import scala.collection.immutable.{Map, SortedMap, TreeMap}
import scala.collection.mutable
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
  def revise(events: Map[EventId, Option[Event]],
             blobStorage: BlobStorage[ItemStateUpdate.Key, SnapshotBlob])
    : (LifecyclesState[EventId], BlobStorage[ItemStateUpdate.Key, SnapshotBlob])

  def retainUpTo(when: Unbounded[Instant]): LifecyclesState[EventId]
}

object noLifecyclesState {
  def apply[EventId](): LifecyclesState[EventId] =
    new LifecyclesStateImplementation[EventId]
}

class LifecyclesStateImplementation[EventId](
    events: Map[EventId, EventData] = Map.empty[EventId, EventData],
    itemStateUpdateKeysByEvent: Map[EventId, Set[ItemStateUpdate.Key]] =
      Map.empty[EventId, Set[ItemStateUpdate.Key]],
    nextRevision: Revision = initialRevision)
    extends LifecyclesState[EventId] {
  override def revise(events: Map[EventId, Option[Event]],
                      blobStorage: BlobStorage[
                        ItemStateUpdate.Key,
                        SnapshotBlob]): (LifecyclesState[EventId],
                                         BlobStorage[ItemStateUpdate.Key,
                                                     SnapshotBlob]) = {
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

    val eventsMadeObsolete = this.events.keySet intersect events.keySet

    val (itemStateUpdates, itemStateUpdateKeysByEventForNewTimeline) =
      createUpdates(eventsForNewTimeline)

    val obsoleteItemStateUpdateKeys
      : Set[ItemStateUpdate.Key] = eventsMadeObsolete flatMap itemStateUpdateKeysByEvent.apply

    val updatePlan =
      UpdatePlan(obsoleteItemStateUpdateKeys, itemStateUpdates)

    new LifecyclesStateImplementation[EventId](
      events = eventsForNewTimeline,
      itemStateUpdateKeysByEvent = itemStateUpdateKeysByEventForNewTimeline,
      nextRevision = 1 + nextRevision) -> updatePlan(blobStorage)
  }

  private def createUpdates(eventsForNewTimeline: Map[EventId, EventData])
    : (Seq[(ItemStateUpdate.Key, ItemStateUpdate)],
       Map[EventId, Set[ItemStateUpdate.Key]]) = {
    val eventTimeline = WorldImplementationCodeFactoring.eventTimelineFrom(
      eventsForNewTimeline.toSeq)

    val itemStateUpdatesByTimeslice = mutable.SortedMap
      .empty[Unbounded[Instant],
             mutable.MutableList[(ItemStateUpdate, Set[EventId])]]

    val patchRecorder: PatchRecorder[EventId] =
      new PatchRecorderImplementation[EventId](PositiveInfinity())
      with PatchRecorderContracts[EventId] with BestPatchSelectionImplementation
      with BestPatchSelectionContracts {
        override val updateConsumer: UpdateConsumer[EventId] =
          new UpdateConsumer[EventId] {
            private def itemStateUpdatesFor(when: Unbounded[Instant]) =
              itemStateUpdatesByTimeslice
                .getOrElseUpdate(when,
                                 mutable.MutableList
                                   .empty[(ItemStateUpdate, Set[EventId])])

            override def captureAnnihilation(
                eventId: EventId,
                annihilation: Annihilation): Unit = {
              val itemStateUpdate = ItemStateAnnihilation(annihilation)
              itemStateUpdatesFor(annihilation.when) += (itemStateUpdate -> Set(
                eventId))
            }

            override def capturePatch(when: Unbounded[Instant],
                                      eventIds: Set[EventId],
                                      patch: AbstractPatch): Unit = {
              val itemStateUpdate = ItemStatePatch(patch)
              itemStateUpdatesFor(when) += (itemStateUpdate -> eventIds)
            }
          }
      }

    WorldImplementationCodeFactoring.recordPatches(eventTimeline, patchRecorder)

    val (itemStateUpdates: Seq[(ItemStateUpdate.Key, ItemStateUpdate)],
         foo: Seq[(ItemStateUpdate.Key, Set[EventId])]) =
      itemStateUpdatesByTimeslice.toSeq.flatMap {
        case (when, itemStateUpdates) =>
          itemStateUpdates.zipWithIndex map {
            case ((itemStateUpdate, eventIds), intraEventIndex) =>
              val itemStateUpdateKey = when -> intraEventIndex
              (itemStateUpdateKey -> itemStateUpdate) -> (itemStateUpdateKey -> eventIds)
          }
      }.unzip

    itemStateUpdates -> (foo flatMap {
      case (itemStateUpdateKey, eventIds) =>
        eventIds.toSeq map (_ -> itemStateUpdateKey)
    } groupBy (_._1)).mapValues(_.map(_._2).toSet)
  }

  override def retainUpTo(when: Unbounded[Instant]): LifecyclesState[EventId] =
    new LifecyclesStateImplementation[EventId](
      events = this.events filter (when >= _._2.serializableEvent.when),
      itemStateUpdateKeysByEvent = itemStateUpdateKeysByEvent mapValues (_.filter {
        case (whenUpdateTakesPlace, _) => when >= whenUpdateTakesPlace
      }) filter { case (_, keys) => keys.nonEmpty },
      nextRevision = this.nextRevision
    )
}
