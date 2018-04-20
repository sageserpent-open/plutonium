package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob
import com.sageserpent.plutonium.PatchRecorder.UpdateConsumer
import com.sageserpent.plutonium.World.{Revision, initialRevision}
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.EventData

import scala.collection.immutable.{Map, SortedMap, TreeMap}
import scala.collection.mutable
import scalaz.std.list._
import scalaz.syntax.monadPlus._
import scalaz.{-\/, \/-}

trait LifecyclesState {

  // NOTE: one might be tempted to think that as a revision of a 'LifecyclesState' also simultaneously revises
  // a 'BlobStorage', then the two things should be fused into one. It seems obvious, doesn't it? - Especially
  // when one looks at 'TimelineImplementation'. Think twice - 'BlobStorage' is revised in several micro-revisions
  // when the 'LifecyclesState' is revised just once. Having said that, I still think that perhaps persistent storage
  // of a 'BlobStorage' can be fused with the persistent storage of a 'LifecyclesState', so perhaps the latter should
  // encapsulate the former in some way - it could hive off a local 'BlobStorageInMemory' as it revises itself, then
  // reabsorb the new blob storage instance back into its own revision. If so, then perhaps 'TimelineImplementation' *is*
  // in fact the cutover form of 'LifecyclesState'. Food for thought...
  def revise(events: Map[_ <: EventId, Option[Event]],
             blobStorage: BlobStorage[SnapshotBlob])
    : (LifecyclesState, BlobStorage[SnapshotBlob])

  def retainUpTo(when: Unbounded[Instant]): LifecyclesState
}

object noLifecyclesState {
  def apply(): LifecyclesState =
    new LifecyclesStateImplementation
}

class LifecyclesStateImplementation(events: Map[EventId, EventData] =
                                      Map.empty[EventId, EventData],
                                    nextRevision: Revision = initialRevision)
    extends LifecyclesState {
  override def revise(events: Map[_ <: EventId, Option[Event]],
                      blobStorage: BlobStorage[SnapshotBlob])
    : (LifecyclesState, BlobStorage[SnapshotBlob]) = {
    val (annulledEvents: Seq[EventId], newEvents) =
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
      .asInstanceOf[Set[EventId]]

    val updatePlan =
      UpdatePlan(eventsMadeObsolete, createUpdates(eventsForNewTimeline))

    new LifecyclesStateImplementation(
      events = eventsForNewTimeline,
      nextRevision = 1 + nextRevision) -> updatePlan(blobStorage)
  }

  private def createUpdates(eventsForNewTimeline: Map[EventId, EventData])
    : SortedMap[Unbounded[Instant], Seq[(Set[EventId], ItemStateUpdate)]] = {
    val eventTimeline = WorldImplementationCodeFactoring.eventTimelineFrom(
      eventsForNewTimeline.toSeq)

    val updatePlanBuffer = mutable.SortedMap
      .empty[Unbounded[Instant],
             mutable.MutableList[(Set[EventId], ItemStateUpdate)]]

    val patchRecorder: PatchRecorder =
      new PatchRecorderImplementation(PositiveInfinity())
      with PatchRecorderContracts with BestPatchSelectionImplementation
      with BestPatchSelectionContracts {
        override val updateConsumer: UpdateConsumer =
          new UpdateConsumer {
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
                                      eventIds: Set[_ <: EventId],
                                      patch: AbstractPatch): Unit = {
              itemStatesFor(when) += eventIds
                .asInstanceOf[Set[EventId]] -> ItemStatePatch(patch)
            }
          }
      }

    WorldImplementationCodeFactoring.recordPatches(eventTimeline, patchRecorder)

    TreeMap[Unbounded[Instant], Seq[(Set[EventId], ItemStateUpdate)]](
      updatePlanBuffer.toSeq: _*)
  }

  override def retainUpTo(when: Unbounded[Instant]): LifecyclesState =
    new LifecyclesStateImplementation(
      events = this.events filter (when >= _._2.serializableEvent.when),
      nextRevision = this.nextRevision)
}
