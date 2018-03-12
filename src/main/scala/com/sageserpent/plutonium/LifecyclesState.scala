package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.LifecyclesState.Dependencies
import com.sageserpent.plutonium.PatchRecorder.UpdateConsumer
import com.sageserpent.plutonium.World.{Revision, initialRevision}
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.EventData

import scala.collection.immutable.{Map, SortedMap, TreeMap}
import scala.collection.mutable
import scalaz.{-\/, \/-}
import scalaz.std.list._
import scalaz.syntax.monadPlus._

object LifecyclesState {
  trait Dependencies

  object noDependencies extends Dependencies
}

trait LifecyclesState[EventId] {

  def revise[Output](
      events: Map[EventId, Option[Event]],
      updatePlanConsumer: (UpdatePlan[EventId]) => (Dependencies, Output))
    : (LifecyclesState[EventId], Output)

  def retainUpTo(when: Unbounded[Instant]): LifecyclesState[EventId]
}

object noLifecyclesState {
  def apply[EventId](): LifecyclesState[EventId] =
    new LifecyclesStateImplementation[EventId]
}

class LifecyclesStateImplementation[EventId](
    events: Map[EventId, EventData] = Map.empty[EventId, EventData],
    nextRevision: Revision = initialRevision)
    extends LifecyclesState[EventId] {
  override def revise[Output](
      events: Map[EventId, Option[Event]],
      updatePlanConsumer: UpdatePlan[EventId] => (Dependencies, Output))
    : (LifecyclesState[EventId], Output) = {
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

    val updatePlan =
      UpdatePlan(annulledEvents.toSet, createUpdates(eventsForNewTimeline))

    val (_, output) = updatePlanConsumer(updatePlan)

    new LifecyclesStateImplementation[EventId](
      events = eventsForNewTimeline,
      nextRevision = 1 + nextRevision) -> output
  }

  private def createUpdates(eventsForNewTimeline: Map[EventId, EventData])
    : SortedMap[Unbounded[Instant], Seq[(Set[EventId], ItemStateUpdate)]] = {
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

    TreeMap[Unbounded[Instant], Seq[(Set[EventId], ItemStateUpdate)]](
      updatePlanBuffer.toSeq: _*)
  }

  override def retainUpTo(when: Unbounded[Instant]): LifecyclesState[EventId] =
    new LifecyclesStateImplementation[EventId](
      events = this.events filter (when >= _._2.serializableEvent.when),
      nextRevision = this.nextRevision)
}
