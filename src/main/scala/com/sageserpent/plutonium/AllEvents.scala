package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.TimelineImplementation.AllEventsImplementation

import scala.collection.immutable.Map

object AllEvents {
  case class EventsRevisionOutcome[AllEventsType <: AllEvents](
      events: AllEventsType,
      itemStateUpdateKeysThatNeedToBeRevoked: Set[ItemStateUpdate.Key],
      newOrModifiedItemStateUpdates: Map[ItemStateUpdate.Key, ItemStateUpdate]) {
    def flatMap(step: AllEventsType => EventsRevisionOutcome[AllEventsType])
      : EventsRevisionOutcome[AllEventsType] = {
      val EventsRevisionOutcome(eventsFromStep,
                                itemStateUpdateKeysThatNeedToBeRevokedFromStep,
                                newOrModifiedItemStateUpdatesFromStep) = step(
        events)
      EventsRevisionOutcome(
        eventsFromStep,
        itemStateUpdateKeysThatNeedToBeRevokedFromStep -- newOrModifiedItemStateUpdates.keys ++ itemStateUpdateKeysThatNeedToBeRevoked,
        newOrModifiedItemStateUpdates -- itemStateUpdateKeysThatNeedToBeRevokedFromStep ++ newOrModifiedItemStateUpdatesFromStep
      )
    }
  }

  val noEvents = new AllEventsImplementation(boringOldEventsMap = Map.empty,
                                             itemStateUpdates = Set.empty)
}

trait AllEvents {
  import AllEvents._
  // TODO: we can get the lifecycle start keys from there too...

  type AllEventsType <: AllEvents

  def revise(events: Map[_ <: EventId, Option[Event]])
    : EventsRevisionOutcome[AllEventsType]

  def retainUpTo(when: Unbounded[Instant]): AllEvents

  def itemStateUpdateTime(
      itemStateUpdateKey: ItemStateUpdate.Key): ItemStateUpdateTime

  def when(itemStateUpdateKey: ItemStateUpdate.Key): Unbounded[Instant] =
    itemStateUpdateTime(itemStateUpdateKey) match {
      case IntraTimesliceTime((when, _, _), _) => when
      case EndOfTimesliceTime(when)            => when
    }
}
