package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.AllEvents.ItemStateUpdatesDelta

import scala.collection.immutable.Map

object AllEvents {
  case class ItemStateUpdatesDelta[Payload](
      payload: Payload,
      itemStateUpdateKeysThatNeedToBeRevoked: Set[ItemStateUpdateKey],
      newOrModifiedItemStateUpdates: Map[ItemStateUpdateKey, ItemStateUpdate]) {
    def flatMap(step: Payload => ItemStateUpdatesDelta[Payload])
      : ItemStateUpdatesDelta[Payload] = {
      val ItemStateUpdatesDelta(eventsFromStep,
                                itemStateUpdateKeysThatNeedToBeRevokedFromStep,
                                newOrModifiedItemStateUpdatesFromStep) = step(
        payload)
      ItemStateUpdatesDelta(
        eventsFromStep,
        itemStateUpdateKeysThatNeedToBeRevokedFromStep -- newOrModifiedItemStateUpdates.keys ++ itemStateUpdateKeysThatNeedToBeRevoked,
        newOrModifiedItemStateUpdates -- itemStateUpdateKeysThatNeedToBeRevokedFromStep ++ newOrModifiedItemStateUpdatesFromStep
      )
    }
  }

  val noEvents = new AllEventsImplementation()
}

trait AllEvents {
  // TODO: we can get the lifecycle start keys from there too...

  type AllEventsType <: AllEvents

  def revise(events: Map[_ <: EventId, Option[Event]])
    : ItemStateUpdatesDelta[AllEventsType]

  def retainUpTo(when: Unbounded[Instant]): AllEvents
}
