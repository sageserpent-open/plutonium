package com.sageserpent.plutonium

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.AllEvents.ItemStateUpdatesDelta

import java.time.Instant

object AllEvents {
  val noEvents = new AllEventsImplementation()

  case class ItemStateUpdatesDelta[AllEventsType <: AllEvents](
      allEvents: AllEventsType,
      itemStateUpdateKeysThatNeedToBeRevoked: Set[ItemStateUpdateKey],
      newOrModifiedItemStateUpdates: Map[ItemStateUpdateKey, ItemStateUpdate]
  )
}

trait AllEvents {
  type AllEventsType <: AllEvents

  def revise(
      events: collection.Map[_ <: EventId, Option[Event]]
  ): ItemStateUpdatesDelta[AllEventsType]

  def retainUpTo(when: Unbounded[Instant]): AllEvents

  def startOfFollowingLifecycleFor(
      uniqueItemSpecification: UniqueItemSpecification,
      itemStateUpdateKey: ItemStateUpdateTime
  ): Option[ItemStateUpdateKey]
}
