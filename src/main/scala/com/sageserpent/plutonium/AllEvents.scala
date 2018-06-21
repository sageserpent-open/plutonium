package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.AllEvents.ItemStateUpdatesDelta
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification

import scala.collection.immutable.Map

object AllEvents {
  case class ItemStateUpdatesDelta[AllEventsType <: AllEvents](
      allEvents: AllEventsType,
      itemStateUpdateKeysThatNeedToBeRevoked: Set[ItemStateUpdateKey],
      newOrModifiedItemStateUpdates: Map[ItemStateUpdateKey, ItemStateUpdate])

  val noEvents = new AllEventsImplementation()
}

trait AllEvents {
  // TODO: we can get the lifecycle start keys from there too...

  type AllEventsType <: AllEvents

  def revise(events: Map[_ <: EventId, Option[Event]])
    : ItemStateUpdatesDelta[AllEventsType]

  def retainUpTo(when: Unbounded[Instant]): AllEvents

  def startOfFollowingLifecycleFor(
      uniqueItemSpecification: UniqueItemSpecification,
      itemStateUpdateKey: ItemStateUpdateTime): Option[ItemStateUpdateKey]
}
