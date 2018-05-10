package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded

import scala.collection.immutable.Map

class AllEventsSplendidImplementation extends AllEvents {
  import AllEvents.EventsRevisionOutcome

  override type AllEventsType = AllEventsSplendidImplementation

  override def revise(events: Map[_ <: EventId, Option[Event]])
    : EventsRevisionOutcome[AllEventsType] = {

    val initialOutcome = EventsRevisionOutcome(
      events = this,
      itemStateUpdateKeysThatNeedToBeRevoked = Set.empty,
      newOrModifiedItemStateUpdates = Map.empty)

    (events :\ initialOutcome) {
      case ((eventId, Some(change: Change)), eventRevisionOutcome) =>
        eventRevisionOutcome.flatMap(_.record(eventId, change))
      case ((eventId, Some(measurement: Measurement)), eventRevisionOutcome) =>
        eventRevisionOutcome.flatMap(_.record(eventId, measurement))
      case ((eventId, None), eventRevisionOutcome) =>
        eventRevisionOutcome.flatMap(_.annul(eventId))
    }
  }

  override def retainUpTo(when: Unbounded[Instant]): AllEvents = ???

  override def itemStateUpdateTime(
      itemStateUpdateKey: ItemStateUpdate.Key): ItemStateUpdateTime = ???

  def annul(eventId: EventId): EventsRevisionOutcome[AllEventsType] = ???

  def record(eventId: EventId,
             change: Change): EventsRevisionOutcome[AllEventsType] = ???

  def record(eventId: EventId,
             measurement: Measurement): EventsRevisionOutcome[AllEventsType] =
    ???

  def record(eventId: EventId,
             annihilation: Annihilation): EventsRevisionOutcome[AllEventsType] =
    ???

}
/*
package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import com.sageserpent.plutonium.Lifecycle.FusionResult

import scala.reflect.runtime.universe.TypeTag

object Lifecycle {
  val orderingByStartTime: Ordering[Lifecycle] =
    Ordering.by[Lifecycle, Unbounded[Instant]](_.startTime)

  val orderingByEndTime: Ordering[Lifecycle] =
    Ordering.by[Lifecycle, Unbounded[Instant]](
      _.endTime.getOrElse(PositiveInfinity()))

  trait PatchKind

  case object Change extends PatchKind

  case object Measurement extends PatchKind

  def apply(eventId: EventId,
            when: Unbounded[Instant],
            patch: AbstractPatch,
            kind: PatchKind): Lifecycle = ???

  trait FusionResult

  case class Split(first: Lifecycle, second: Lifecycle) extends FusionResult {
    require(first.endTime.fold(false)(_ < second.startTime))
  }

  case class Merge(merged: Lifecycle) extends FusionResult
}

trait Lifecycle {
  import Lifecycle.FusionResult

  val uniqueItemSpecification: UniqueItemSpecification

  val lowerBoundTypeTag = uniqueItemSpecification.typeTag

  val upperBoundTypeTag: TypeTag[_]

  require(lowerBoundTypeTag.tpe <:< upperBoundTypeTag.tpe)

  def isInconsistentWith(another: Lifecycle): Boolean =
    (another.upperBoundTypeTag.tpe <:< this.upperBoundTypeTag.tpe || this.upperBoundTypeTag.tpe <:< another.upperBoundTypeTag.tpe) && !this
      .isFusibleWith(another)

  def isFusibleWith(another: Lifecycle): Boolean =
    this.lowerBoundTypeTag.tpe <:< another.lowerBoundTypeTag.tpe || another.lowerBoundTypeTag.tpe <:< this.lowerBoundTypeTag.tpe

  def overlapsWith(another: Lifecycle): Boolean =
    !(this.endTime.fold(false)(_ < another.startTime) || another.endTime.fold(
      false)(_ < this.startTime))

  val startTime: Unbounded[Instant]

  val endTime: Option[Unbounded[Instant]]

  def fuseWith(another: Lifecycle): FusionResult

  def annul(eventId: EventId): Lifecycle
}

trait LifecycleContracts extends Lifecycle {
  require(endTime.fold(true)(startTime < _))
  require(lowerBoundTypeTag.tpe <:< upperBoundTypeTag.tpe)

  abstract override def fuseWith(another: Lifecycle): FusionResult = {
    require(
      this.uniqueItemSpecification.id == another.uniqueItemSpecification.id)
    require(isFusibleWith(another))
    require(!isInconsistentWith(another))
    require(overlapsWith(another))
    super.fuseWith(another)
  }
}



 */
