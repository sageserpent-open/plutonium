package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{NegativeInfinity, PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import de.sciss.fingertree.RangedSeq

import scala.collection.immutable.Map
import scala.reflect.runtime.universe.TypeTag
import ItemStateUpdateTime.itemStateUpdateTimeOrdering

object AllEventsImplementation {
  // NASTY HACK: we can get away with this for now, as 'Event' currently forbids
  // the booking of events at 'PositiveInfinity'. Yes, this is very hokey.
  val sentinelForEndTimeOfLifecycleWithoutAnnihilation = UpperBoundOfTimeslice(
    PositiveInfinity())

  type LifecycleEndPoints = (ItemStateUpdateTime, ItemStateUpdateTime)

  trait Lifecycle {
    val startTime: ItemStateUpdateTime

    val endTime: ItemStateUpdateTime

    require(Ordering[ItemStateUpdateTime].lt(startTime, endTime))

    val endPoints: LifecycleEndPoints = startTime -> endTime

    val uniqueItemSpecification: UniqueItemSpecification

    val lowerBoundTypeTag = uniqueItemSpecification.typeTag

    val upperBoundTypeTag: TypeTag[_]

    require(lowerBoundTypeTag.tpe <:< upperBoundTypeTag.tpe)

    def isInconsistentWith(another: Lifecycle): Boolean =
      (another.upperBoundTypeTag.tpe <:< this.upperBoundTypeTag.tpe || this.upperBoundTypeTag.tpe <:< another.upperBoundTypeTag.tpe) && !this
        .isFusibleWith(another)

    def isFusibleWith(another: Lifecycle): Boolean =
      this.lowerBoundTypeTag.tpe <:< another.lowerBoundTypeTag.tpe || another.lowerBoundTypeTag.tpe <:< this.lowerBoundTypeTag.tpe

    // NOTE: this is quite defensive, we can answer with 'None' if 'when' is not greater than the start time.
    def retainUpTo(when: Unbounded[Instant]): Option[Lifecycle]
  }

  trait LifecycleContracts extends Lifecycle // TODO - see if we need this...

  implicit val endPoints = (_: Lifecycle).endPoints

  type Lifecycles = RangedSeq[Lifecycle, ItemStateUpdateTime]

  type EventItemFootprint = (ItemStateUpdateTime, Set[Any])
}

class AllEventsImplementation(
    lifecycleSetsInvolvingEvent: Map[
      EventId,
      AllEventsImplementation.EventItemFootprint] = Map.empty,
    lifecyclesById: Map[Any, AllEventsImplementation.Lifecycles] = Map.empty)
    extends AllEvents {
  import AllEvents.EventsRevisionOutcome
  import AllEventsImplementation.Lifecycle

  override type AllEventsType = AllEventsImplementation

  override def revise(events: Map[_ <: EventId, Option[Event]])
    : EventsRevisionOutcome[AllEventsType] = {

    val initialOutcome = EventsRevisionOutcome(
      events = this,
      itemStateUpdateKeysThatNeedToBeRevoked = Set.empty,
      newOrModifiedItemStateUpdates = Map.empty)

    (events :\ initialOutcome) {
      case ((eventId, Some(change: Change)), eventRevisionOutcome) =>
        eventRevisionOutcome.flatMap(
          _.annul(eventId).flatMap(_.record(eventId, change)))
      case ((eventId, Some(measurement: Measurement)), eventRevisionOutcome) =>
        eventRevisionOutcome.flatMap(
          _.annul(eventId).flatMap(_.record(eventId, measurement)))
      case ((eventId, None), eventRevisionOutcome) =>
        eventRevisionOutcome.flatMap(_.annul(eventId))
    }
  }

  override def retainUpTo(when: Unbounded[Instant]): AllEvents = {
    val timespanUpToAndIncludingTheCutoff = LowerBoundOfTimeslice(
      NegativeInfinity()) -> UpperBoundOfTimeslice(when)

    new AllEventsImplementation(
      lifecycleSetsInvolvingEvent = lifecycleSetsInvolvingEvent.filter {
        case (_, (whenEventTakesPlace, _)) =>
          Ordering[ItemStateUpdateTime].lteq(whenEventTakesPlace,
                                             UpperBoundOfTimeslice(when))
      },
      lifecyclesById.mapValues { lifecycles =>
        val (retainedUnchangedLifecycles, retainedTrimmedLifecycles) =
          lifecycles
            .filterIncludes(timespanUpToAndIncludingTheCutoff)
            .partition(lifecycle =>
              Ordering[ItemStateUpdateTime].lteq(lifecycle.endTime,
                                                 UpperBoundOfTimeslice(when)))

        (RangedSeq
          .empty[Lifecycle, ItemStateUpdateTime] /: (retainedUnchangedLifecycles ++ retainedTrimmedLifecycles
          .flatMap(_.retainUpTo(when))))(_ + _)
      }
    )
  }

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
