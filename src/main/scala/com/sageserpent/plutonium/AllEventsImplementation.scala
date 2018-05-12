package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{NegativeInfinity, PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import de.sciss.fingertree.RangedSeq

import scala.collection.immutable.Map
import scala.reflect.runtime.universe.TypeTag
import ItemStateUpdateTime.ordering
import com.sageserpent.plutonium.AllEvents.EventsRevisionOutcome
import com.sageserpent.plutonium.AllEventsImplementation.{
  EventFootprint,
  Lifecycle
}

object AllEventsImplementation {
  val sentinelForEndTimeOfLifecycleWithoutAnnihilation = UpperBoundOfTimeslice(
    PositiveInfinity())

  type LifecycleEndPoints = (ItemStateUpdateTime, ItemStateUpdateTime)

  object Lifecycle {
    def apply(eventId: EventId,
              itemStateUpdateKey: ItemStateUpdateKey,
              itemStateUpdate: ItemStateUpdate): Lifecycle = ???
  }

  trait Lifecycle {
    val startTime: ItemStateUpdateTime

    val endTime: ItemStateUpdateTime

    require(Ordering[ItemStateUpdateTime].lt(startTime, endTime))

    val endPoints: LifecycleEndPoints = startTime -> endTime

    val uniqueItemSpecification: UniqueItemSpecification

    val lowerBoundTypeTag = uniqueItemSpecification.typeTag

    val upperBoundTypeTag: TypeTag[_]

    require(lowerBoundTypeTag.tpe <:< upperBoundTypeTag.tpe)

    def typeBoundsAreInconsistentWith(another: Lifecycle): Boolean =
      (another.upperBoundTypeTag.tpe <:< this.upperBoundTypeTag.tpe || this.upperBoundTypeTag.tpe <:< another.upperBoundTypeTag.tpe) && !this
        .lowerTypeIsConsistentWith(another)

    def lowerTypeIsConsistentWith(another: Lifecycle): Boolean =
      this.lowerBoundTypeTag.tpe <:< another.lowerBoundTypeTag.tpe || another.lowerBoundTypeTag.tpe <:< this.lowerBoundTypeTag.tpe

    // NOTE: this is quite defensive, we can answer with 'None' if 'when' is not greater than the start time.
    def retainUpTo(when: Unbounded[Instant]): Option[Lifecycle]

    def annul(eventId: EventId): Option[Lifecycle]

    def isFusibleWith(another: Lifecycle): Boolean

    // TODO - contracts! Overlapping, fusibility etc.
    def fuseWith(another: Lifecycle): Lifecycle
  }

  trait LifecycleContracts extends Lifecycle // TODO - see if we need this...

  implicit val endPoints = (_: Lifecycle).endPoints

  type Lifecycles = RangedSeq[Lifecycle, ItemStateUpdateTime]

  // NOTE: an event footprint an cover several item state updates, each of which in turn can affect several items.
  case class EventFootprint(when: Unbounded[Instant], itemIds: Set[Any])
}

class AllEventsImplementation(
    lifecycleFootprintPerEvent: Map[EventId,
                                    AllEventsImplementation.EventFootprint] =
      Map.empty,
    lifecyclesById: Map[Any, AllEventsImplementation.Lifecycles] = Map.empty)
    extends AllEvents {
  override type AllEventsType = AllEventsImplementation

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
      case ((eventId, Some(annihilation: Annihilation)),
            eventRevisionOutcome) =>
        eventRevisionOutcome.flatMap(_.record(eventId, annihilation))
      case ((eventId, None), eventRevisionOutcome) =>
        eventRevisionOutcome.flatMap(_.annul(eventId))
    }
  }

  override def retainUpTo(when: Unbounded[Instant]): AllEvents = {
    val cutoff = UpperBoundOfTimeslice(when)

    val timespanUpToAndIncludingTheCutoff = LowerBoundOfTimeslice(
      NegativeInfinity()) -> cutoff

    new AllEventsImplementation(
      lifecycleFootprintPerEvent = lifecycleFootprintPerEvent.filter {
        case (_, EventFootprint(whenEventTakesPlace, _)) =>
          Ordering[ItemStateUpdateTime]
            .lteq(UpperBoundOfTimeslice(whenEventTakesPlace), cutoff)
      },
      lifecyclesById.mapValues { lifecycles =>
        val (retainedUnchangedLifecycles, retainedTrimmedLifecycles) =
          lifecycles
            .filterIncludes(timespanUpToAndIncludingTheCutoff)
            .partition(lifecycle =>
              Ordering[ItemStateUpdateTime].lteq(lifecycle.endTime, cutoff))

        (RangedSeq
          .empty[Lifecycle, ItemStateUpdateTime] /: (retainedUnchangedLifecycles ++ retainedTrimmedLifecycles
          .flatMap(_.retainUpTo(when))))(_ + _)
      }
    )
  }

  def annul(eventId: EventId): EventsRevisionOutcome[AllEventsType] = {
    val EventFootprint(when, itemIds) = lifecycleFootprintPerEvent(eventId)

    val (relevantLifecycles, irrelevantLifecycles) = lifecyclesById.partition {
      case (lifecycleId, _) => itemIds.contains(lifecycleId)
    }

    EventsRevisionOutcome(
      events = new AllEventsImplementation(
        lifecycleFootprintPerEvent = lifecycleFootprintPerEvent - eventId,
        lifecyclesById = irrelevantLifecycles),
      itemStateUpdateKeysThatNeedToBeRevoked = ???,
      newOrModifiedItemStateUpdates = Map.empty
    )
  }

  // TODO - will need to do some fancy footwork for the three overloads below - if an event id migrates off a lifecycle, then that lifecycle
  // needs to be told to annul it. Otherwise we should not annul upfront, at least not before doing lifecycle fusion.
  // TODO - it may be the case that a modified or new lifecycle can be fused with more than one other lifecycle - this is a sign of inconsistency.
  // At least I think it is - but what if several events allow progressive fusion? Ah - we are just dealing with one event here. Hold on ... is
  // this approach correct? Could we allow an entire revision booking to indulge in some mutually self-consistent revision that taken piecemeal might
  // be inconsistent?
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
