package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{NegativeInfinity, PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import de.sciss.fingertree.RangedSeq

import scala.collection.immutable.Map
import scala.reflect.runtime.universe.TypeTag
import ItemStateUpdateTime.ordering
import com.sageserpent.plutonium.AllEvents.ItemStateUpdatesDelta
import com.sageserpent.plutonium.AllEventsImplementation.{
  EventFootprint,
  Lifecycle,
  Lifecycles,
  LifecyclesById
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

    // NOTE: this can (and has to) cope with irrelevant event ids - it simply yields this -> None.
    def annul(eventId: EventId): (Option[Lifecycle], Set[ItemStateUpdateKey])

    def isFusibleWith(another: Lifecycle): Boolean

    // TODO - contracts! Overlapping, fusibility etc.
    def fuseWith(another: Lifecycle): Lifecycle
  }

  trait LifecycleContracts extends Lifecycle // TODO - see if we need this...

  implicit val endPoints = (_: Lifecycle).endPoints

  type Lifecycles = RangedSeq[Lifecycle, ItemStateUpdateTime]

  type LifecyclesById = Map[Any, Lifecycles]

  // NOTE: an event footprint an cover several item state updates, each of which in turn can affect several items.
  case class EventFootprint(when: Unbounded[Instant], itemIds: Set[Any])
}

class AllEventsImplementation(
    lifecycleFootprintPerEvent: Map[EventId,
                                    AllEventsImplementation.EventFootprint] =
      Map.empty,
    lifecyclesById: LifecyclesById = Map.empty)
    extends AllEvents {
  override type AllEventsType = AllEventsImplementation

  override def revise(events: Map[_ <: EventId, Option[Event]])
    : ItemStateUpdatesDelta[AllEventsType] = {

    /*
     * PLAN
     *

     *
     * */

    val eventIdsToRevoke = events.keys

    val initialOutcome = ItemStateUpdatesDelta(
      payload = AnnulmentResult(unchangedLifecycles = this.lifecyclesById,
                                changedLifecycles = Map.empty),
      itemStateUpdateKeysThatNeedToBeRevoked = Set.empty,
      newOrModifiedItemStateUpdates = Map.empty
    )

    val enMasseAnnulmentsResult: ItemStateUpdatesDelta[AnnulmentResult] =
      (initialOutcome /: eventIdsToRevoke) {
        case (outcome, eventId) =>
          for {
            AnnulmentResult(unchangedLifecycles, changedLifecycles) <- outcome
            AnnulmentResult(stillLeftUnchanged, partOne) <- annul(
              unchangedLifecycles,
              eventId)
            AnnulmentResult(partTwo, partThree) <- annul(changedLifecycles,
                                                         eventId)
          } yield {
            AnnulmentResult(unchangedLifecycles = stillLeftUnchanged,
                            changedLifecycles = partOne ++ partTwo ++ partThree)
          }
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

  case class AnnulmentResult(unchangedLifecycles: Map[Any, Lifecycles],
                             changedLifecycles: Map[Any, Lifecycles])

  def annul(lifecyclesById: Map[Any, Lifecycles],
            eventId: EventId): ItemStateUpdatesDelta[AnnulmentResult] = {
    val EventFootprint(when, itemIds) = lifecycleFootprintPerEvent(eventId)

    val timeslice = UpperBoundOfTimeslice(when)

    val (lifecyclesWithRelevantIds: Map[Any, Lifecycles],
         lifecyclesWithIrrelevantIds: Map[Any, Lifecycles]) =
      lifecyclesById.partition {
        case (lifecycleId, lifecycles) => itemIds.contains(lifecycleId)
      }

    val (unchangedLifecycles, changedLifecycles, revokedItemStateUpdateKeys) =
      (lifecyclesWithRelevantIds map {
        case (itemId, lifecycles: Lifecycles) =>
          val lifecyclesIncludingEventTime =
            lifecycles.filterIncludes(timeslice -> timeslice).toSeq

          val otherLifecycles: Lifecycles =
            (lifecycles /: lifecyclesIncludingEventTime)(_ - _)

          val (lifecyclesWithAnnulments, revokedItemStateUpdateKeys) =
            lifecyclesIncludingEventTime.map(_.annul(eventId)).unzip

          (itemId -> otherLifecycles,
           itemId -> RangedSeq[Lifecycle, ItemStateUpdateTime](
             lifecyclesWithAnnulments.flatten: _*),
           (Set.empty[ItemStateUpdateKey] /: revokedItemStateUpdateKeys)(
             _ ++ _))
      }).unzip3

    ItemStateUpdatesDelta(
      payload = AnnulmentResult(unchangedLifecycles = unchangedLifecycles.toMap,
                                changedLifecycles = changedLifecycles.toMap),
      itemStateUpdateKeysThatNeedToBeRevoked =
        (Set.empty[ItemStateUpdateKey] /: revokedItemStateUpdateKeys)(_ ++ _),
      newOrModifiedItemStateUpdates = Map.empty
    )
  }
}
