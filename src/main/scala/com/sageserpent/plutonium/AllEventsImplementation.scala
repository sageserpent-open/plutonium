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
    // This creates a single lifecycle based on an actual update.
    def apply(eventId: EventId,
              itemStateUpdateKey: ItemStateUpdateKey,
              itemStateUpdate: ItemStateUpdate): Lifecycle = ???

    // This creates a single lifecycle based on an argument reference.
    def apply(eventId: EventId,
              itemStateUpdateKey: ItemStateUpdateKey,
              uniqueItemSpecification: UniqueItemSpecification): Lifecycle = ???
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

    def isRelevantTo(eventId: EventId): Boolean

    // NOTE: there should be a precondition on this that the event id is relevant to this lifecycle.
    def annul(eventId: EventId): (Option[Lifecycle], Set[ItemStateUpdateKey])

    // NOTE: there should be a precondition that the two lifecycles share the same id.
    def isFusibleWith(another: Lifecycle): Boolean

    // TODO - contracts! Overlapping, fusibility, ids etc.
    def fuseWith(another: Lifecycle): Lifecycle

    // NOTE: these will have best patches applied along with type adjustments, including on the arguments. Hence the crufty argument...
    def itemStateUpdatesByKey(
        uniqueItemSpecificationToTypeTagMap: collection.Map[
          UniqueItemSpecification,
          TypeTag[_]]): Map[ItemStateUpdateKey, ItemStateUpdate]

    // NOTE: the client code merges this across all the lifecycles to provide the argument for the previous method.
    def uniqueItemSpecificationToTypeTagMap
      : collection.Map[UniqueItemSpecification, TypeTag[_]]
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
     * PLAN:
     *
     * We'll build a pair of sets of lifecycles, one for those that have become defunct and the other for those (re)created anew.
     *
     * 1. For *all* the event ids, successively annul the event id, making a new 'LifecyclesById' state. Update the pair of sets for each step.
     *
     * 2. Build simple lifecycles from each new / modified event, both as fully-fledged updates and also as argument references. Add these into the second
     * set in the pair and to the 'LifecyclesById' state.
     *
     * 3. Put all the lifecycles in the second set in the pair on to a priority queue in order of the start time.
     *
     * 4. Iterate through the priority queue, searching for *other* overlapping lifecycles in the 'LifecyclesById' state and either:-
     * i) throw an exception because more than one other lifecycle can fuse with the one in question.
     * ii) fusing with the only other one available - put the result on the priority queue as well as in the 'LifecyclesById' state.
     * iii) leaving it in the 'LifecyclesById' as is, which results in the priority queue getting smaller.
     *
     * At each stage in #4, track which lifecycles go defunct and which are created anew.
     *
     * 5. Take the two sets and compare the item state updates (key / update pairs) from them - these provide the delta.
     * We need to harvest a map of unique item specifications to lowered type tags....
     *
     * */

    val eventIdsToRevoke = events.keys

    ???

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

// TODO: make this fit in with the Grand Master Plan....
  def annul(lifecyclesById: Map[Any, Lifecycles], eventId: EventId) = {
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

    ???

  }
}
