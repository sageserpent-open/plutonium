package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{
  Finite,
  NegativeInfinity,
  PositiveInfinity,
  Unbounded
}
import com.sageserpent.plutonium.AllEvents.ItemStateUpdatesDelta
import com.sageserpent.plutonium.AllEventsImplementation.Lifecycle.{
  EndOfLifecycle,
  IndivisibleEvent,
  bagConfiguration
}
import com.sageserpent.plutonium.AllEventsImplementation.{
  EventFootprint,
  Lifecycle,
  LifecyclesById,
  noLifecycles
}
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import com.sageserpent.plutonium.ItemStateUpdateTime.ordering
import com.sageserpent.plutonium.World.{Revision, initialRevision}
import de.sciss.fingertree.RangedSeq
import de.ummels.prioritymap.PriorityMap

import scala.annotation.tailrec
import scala.collection.immutable.{Bag, HashedBagConfiguration, Map, SortedMap}
import scala.reflect.runtime.universe.TypeTag

object AllEventsImplementation {
  // TODO - can we get rid of this? As long as the support for a closed-open interval exists, maybe we don't need an explicit end time?
  val sentinelForEndTimeOfLifecycleWithoutAnnihilation = UpperBoundOfTimeslice(
    PositiveInfinity())

  type LifecycleEndPoints = (ItemStateUpdateTime, ItemStateUpdateTime)

  object Lifecycle {
    implicit val bagConfiguration = HashedBagConfiguration.compact[TypeTag[_]]

    def fromChange(eventId: EventId,
                   itemStateUpdateKey: ItemStateUpdateKey,
                   patch: AbstractPatch): Lifecycle =
      new Lifecycle(eventId = eventId,
                    itemStateUpdateKey = itemStateUpdateKey,
                    indivisibleEvent = IndivisibleChange(patch))

    def fromMeasurement(eventId: EventId,
                        itemStateUpdateKey: ItemStateUpdateKey,
                        patch: AbstractPatch): Lifecycle =
      new Lifecycle(eventId = eventId,
                    itemStateUpdateKey = itemStateUpdateKey,
                    indivisibleEvent = IndivisibleMeasurement(patch))

    def fromAnnihilation(
        eventId: EventId,
        itemStateUpdateKey: ItemStateUpdateKey,
        uniqueItemSpecification: UniqueItemSpecification): Lifecycle =
      new Lifecycle(eventId = eventId,
                    itemStateUpdateKey = itemStateUpdateKey,
                    indivisibleEvent = EndOfLifecycle(uniqueItemSpecification))

    def fromArgumentTypeReference(
        eventId: EventId,
        itemStateUpdateKey: ItemStateUpdateKey,
        uniqueItemSpecification: UniqueItemSpecification): Lifecycle = {
      new Lifecycle(eventId = eventId,
                    itemStateUpdateKey = itemStateUpdateKey,
                    indivisibleEvent =
                      ArgumentReference(uniqueItemSpecification))
    }

    sealed trait IndivisibleEvent {
      def uniqueItemSpecification: UniqueItemSpecification
    }

    case class ArgumentReference(
        override val uniqueItemSpecification: UniqueItemSpecification)
        extends IndivisibleEvent {}

    case class IndivisibleChange(patch: AbstractPatch)
        extends IndivisibleEvent {
      override def uniqueItemSpecification: UniqueItemSpecification =
        patch.targetItemSpecification
    }

    case class IndivisibleMeasurement(patch: AbstractPatch)
        extends IndivisibleEvent {
      override def uniqueItemSpecification: UniqueItemSpecification =
        patch.targetItemSpecification
    }

    case class EndOfLifecycle(
        override val uniqueItemSpecification: UniqueItemSpecification)
        extends IndivisibleEvent {}
  }

  class Lifecycle(
      typeTags: Bag[TypeTag[_]],
      eventsArrangedInTimeOrder: SortedMap[ItemStateUpdateTime,
                                           IndivisibleEvent],
      itemStateUpdateTimesByEventId: Map[EventId, Set[ItemStateUpdateTime]]) {
    require(typeTags.nonEmpty)

    require(eventsArrangedInTimeOrder.nonEmpty)

    require(eventsArrangedInTimeOrder.init.forall {
      case (_, indivisibleEvent) =>
        indivisibleEvent match {
          case _: EndOfLifecycle => false
          case _                 => true
        }
    })

    require(
      itemStateUpdateTimesByEventId.nonEmpty && itemStateUpdateTimesByEventId
        .forall { case (_, times) => times.nonEmpty })

    def this(eventId: EventId,
             itemStateUpdateKey: ItemStateUpdateKey,
             indivisibleEvent: IndivisibleEvent) {
      this(
        typeTags = Bag(indivisibleEvent.uniqueItemSpecification.typeTag),
        eventsArrangedInTimeOrder = SortedMap(
          (itemStateUpdateKey: ItemStateUpdateTime) -> indivisibleEvent),
        itemStateUpdateTimesByEventId =
          Map(eventId -> Set(itemStateUpdateKey: ItemStateUpdateTime))
      )
    }

    val startTime: ItemStateUpdateTime = eventsArrangedInTimeOrder.firstKey

    // TODO: the way annihilations are just mixed in with all the other events in 'eventsArrangedInTimeOrder' feels hokey:
    // it necessitates a pesky invariant check, along with the special case logic below. Sort this out!
    val endTime: ItemStateUpdateTime = eventsArrangedInTimeOrder.last match {
      case (itemStateUpdateTime, _: EndOfLifecycle) => itemStateUpdateTime
      case _                                        => sentinelForEndTimeOfLifecycleWithoutAnnihilation
    }

    require(Ordering[ItemStateUpdateTime].lt(startTime, endTime))

    val endPoints: LifecycleEndPoints = startTime -> endTime

    def overlapsWith(another: Lifecycle): Boolean =
      Ordering[ItemStateUpdateTime]
        .lteq(this.startTime, another.endTime) && Ordering[ItemStateUpdateTime]
        .lteq(another.startTime, this.endTime)

    val uniqueItemSpecification: UniqueItemSpecification =
      UniqueItemSpecification(id, lowerBoundTypeTag)

    def id: Any = eventsArrangedInTimeOrder.head._2.uniqueItemSpecification.id

    def lowerBoundTypeTag: TypeTag[_] = typeTags.distinct.reduce[TypeTag[_]] {
      case (first, second) => if (first.tpe <:< second.tpe) first else second
    }

    def upperBoundTypeTag: TypeTag[_] = typeTags.distinct.reduce[TypeTag[_]] {
      case (first, second) => if (first.tpe <:< second.tpe) second else first
    }

    require(lowerBoundTypeTag.tpe <:< upperBoundTypeTag.tpe)

    def typeBoundsAreInconsistentWith(another: Lifecycle): Boolean =
      // NOTE: the conjunction of the negatives of the two sub-predicates isn't checked. Think of
      // multiple inheritance of interfaces in Java and trait mixins in Scala; you'll see why.
      this.upperTypeIsConsistentWith(another) && !this
        .lowerTypeIsConsistentWith(another)

    def lowerTypeIsConsistentWith(another: Lifecycle): Boolean =
      this.lowerBoundTypeTag.tpe <:< another.lowerBoundTypeTag.tpe || another.lowerBoundTypeTag.tpe <:< this.lowerBoundTypeTag.tpe

    def upperTypeIsConsistentWith(another: Lifecycle): Boolean =
      another.upperBoundTypeTag.tpe <:< this.upperBoundTypeTag.tpe || this.upperBoundTypeTag.tpe <:< another.upperBoundTypeTag.tpe

    // NOTE: this is quite defensive, we can answer with 'None' if 'when' is not greater than the start time.
    def retainUpTo(when: Unbounded[Instant]): Option[Lifecycle] = {
      val cutoff = UpperBoundOfTimeslice(when)

      val (retainedEvents, trimmedEvents) =
        eventsArrangedInTimeOrder.partition {
          case (itemStateUpdateTime, _) =>
            Ordering[ItemStateUpdateTime].gteq(cutoff, itemStateUpdateTime)
        }

      if (retainedEvents.nonEmpty) {
        val retainedTypeTags = (typeTags /: trimmedEvents) {
          case (typeTags, (_, trimmedEvent)) =>
            typeTags - trimmedEvent.uniqueItemSpecification.typeTag
        }

        // TODO - consider just using 'itemStateUpdateTimesByEventId' - this will result in
        // extraneous entries, however 'annul' can be made to tolerate these. Just a thought...
        val retainedItemStateUpdateTimesByEventId =
          itemStateUpdateTimesByEventId
            .mapValues(_.filter(Ordering[ItemStateUpdateTime].gteq(cutoff, _)))
            .filter {
              case (_, itemStateUpdateTimes) => itemStateUpdateTimes.nonEmpty
            }

        Some(
          new Lifecycle(typeTags = retainedTypeTags,
                        eventsArrangedInTimeOrder = retainedEvents,
                        itemStateUpdateTimesByEventId =
                          retainedItemStateUpdateTimesByEventId))
      } else None
    }

    def isRelevantTo(eventId: EventId): Boolean =
      itemStateUpdateTimesByEventId.contains(eventId)

    def annul(eventId: EventId): Option[Lifecycle] = {
      itemStateUpdateTimesByEventId.get(eventId) match {
        case Some(itemStateUpdateTimes) =>
          val preservedEvents =
            (eventsArrangedInTimeOrder /: itemStateUpdateTimes)(_ - _)
          if (preservedEvents.nonEmpty) {
            val annulledEvents = itemStateUpdateTimes map (eventsArrangedInTimeOrder.apply)
            val preservedTypeTags = (typeTags /: annulledEvents) {
              case (typeTags, annulledEvent) =>
                typeTags - annulledEvent.uniqueItemSpecification.typeTag
            }
            val preservedItemStateUpdateTimesByEventId = itemStateUpdateTimesByEventId - eventId
            Some(
              new Lifecycle(typeTags = preservedTypeTags,
                            eventsArrangedInTimeOrder = preservedEvents,
                            itemStateUpdateTimesByEventId =
                              preservedItemStateUpdateTimesByEventId))
          } else None
        case None => Some(this)
      }
    }

    // The lower type bounds are compatible and there is overlap.
    def isFusibleWith(another: Lifecycle): Boolean =
      // NOTE: there is no check on upper types. Think of multiple inheritance
      // of interfaces in Java and trait mixins in Scala; you'll see why.
      this.lowerTypeIsConsistentWith(another) && this.overlapsWith(another)

    def fuseWith(another: Lifecycle): Lifecycle = ???

    // NOTE: these will have best patches applied along with type adjustments, including on the arguments. That's why
    // 'lifecyclesById' is provided - although any reference to the same unique item as that referenced by the receiver
    // lifecycle will ignore the argument and use the receiver lifecycle. The item state update key of the update is used
    // to select the correct lifecycle to resolve an argument type, if there is more than one lifecycle for that unique item.
    def itemStateUpdates(lifecyclesById: LifecyclesById)
      : Set[(ItemStateUpdateKey, ItemStateUpdate)] = ???
  }

  trait LifecycleContracts extends Lifecycle {
    abstract override def annul(eventId: EventId): Option[Lifecycle] = {
      require(isRelevantTo(eventId))
      super.annul(eventId)
    }

    abstract override def isFusibleWith(another: Lifecycle): Boolean = {
      require(this.id == another.id)
      super.isFusibleWith(another)
    }

    abstract override def fuseWith(another: Lifecycle): Lifecycle = {
      require(isFusibleWith(another))
      super.fuseWith(another)
    }
  }

  implicit val endPoints = (lifecycle: Lifecycle) =>
    Split.alignedWith(lifecycle.startTime) -> Split.upperBoundOf(
      lifecycle.endTime)

  type Lifecycles = RangedSeq[Lifecycle, Split[ItemStateUpdateTime]]

  val noLifecycles = RangedSeq.empty[Lifecycle, Split[ItemStateUpdateTime]]

  type LifecyclesById = Map[Any, Lifecycles]

  // NOTE: an event footprint an cover several item state updates, each of which in turn can affect several items.
  case class EventFootprint(when: Unbounded[Instant], itemIds: Set[Any])
}

class AllEventsImplementation(
    nextRevision: Revision = initialRevision,
    lifecycleFootprintPerEvent: Map[EventId,
                                    AllEventsImplementation.EventFootprint] =
      Map.empty,
    lifecyclesById: LifecyclesById = Map.empty)
    extends AllEvents {
  override type AllEventsType = AllEventsImplementation

  override def revise(events: Map[_ <: EventId, Option[Event]])
    : ItemStateUpdatesDelta[AllEventsType] = {
    case class CalculationState(defunctLifecycles: Set[Lifecycle],
                                newLifecycles: Set[Lifecycle],
                                lifecyclesById: LifecyclesById) {
      require(defunctLifecycles.intersect(newLifecycles).isEmpty)

      def flatMap(
          step: LifecyclesById => CalculationState): CalculationState = {
        val CalculationState(defunctLifecyclesFromStep,
                             newLifecyclesFromStep,
                             lifecyclesByIdFromStep) = step(lifecyclesById)

        CalculationState(
          defunctLifecycles = defunctLifecycles -- newLifecyclesFromStep ++ defunctLifecyclesFromStep,
          newLifecycles = newLifecycles -- defunctLifecyclesFromStep ++ newLifecyclesFromStep,
          lifecyclesById = lifecyclesByIdFromStep
        )
      }

      @tailrec
      final def fuseLifecycles(
          priorityQueueOfLifecyclesConsideredForFusionOrAddition: PriorityMap[
            Lifecycle,
            ItemStateUpdateTime] = PriorityMap[Lifecycle, ItemStateUpdateTime](
            newLifecycles.toSeq.map(lifecycle =>
              lifecycle -> lifecycle.startTime): _*)): CalculationState = {
        if (priorityQueueOfLifecyclesConsideredForFusionOrAddition.nonEmpty) {
          val (candidateForFusion, startTimeOfCandidate) =
            priorityQueueOfLifecyclesConsideredForFusionOrAddition.head

          val itemId = candidateForFusion.id

          val overlappingLifecycles = lifecyclesById
            .get(itemId)
            .fold(Iterator.empty: Iterator[Lifecycle]) { lifecycle => // NASTY HACK - the 'RangedSeq' API has a strange way of dealing with intervals - have to work around it here...
              val startTime = Split.alignedWith(candidateForFusion.startTime)
              val endTime   = Split.alignedWith(candidateForFusion.endTime)
              (lifecycle.filterOverlaps(candidateForFusion) ++ lifecycle
                .filterIncludes(startTime -> startTime) ++ lifecycle
                .filterIncludes(endTime   -> endTime))
                .filterNot(_ == candidateForFusion)
            }

          val conflictingLifecycles = overlappingLifecycles.filter(
            _.typeBoundsAreInconsistentWith(candidateForFusion))

          if (conflictingLifecycles.nonEmpty) {
            throw new RuntimeException(
              s"There is at least one item of id: '${itemId}' that would be inconsistent with type '${candidateForFusion.lowerBoundTypeTag.tpe}', these have types: '${conflictingLifecycles map (_.lowerBoundTypeTag.tpe)}'.")
          }

          val fusibleLifecycles =
            overlappingLifecycles
              .filter(_.isFusibleWith(candidateForFusion))
              .toList

          fusibleLifecycles match {
            case Nil =>
              this.fuseLifecycles(
                priorityQueueOfLifecyclesConsideredForFusionOrAddition.drop(1))
            case matchForFusion :: Nil =>
              val fusedLifecycle = candidateForFusion.fuseWith(matchForFusion)
              val nextState = CalculationState(
                defunctLifecycles = Set(candidateForFusion, matchForFusion),
                newLifecycles = Set(fusedLifecycle),
                lifecyclesById = lifecyclesById.updated(
                  itemId,
                  lifecyclesById(itemId) - matchForFusion - candidateForFusion + fusedLifecycle)
              )

              nextState.fuseLifecycles(
                priorityQueueOfLifecyclesConsideredForFusionOrAddition
                  .drop(1) + (fusedLifecycle -> fusedLifecycle.startTime))
            case _ =>
              throw new scala.RuntimeException(
                s"There is more than one item of id: '${itemId}' compatible with type '${candidateForFusion.lowerBoundTypeTag.tpe}', these have types: '${fusibleLifecycles map (_.lowerBoundTypeTag.tpe)}'.")
          }
        } else this
      }
    }

    def annul(lifecyclesById: LifecyclesById,
              eventId: EventId): CalculationState = {
      val EventFootprint(when, itemIds) = lifecycleFootprintPerEvent(eventId)

      val timeslice = UpperBoundOfTimeslice(when)

      val (lifecyclesWithRelevantIds: LifecyclesById,
           lifecyclesWithIrrelevantIds: LifecyclesById) =
        lifecyclesById.partition {
          case (lifecycleId, lifecycles) => itemIds.contains(lifecycleId)
        }

      val (lifecyclesByIdWithAnnulments, changedLifecycles, defunctLifecycles) =
        (lifecyclesWithRelevantIds map {
          case (itemId, lifecycles) =>
            val lifecyclesIncludingEventTime =
              lifecycles.intersect(Split.alignedWith(timeslice)).toSeq

            val lifecyclesWithAnnulments =
              lifecyclesIncludingEventTime.flatMap(_.annul(eventId))

            val otherLifecycles =
              (lifecycles /: lifecyclesIncludingEventTime)(_ - _)

            (itemId -> (otherLifecycles /: lifecyclesWithAnnulments)(_ + _),
             lifecyclesWithAnnulments,
             lifecyclesIncludingEventTime)
        }).unzip3

      CalculationState(
        defunctLifecycles = defunctLifecycles.flatten.toSet,
        newLifecycles = (Set.empty[Lifecycle] /: changedLifecycles)(_ ++ _),
        lifecyclesById = lifecyclesByIdWithAnnulments.toMap
      )
    }

    val initialCalculationState = CalculationState(
      defunctLifecycles = Set.empty,
      newLifecycles = Set.empty,
      lifecyclesById = lifecyclesById)

    val eventIdsToRevoke = events.keys

    val calculationStateAfterAnnulments =
      (initialCalculationState /: eventIdsToRevoke) {
        case (calculationState, eventId) =>
          calculationState.flatMap(annul(_, eventId))
      }

    val newAndModifiedEvents: Map[EventId, Event] = events.collect {
      case (eventId, Some(event)) => eventId -> event
    }

    val simpleLifecyclesForNewAndModifiedEvents =
      buildSimpleLifecyclesFrom(newAndModifiedEvents)

    def addLifecycle(lifecyclesById: LifecyclesById,
                     lifecycle: Lifecycle): LifecyclesById =
      lifecyclesById.updated(
        lifecycle.id,
        lifecyclesById.getOrElse(lifecycle.id, noLifecycles) + lifecycle)

    val calculationStateWithSimpleLifecyclesAddedIn =
      calculationStateAfterAnnulments.flatMap(
        lifecyclesById =>
          CalculationState(
            defunctLifecycles = Set.empty,
            newLifecycles = simpleLifecyclesForNewAndModifiedEvents.toSet,
            lifecyclesById =
              (lifecyclesById /: simpleLifecyclesForNewAndModifiedEvents)(
                addLifecycle)
        ))

    val CalculationState(finalDefunctLifecyles,
                         finalNewLifecycles,
                         finalLifecyclesById) =
      calculationStateWithSimpleLifecyclesAddedIn.fuseLifecycles()

    // NOTE: is it really valid to use *'lifecycleById'* with 'finalDefunctLifecycles'? Yes, because any lifecycle *not* in 'lifecycleById'
    // either makes it all the way through in the above code to 'finalLifecyclesById', or is made defunct itself and thrown away by the
    // balancing done when flat-mapping a 'CalculationState'.
    val itemStateUpdatesFromDefunctLifecycles
      : Set[(ItemStateUpdateKey, ItemStateUpdate)] =
      finalDefunctLifecyles.flatMap(_.itemStateUpdates(lifecyclesById))

    val itemStateUpdatesFromNewOrModifiedLifecycles
      : Set[(ItemStateUpdateKey, ItemStateUpdate)] =
      finalNewLifecycles.flatMap(_.itemStateUpdates(finalLifecyclesById))

    val itemStateUpdateKeysThatNeedToBeRevoked: Set[ItemStateUpdateKey] =
      (itemStateUpdatesFromDefunctLifecycles -- itemStateUpdatesFromNewOrModifiedLifecycles)
        .map(_._1)

    val newOrModifiedItemStateUpdates
      : Map[ItemStateUpdateKey, ItemStateUpdate] =
      (itemStateUpdatesFromNewOrModifiedLifecycles -- itemStateUpdatesFromDefunctLifecycles).toMap

    val allEventIdsBookedIn: Set[EventId] = events map (_._1) toSet

    def eventFootprintFrom(event: Event): EventFootprint = event match {
      case Change(when, patches) =>
        EventFootprint(
          when = when,
          itemIds = patches
            .flatMap(patch =>
              patch.targetItemSpecification.id +: patch.argumentItemSpecifications
                .map(_.id))
            .toSet)
      case Measurement(when, patches) =>
        EventFootprint(
          when = when,
          itemIds = patches
            .flatMap(patch =>
              patch.targetItemSpecification.id +: patch.argumentItemSpecifications
                .map(_.id))
            .toSet)
      case Annihilation(when, uniqueItemSpecification) =>
        EventFootprint(when = Finite(when),
                       itemIds = Set(uniqueItemSpecification.id))
    }

    val lifecycleFootprintPerEventWithoutEventIdsForThisRevisionBooking: Map[
      EventId,
      AllEventsImplementation.EventFootprint] = lifecycleFootprintPerEvent filter {
      case (eventId, _) => allEventIdsBookedIn.contains(eventId)
    }

    val finalLifecycleFootprintPerEvent =
      (lifecycleFootprintPerEventWithoutEventIdsForThisRevisionBooking /: newAndModifiedEvents) {
        case (lifecycleFootprintPerEvent, (eventId, event)) =>
          lifecycleFootprintPerEvent + (eventId -> eventFootprintFrom(event))
      }

    ItemStateUpdatesDelta(
      allEvents =
        new AllEventsImplementation(nextRevision = 1 + this.nextRevision,
                                    lifecycleFootprintPerEvent =
                                      finalLifecycleFootprintPerEvent,
                                    lifecyclesById = finalLifecyclesById),
      itemStateUpdateKeysThatNeedToBeRevoked =
        itemStateUpdateKeysThatNeedToBeRevoked,
      newOrModifiedItemStateUpdates = newOrModifiedItemStateUpdates
    )
  }

  override def retainUpTo(when: Unbounded[Instant]): AllEvents = {
    val cutoff = UpperBoundOfTimeslice(when)

    val timespanUpToAndIncludingTheCutoff = (Split.alignedWith(
      LowerBoundOfTimeslice(NegativeInfinity())): Split[ItemStateUpdateTime]) -> (Split
      .upperBoundOf(cutoff): Split[ItemStateUpdateTime])

    new AllEventsImplementation(
      lifecycleFootprintPerEvent = lifecycleFootprintPerEvent.filter {
        case (_, EventFootprint(whenEventTakesPlace, _)) =>
          Ordering[ItemStateUpdateTime]
            .lteq(UpperBoundOfTimeslice(whenEventTakesPlace), cutoff)
      },
      lifecyclesById = lifecyclesById.mapValues { lifecycles =>
        val (retainedUnchangedLifecycles, retainedTrimmedLifecycles) =
          lifecycles
            .filterIncludes(timespanUpToAndIncludingTheCutoff)
            .partition(lifecycle =>
              Ordering[ItemStateUpdateTime].lteq(lifecycle.endTime, cutoff))

        (noLifecycles /: (retainedUnchangedLifecycles ++ retainedTrimmedLifecycles
          .flatMap(_.retainUpTo(when))))(_ + _)
      }
    )
  }

  private def buildSimpleLifecyclesFrom(
      events: Map[EventId, Event]): Iterable[Lifecycle] = {
    events.zipWithIndex.flatMap {
      case ((eventId, event), eventOrderingTiebreakerIndex) =>
        event match {
          case Change(when, patches) =>
            patches.zipWithIndex.flatMap {
              case (patch, intraEventIndex) =>
                val eventOrderingKey =
                  (when, nextRevision, eventOrderingTiebreakerIndex)
                val itemStateUpdateKey =
                  ItemStateUpdateKey(eventOrderingKey = eventOrderingKey,
                                     intraEventIndex = intraEventIndex)
                Lifecycle.fromChange(
                  eventId = eventId,
                  itemStateUpdateKey,
                  patch = patch) +: patch.argumentItemSpecifications.map(
                  uniqueItemSpecification =>
                    Lifecycle.fromArgumentTypeReference(
                      eventId = eventId,
                      itemStateUpdateKey = itemStateUpdateKey,
                      uniqueItemSpecification = uniqueItemSpecification))
            }
          case Measurement(when, patches) =>
            patches.zipWithIndex.flatMap {
              case (patch, intraEventIndex) =>
                val eventOrderingKey =
                  (when, nextRevision, eventOrderingTiebreakerIndex)
                val itemStateUpdateKey =
                  ItemStateUpdateKey(eventOrderingKey = eventOrderingKey,
                                     intraEventIndex = intraEventIndex)
                Lifecycle.fromMeasurement(
                  eventId = eventId,
                  itemStateUpdateKey,
                  patch = patch) +: patch.argumentItemSpecifications.map(
                  uniqueItemSpecification =>
                    Lifecycle.fromArgumentTypeReference(
                      eventId = eventId,
                      itemStateUpdateKey = itemStateUpdateKey,
                      uniqueItemSpecification = uniqueItemSpecification))
            }
          case annihilation @ Annihilation(when, _) =>
            val eventOrderingKey =
              (Finite(when), nextRevision, eventOrderingTiebreakerIndex)
            Seq(
              Lifecycle.fromAnnihilation(
                eventId = eventId,
                itemStateUpdateKey = ItemStateUpdateKey(eventOrderingKey =
                                                          eventOrderingKey,
                                                        intraEventIndex = 0),
                uniqueItemSpecification = annihilation.uniqueItemSpecification
              ))
        }
    }
  }
}
