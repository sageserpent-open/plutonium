package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{
  Finite,
  NegativeInfinity,
  PositiveInfinity,
  Unbounded
}
import com.sageserpent.plutonium.AllEvents.ItemStateUpdatesDelta
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
import scala.collection.immutable.Map
import scala.reflect.runtime.universe.TypeTag

object AllEventsImplementation {
  // TODO - can we get rid of this? As long as the support for a closed-open interval exists, maybe we don't need an explicit end time?
  val sentinelForEndTimeOfLifecycleWithoutAnnihilation = UpperBoundOfTimeslice(
    PositiveInfinity())

  // TODO: I'm getting the impression that 'RangedSeq' works with closed-open intervals.
  // If so, we should probably cutover to using a 'SplitLevel[ItemStateUpdateTime]' so that
  // the open end can be modelled cleanly.
  type LifecycleEndPoints = (ItemStateUpdateTime, ItemStateUpdateTime)

  object Lifecycle {
    def fromChange(eventId: EventId,
                   itemStateUpdateKey: ItemStateUpdateKey,
                   patch: AbstractPatch): Lifecycle = ???

    def fromMeasurement(eventId: EventId,
                        itemStateUpdateKey: ItemStateUpdateKey,
                        patch: AbstractPatch): Lifecycle =
      ???

    def fromAnnihilation(eventId: EventId,
                         itemStateUpdateKey: ItemStateUpdateKey,
                         annihilation: Annihilation): Lifecycle = ???

    def fromArgumentTypeReference(
        eventId: EventId,
        itemStateUpdateKey: ItemStateUpdateKey,
        uniqueItemSpecification: UniqueItemSpecification): Lifecycle = ???
  }

  trait Lifecycle {
    val startTime: ItemStateUpdateTime

    val endTime: ItemStateUpdateTime

    require(Ordering[ItemStateUpdateTime].lt(startTime, endTime))

    val endPoints: LifecycleEndPoints = startTime -> endTime

    def overlapsWith(another: Lifecycle): Boolean =
      Ordering[ItemStateUpdateTime]
        .lteq(this.startTime, another.endTime) && Ordering[ItemStateUpdateTime]
        .lteq(another.startTime, this.endTime)

    val uniqueItemSpecification: UniqueItemSpecification

    def id = uniqueItemSpecification.id

    val lowerBoundTypeTag = uniqueItemSpecification.typeTag

    val upperBoundTypeTag: TypeTag[_]

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
    def retainUpTo(when: Unbounded[Instant]): Option[Lifecycle]

    def isRelevantTo(eventId: EventId): Boolean

    def annul(eventId: EventId): Option[Lifecycle]

    // The lower type bounds are compatible and there is overlap.
    def isFusibleWith(another: Lifecycle): Boolean =
      // NOTE: there is no check on upper types. Think of multiple inheritance
      // of interfaces in Java and trait mixins in Scala; you'll see why.
      this.lowerTypeIsConsistentWith(another) && this.overlapsWith(another)

    def fuseWith(another: Lifecycle): Lifecycle

    // NOTE: these will have best patches applied along with type adjustments, including on the arguments. That's why
    // 'lifecyclesById' is provided - although the any reference to the same unique item as that references by the receiver
    // lifecycle will ignore the argument and use the receiver lifecycle. The item state update key of the update is used
    // to select the correct lifecycle to resolve an argument type, if there is more than one lifecycle for that unique item.
    def itemStateUpdates(lifecyclesById: LifecyclesById)
      : Set[(ItemStateUpdateKey, ItemStateUpdate)]

    def uniqueItemSpecificationToTypeTagMap
      : collection.Map[UniqueItemSpecification, TypeTag[_]]
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
            .fold(Iterator.empty: Iterator[Lifecycle])(
              lifecycle =>
                // NASTY HACK - the 'RangedSeq' API has a strange way of dealing with intervals - have to work around it here...
                lifecycle.filterOverlaps(candidateForFusion) ++ lifecycle
                  .filterIncludes(
                    Split.alignedWith(candidateForFusion.startTime) -> Split
                      .alignedWith(candidateForFusion.startTime))
                  ++ lifecycle
                    .filterIncludes(
                      Split.alignedWith(candidateForFusion.endTime) -> Split
                        .alignedWith(candidateForFusion.endTime))
                  filterNot (_ == candidateForFusion))

          val conflictingLifecycles = overlappingLifecycles.filter(
            _.typeBoundsAreInconsistentWith(candidateForFusion))

          if (conflictingLifecycles.nonEmpty) {
            throw new RuntimeException(
              s"There is at least one item of id: '${itemId}' that would be inconsistent with type '${candidateForFusion.lowerBoundTypeTag.tpe}', these have types: '${conflictingLifecycles map (_.lowerBoundTypeTag.tpe)}'.")
          }

          val fusibleLifecycles =
            overlappingLifecycles.filter(_.isFusibleWith(candidateForFusion))

          fusibleLifecycles.size match {
            case 0 =>
              this.fuseLifecycles(
                priorityQueueOfLifecyclesConsideredForFusionOrAddition.drop(1))
            case 1 =>
              val matchForFusion = fusibleLifecycles.next()
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
                annihilation = annihilation))
        }
    }
  }
}
