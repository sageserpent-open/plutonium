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
  IndivisibleChange,
  IndivisibleEvent,
  IndivisibleMeasurement,
  refineTypeFor
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

  implicit def closedOpenEndPoints(lifecycle: Lifecycle)
    : (Split[ItemStateUpdateTime], Split[ItemStateUpdateTime]) =
    Split.alignedWith(lifecycle.startTime) -> Split.upperBoundOf(
      lifecycle.endTime)

  object Lifecycle {
    implicit val bagConfiguration = HashedBagConfiguration.compact[TypeTag[_]]

    def apply(eventId: EventId,
              itemStateUpdateKey: ItemStateUpdateKey,
              indivisibleEvent: IndivisibleEvent): Lifecycle =
      Lifecycle(
        typeTags = Bag(indivisibleEvent.uniqueItemSpecification.typeTag),
        eventsArrangedInTimeOrder =
          SortedMap(itemStateUpdateKey              -> indivisibleEvent),
        itemStateUpdateTimesByEventId = Map(eventId -> Set(itemStateUpdateKey))
      )

    def fromChange(eventId: EventId,
                   itemStateUpdateKey: ItemStateUpdateKey,
                   patch: AbstractPatch): Lifecycle =
      Lifecycle(eventId = eventId,
                itemStateUpdateKey = itemStateUpdateKey,
                indivisibleEvent = IndivisibleChange(patch))

    def fromMeasurement(eventId: EventId,
                        itemStateUpdateKey: ItemStateUpdateKey,
                        patch: AbstractPatch): Lifecycle =
      Lifecycle(eventId = eventId,
                itemStateUpdateKey = itemStateUpdateKey,
                indivisibleEvent = IndivisibleMeasurement(patch))

    def fromAnnihilation(eventId: EventId,
                         itemStateUpdateKey: ItemStateUpdateKey,
                         annihilation: Annihilation): Lifecycle =
      Lifecycle(eventId = eventId,
                itemStateUpdateKey = itemStateUpdateKey,
                indivisibleEvent = EndOfLifecycle(annihilation))

    def fromArgumentTypeReference(
        eventId: EventId,
        itemStateUpdateKey: ItemStateUpdateKey,
        uniqueItemSpecification: UniqueItemSpecification): Lifecycle = {
      Lifecycle(eventId = eventId,
                itemStateUpdateKey = itemStateUpdateKey,
                indivisibleEvent = ArgumentReference(uniqueItemSpecification))
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

    case class EndOfLifecycle(annihilation: Annihilation)
        extends IndivisibleEvent {
      override def uniqueItemSpecification: UniqueItemSpecification =
        annihilation.uniqueItemSpecification
    }

    def refineTypeFor(itemStateUpdateTime: ItemStateUpdateTime,
                      lifecyclesById: LifecyclesById)(
        uniqueItemSpecification: UniqueItemSpecification): TypeTag[_] = {
      val Seq(relevantLifecycle: Lifecycle) =
        lifecyclesById(uniqueItemSpecification.id)
          .filterIncludes(
            Split.alignedWith(itemStateUpdateTime) -> Split.alignedWith(
              itemStateUpdateTime))
          .filter(lifecycle =>
            lifecycle.lowerBoundTypeTag.tpe <:< uniqueItemSpecification.typeTag.tpe)
          .toList
      relevantLifecycle.lowerBoundTypeTag
    }
  }

  case class Lifecycle(
      typeTags: Bag[TypeTag[_]],
      eventsArrangedInTimeOrder: SortedMap[ItemStateUpdateKey,
                                           IndivisibleEvent],
      itemStateUpdateTimesByEventId: Map[EventId, Set[ItemStateUpdateKey]]) {
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

    val startTime: ItemStateUpdateTime = eventsArrangedInTimeOrder.firstKey

    // TODO: the way annihilations are just mixed in with all the other events in 'eventsArrangedInTimeOrder' feels hokey:
    // it necessitates a pesky invariant check, along with the special case logic below. Sort this out!
    val endTime: ItemStateUpdateTime = eventsArrangedInTimeOrder.last match {
      case (itemStateUpdateTime, _: EndOfLifecycle) => itemStateUpdateTime
      case _                                        => sentinelForEndTimeOfLifecycleWithoutAnnihilation
    }

    require(Ordering[ItemStateUpdateTime].lteq(startTime, endTime))

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
      this.upperTypeIsConsistentWith(another) ^ this
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
          Lifecycle(typeTags = retainedTypeTags,
                    eventsArrangedInTimeOrder = retainedEvents,
                    itemStateUpdateTimesByEventId =
                      retainedItemStateUpdateTimesByEventId))
      } else None
    }

    def isRelevantTo(eventId: EventId): Boolean =
      itemStateUpdateTimesByEventId.contains(eventId)

    def annul(eventId: EventId): Option[Lifecycle] = {
      val itemStateUpdateTimes = itemStateUpdateTimesByEventId(eventId)
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
          Lifecycle(typeTags = preservedTypeTags,
                    eventsArrangedInTimeOrder = preservedEvents,
                    itemStateUpdateTimesByEventId =
                      preservedItemStateUpdateTimesByEventId))
      } else None
    }

    // The lower type bounds are compatible and there is overlap.
    def isFusibleWith(another: Lifecycle): Boolean =
      this.lowerTypeIsConsistentWith(another) && this.upperTypeIsConsistentWith(
        another) && this.overlapsWith(another)

    def fuseWith(another: Lifecycle): Lifecycle = {
      val fusedTypeTags = typeTags ++ another.typeTags

      val fusedEventsArrangedInTimeOrder: SortedMap[
        ItemStateUpdateKey,
        IndivisibleEvent] = eventsArrangedInTimeOrder ++ another.eventsArrangedInTimeOrder

      val fusedItemStateUpdateTimesByEventId
        : Map[EventId, Set[ItemStateUpdateKey]] =
        (itemStateUpdateTimesByEventId.keys ++ another.itemStateUpdateTimesByEventId.keys) map (
            eventId =>
              eventId ->
                itemStateUpdateTimesByEventId.getOrElse(
                  eventId,
                  Set.empty ++ another.itemStateUpdateTimesByEventId
                    .getOrElse(eventId, Set.empty))) toMap

      Lifecycle(typeTags = fusedTypeTags,
                eventsArrangedInTimeOrder = fusedEventsArrangedInTimeOrder,
                itemStateUpdateTimesByEventId =
                  fusedItemStateUpdateTimesByEventId)
    }

    // NOTE: these will have best patches applied along with type adjustments, including on the arguments. That's why
    // 'lifecyclesById' is provided - although any reference to the same unique item as that referenced by the receiver
    // lifecycle will ignore the argument and use the receiver lifecycle. The item state update key of the update is used
    // to select the correct lifecycle to resolve an argument type, if there is more than one lifecycle for that unique item.
    def itemStateUpdates(lifecyclesById: LifecyclesById)
      : Set[(ItemStateUpdateKey, ItemStateUpdate)] = {

      //  TODO: this is a deliberately hokey implementation just put in to do some obvious smoke testing of the implementation as a whole - needs fixing!

      // TODO: the subclasses of 'IndivisibleEvent' look rather like a more refined form of the subclasses of 'ItemStateUpdate'. Hmmm....
      eventsArrangedInTimeOrder.collect {
        case (itemStateUpdateTime, IndivisibleChange(patch)) =>
          itemStateUpdateTime -> ItemStatePatch(
            patch.rewriteItemTypeTags(
              refineTypeFor(itemStateUpdateTime, lifecyclesById)))
        case (itemStateUpdateTime, IndivisibleMeasurement(patch)) => ???
        case (itemStateUpdateTime, EndOfLifecycle(annihilation)) =>
          itemStateUpdateTime -> ItemStateAnnihilation(
            annihilation.rewriteItemTypeTag(lowerBoundTypeTag))
      }.toSet
    }
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

    abstract override def itemStateUpdates(lifecyclesById: LifecyclesById) = {
      val id = uniqueItemSpecification.id
      require(lifecyclesById.contains(id))
      require(
        lifecyclesById(id)
          .filterOverlaps(this)
          .contains(this))
      super.itemStateUpdates(lifecyclesById)
    }
  }

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

        val combinedDefunctLifecycles
          : Set[Lifecycle] = defunctLifecycles ++ defunctLifecyclesFromStep

        val combinedNewLifecycles
          : Set[Lifecycle] = newLifecycles ++ newLifecyclesFromStep

        CalculationState(
          defunctLifecycles = combinedDefunctLifecycles -- combinedNewLifecycles,
          newLifecycles = combinedNewLifecycles -- combinedDefunctLifecycles,
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
          val (candidateForFusion, _) =
            priorityQueueOfLifecyclesConsideredForFusionOrAddition.head

          val itemId = candidateForFusion.id

          val overlappingLifecycles = lifecyclesById
            .get(itemId)
            .fold(Seq.empty[Lifecycle]) { lifecycle => // NASTY HACK - the 'RangedSeq' API has a strange way of dealing with intervals - have to work around it here...
              val startTime = Split.alignedWith(candidateForFusion.startTime)
              val endTime   = Split.alignedWith(candidateForFusion.endTime)
              (lifecycle.filterOverlaps(candidateForFusion) ++ lifecycle
                .filterIncludes(startTime -> startTime) ++ lifecycle
                .filterIncludes(endTime   -> endTime)).toSeq.distinct
                .filterNot(
                  priorityQueueOfLifecyclesConsideredForFusionOrAddition.contains)
            }
            .toList

          val conflictingLifecycles = overlappingLifecycles.filter(
            _.typeBoundsAreInconsistentWith(candidateForFusion))

          if (conflictingLifecycles.nonEmpty) {
            throw new RuntimeException(
              s"There is at least one item of id: '${itemId}' that would be inconsistent with type '${candidateForFusion.lowerBoundTypeTag.tpe}', these have types: '${conflictingLifecycles map (_.lowerBoundTypeTag.tpe)}'.")
          }

          val fusibleLifecycles =
            overlappingLifecycles
              .filter(_.isFusibleWith(candidateForFusion))

          fusibleLifecycles match {
            case Nil =>
              this.fuseLifecycles(
                priorityQueueOfLifecyclesConsideredForFusionOrAddition.drop(1))
            case matchForFusion :: Nil =>
              val fusedLifecycle = candidateForFusion.fuseWith(matchForFusion)
              val nextState = this.flatMap(
                lifecyclesById =>
                  CalculationState(
                    defunctLifecycles = Set(candidateForFusion, matchForFusion),
                    newLifecycles = Set(fusedLifecycle),
                    lifecyclesById = lifecyclesById.updated(
                      itemId,
                      lifecyclesById(itemId) - matchForFusion - candidateForFusion + fusedLifecycle)
                ))

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
      lifecycleFootprintPerEvent.get(eventId) match {
        case Some(EventFootprint(when, itemIds)) =>
          val (lifecyclesWithRelevantIds: LifecyclesById,
               lifecyclesWithIrrelevantIds: LifecyclesById) =
            lifecyclesById.partition {
              case (lifecycleId, lifecycles) => itemIds.contains(lifecycleId)
            }

          val (lifecyclesByIdWithAnnulments,
               changedLifecycles,
               defunctLifecycles) =
            (lifecyclesWithRelevantIds map {
              case (itemId, lifecycles) =>
                val lowerBound = Split.alignedWith(
                  LowerBoundOfTimeslice(when): ItemStateUpdateTime)
                val upperBound = Split.alignedWith(
                  UpperBoundOfTimeslice(when): ItemStateUpdateTime)

                val lifecyclesIncludingEventTime =
                  // NASTY HACK - the 'RangedSeq' API has a strange way of dealing with intervals - have to work around it here...
                  (lifecycles
                    .filterOverlaps(lowerBound           -> upperBound) ++
                    lifecycles.filterIncludes(lowerBound -> lowerBound) ++
                    lifecycles.filterIncludes(upperBound -> upperBound)).toSeq.distinct

                val lifecyclesIncludingEventId = lifecyclesIncludingEventTime filter (_.isRelevantTo(
                  eventId))

                val lifecyclesWithAnnulments =
                  lifecyclesIncludingEventId.flatMap(_.annul(eventId))

                val otherLifecycles =
                  (lifecycles /: lifecyclesIncludingEventId)(_ - _)

                (itemId -> (otherLifecycles /: lifecyclesWithAnnulments)(_ + _),
                 lifecyclesWithAnnulments,
                 lifecyclesIncludingEventId)
            }).unzip3

          CalculationState(
            defunctLifecycles = defunctLifecycles.flatten.toSet,
            newLifecycles = (Set.empty[Lifecycle] /: changedLifecycles)(_ ++ _),
            lifecyclesById = lifecyclesByIdWithAnnulments.toMap
          )

        case None =>
          CalculationState(defunctLifecycles = Set.empty,
                           newLifecycles = Set.empty,
                           lifecyclesById = lifecyclesById)
      }
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

    val newAndModifiedEvents: Seq[(EventId, Event)] = events.toSeq.collect {
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
      calculationStateWithSimpleLifecyclesAddedIn.flatMap(_ =>
        calculationStateWithSimpleLifecyclesAddedIn.fuseLifecycles())

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
      AllEventsImplementation.EventFootprint] = lifecycleFootprintPerEvent filterNot {
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
      .alignedWith(cutoff): Split[ItemStateUpdateTime])

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
      events: Seq[(EventId, Event)]): Iterable[Lifecycle] = {
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
                annihilation = annihilation
              ))
        }
    }
  }
}
