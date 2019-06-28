package com.sageserpent.plutonium

import java.lang.reflect.Method
import java.time.Instant

import alleycats.std.iterable._
import cats.Foldable
import cats.data.Writer
import cats.implicits._
import com.sageserpent.americium.{
  Finite,
  NegativeInfinity,
  PositiveInfinity,
  Unbounded
}
import com.sageserpent.plutonium.AllEvents.ItemStateUpdatesDelta
import com.sageserpent.plutonium.AllEventsImplementation.Lifecycle._
import com.sageserpent.plutonium.AllEventsImplementation._
import com.sageserpent.plutonium.World.{Revision, initialRevision}
import de.sciss.fingertree.RangedSeq
import de.ummels.prioritymap.PriorityMap

import scala.annotation.tailrec
import scala.collection.IterableView
import scala.collection.immutable.{
  Bag,
  HashedBagConfiguration,
  Map,
  Set,
  SortedMap
}

object AllEventsImplementation {
  val maxNumberOfIdsToSample = 100

  // TODO - can we get rid of this? As long as the support for a closed-open interval exists, maybe we don't need an explicit end time?
  val sentinelForEndTimeOfLifecycleWithoutAnnihilation = UpperBoundOfTimeslice(
    PositiveInfinity())

  type LifecycleEndPoints = (ItemStateUpdateTime, ItemStateUpdateTime)

  implicit def closedOpenEndPoints(lifecycle: Lifecycle)
    : (Split[ItemStateUpdateTime], Split[ItemStateUpdateTime]) =
    Split.alignedWith(lifecycle.startTime: ItemStateUpdateTime) -> Split
      .upperBoundOf(lifecycle.endTime)

  object Lifecycle {
    implicit val bagConfiguration = HashedBagConfiguration.compact[Class[_]]

    def apply(eventId: EventId,
              itemStateUpdateKey: ItemStateUpdateKey,
              indivisibleEvent: IndivisibleEvent): Lifecycle =
      new LifecycleImplementation(
        clazzes = Bag(indivisibleEvent.uniqueItemSpecification.clazz),
        eventsArrangedInReverseTimeOrder =
          SortedMap(itemStateUpdateKey -> indivisibleEvent)(
            Ordering[ItemStateUpdateKey].reverse),
        itemStateUpdateTimesByEventId = Map(eventId -> Set(itemStateUpdateKey))
      ) with LifecycleContracts

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
        uniqueItemSpecification: UniqueItemSpecification,
        targetUniqueItemSpecification: UniqueItemSpecification): Lifecycle = {
      Lifecycle(eventId = eventId,
                itemStateUpdateKey = itemStateUpdateKey,
                indivisibleEvent =
                  ArgumentReference(uniqueItemSpecification,
                                    targetUniqueItemSpecification))
    }

    sealed trait FusionResult

    case class LifecycleMerge(mergedLifecycle: Lifecycle) extends FusionResult

    case class LifecycleSplit(endsInAnnihilation: Lifecycle,
                              phoenixLifecycle: Lifecycle)
        extends FusionResult {
      require(
        Ordering[ItemStateUpdateTime].lt(endsInAnnihilation.endTime,
                                         phoenixLifecycle.startTime))
    }

    // TODO: the subclasses of 'IndivisibleEvent' look rather like a more refined form of the subclasses of 'ItemStateUpdate'. Hmmm....
    sealed trait IndivisibleEvent {
      def uniqueItemSpecification: UniqueItemSpecification
    }

    case class ArgumentReference(
        override val uniqueItemSpecification: UniqueItemSpecification,
        targetUniqueItemSpecification: UniqueItemSpecification)
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

    def refineClazzFor(itemStateUpdateTime: ItemStateUpdateTime,
                       uniqueItemSpecification: UniqueItemSpecification,
                       lifecyclesById: LifecyclesById): Class[_] = {
      lifecycleFor(itemStateUpdateTime, uniqueItemSpecification, lifecyclesById).lowerBoundClazz
    }

    def lifecycleFor(itemStateUpdateTime: ItemStateUpdateTime,
                     uniqueItemSpecification: UniqueItemSpecification,
                     lifecyclesById: LifecyclesById): Lifecycle = {
      val Seq(relevantLifecycle: Lifecycle) =
        lifecyclesById(uniqueItemSpecification.id)
          .filterIncludes(
            Split.alignedWith(itemStateUpdateTime) -> Split.alignedWith(
              itemStateUpdateTime))
          .filter(lifecycle =>
            uniqueItemSpecification.clazz.isAssignableFrom(
              lifecycle.lowerBoundClazz))
          .toList
      relevantLifecycle
    }

    def fuse(firstLifecycle: Lifecycle, secondLifecycle: Lifecycle) = {
      val fusedClazzes = firstLifecycle.clazzes ++ secondLifecycle.clazzes

      val fusedEventsArrangedInReverseTimeOrder: SortedMap[
        ItemStateUpdateKey,
        IndivisibleEvent] = firstLifecycle.eventsArrangedInReverseTimeOrder ++ secondLifecycle.eventsArrangedInReverseTimeOrder

      val fusedItemStateUpdateTimesByEventId
        : Map[EventId, Set[ItemStateUpdateKey]] =
        (firstLifecycle.itemStateUpdateTimesByEventId.keys ++ secondLifecycle.itemStateUpdateTimesByEventId.keys) map (
            eventId =>
              eventId ->
                firstLifecycle.itemStateUpdateTimesByEventId
                  .getOrElse(eventId, Set.empty)
                  .union(secondLifecycle.itemStateUpdateTimesByEventId
                    .getOrElse(eventId, Set.empty))) toMap

      new LifecycleImplementation(
        clazzes = fusedClazzes,
        eventsArrangedInReverseTimeOrder = fusedEventsArrangedInReverseTimeOrder,
        itemStateUpdateTimesByEventId = fusedItemStateUpdateTimesByEventId
      ) with LifecycleContracts
    }

    def apply(
        retainedEvents: SortedMap[ItemStateUpdateKey, IndivisibleEvent],
        trimmedEvents: SortedMap[ItemStateUpdateKey, IndivisibleEvent],
        clazzes: Bag[Class[_]],
        itemStateUpdateTimesByEventId: Map[EventId, Set[ItemStateUpdateKey]])
      : Lifecycle = {
      val retainedClazzes = (clazzes /: trimmedEvents) {
        case (clazzes, (_, trimmedEvent)) =>
          clazzes - trimmedEvent.uniqueItemSpecification.clazz
      }

      new LifecycleImplementation(
        clazzes = retainedClazzes,
        eventsArrangedInReverseTimeOrder = retainedEvents,
        itemStateUpdateTimesByEventId = itemStateUpdateTimesByEventId)
      with LifecycleContracts
    }
  }

  trait Lifecycle {
    val startTime: ItemStateUpdateKey

    val endTime: ItemStateUpdateTime

    val uniqueItemSpecification: UniqueItemSpecification

    def id: Any

    def lowerBoundClazz: Class[_]

    def upperBoundClazz: Class[_]

    def annul(eventId: EventId): Option[Lifecycle]

    def fuseWith(another: Lifecycle): FusionResult

    // NOTE: this is quite defensive, we can answer with 'None' if 'when' is not greater than the start time.
    def retainUpTo(when: Unbounded[Instant]): Option[Lifecycle]

    // The lower type bounds are compatible and there is overlap.
    def isFusibleWith(another: Lifecycle): Boolean

    def isIsolatedAnnihilation: Boolean

    def isRelevantTo(eventId: EventId): Boolean

    def typeBoundsAreInconsistentWith(another: Lifecycle): Boolean

    def itemStateUpdates(lifecyclesById: LifecyclesById,
                         bestPatchSelection: BestPatchSelection)
      : Set[(ItemStateUpdateKey, ItemStateUpdate)]

    def referencingLifecycles(lifecyclesById: LifecyclesById): Set[Lifecycle]

    val clazzes: Bag[Class[_]]

    val eventsArrangedInReverseTimeOrder: SortedMap[ItemStateUpdateKey,
                                                    IndivisibleEvent]

    val itemStateUpdateTimesByEventId: Map[EventId, Set[ItemStateUpdateKey]]

  }

  case class LifecycleImplementation(
      clazzes: Bag[Class[_]],
      eventsArrangedInReverseTimeOrder: SortedMap[ItemStateUpdateKey,
                                                  IndivisibleEvent],
      itemStateUpdateTimesByEventId: Map[EventId, Set[ItemStateUpdateKey]])
      extends Lifecycle {
    val startTime: ItemStateUpdateKey =
      eventsArrangedInReverseTimeOrder.lastKey

    // TODO: the way annihilations are just mixed in with all the other events in 'eventsArrangedInTimeOrder' feels hokey:
    // it necessitates a pesky invariant check, along with the special case logic below. Sort this out!
    val endTime: ItemStateUpdateTime =
      eventsArrangedInReverseTimeOrder.head match {
        case (itemStateUpdateTime, _: EndOfLifecycle) => itemStateUpdateTime
        case _                                        => sentinelForEndTimeOfLifecycleWithoutAnnihilation
      }

    val uniqueItemSpecification: UniqueItemSpecification =
      UniqueItemSpecification(id, lowerBoundClazz)

    def id: Any =
      eventsArrangedInReverseTimeOrder.head._2.uniqueItemSpecification.id

    def lowerBoundClazz: Class[_] = clazzes.reduce[Class[_]] {
      case (first, second) =>
        if (second.isAssignableFrom(first)) first else second
    }

    def upperBoundClazz: Class[_] = clazzes.reduce[Class[_]] {
      case (first, second) =>
        if (second.isAssignableFrom(first)) second else first
    }

    def annul(eventId: EventId): Option[Lifecycle] = {
      val itemStateUpdateTimes = itemStateUpdateTimesByEventId(eventId).filter(
        eventsArrangedInReverseTimeOrder.contains)
      val preservedEvents =
        (eventsArrangedInReverseTimeOrder /: itemStateUpdateTimes)(_ - _)
      if (preservedEvents.nonEmpty) {
        val annulledEvents = itemStateUpdateTimes map eventsArrangedInReverseTimeOrder.apply
        val preservedClazzes = (clazzes /: annulledEvents) {
          case (clazzes, annulledEvent) =>
            clazzes - annulledEvent.uniqueItemSpecification.clazz
        }
        val preservedItemStateUpdateTimesByEventId = itemStateUpdateTimesByEventId - eventId
        Some(
          new LifecycleImplementation(
            clazzes = preservedClazzes,
            eventsArrangedInReverseTimeOrder = preservedEvents,
            itemStateUpdateTimesByEventId =
              preservedItemStateUpdateTimesByEventId) with LifecycleContracts)
      } else None
    }

    def fuseWith(another: Lifecycle): FusionResult =
      this.eventsArrangedInReverseTimeOrder.head -> another.eventsArrangedInReverseTimeOrder.head match {
        case ((whenThisLifecycleEnds, _: EndOfLifecycle),
              (whenTheLastEventInTheOtherLifecycleTakesPlace, _))
            if Ordering[ItemStateUpdateKey].lt(
              whenThisLifecycleEnds,
              whenTheLastEventInTheOtherLifecycleTakesPlace) =>
          val eventsFromTheOtherForEarlierLifecycle =
            another.eventsArrangedInReverseTimeOrder.from(whenThisLifecycleEnds)
          val eventsFromTheOtherForLaterLifecycle =
            another.eventsArrangedInReverseTimeOrder.until(
              whenThisLifecycleEnds)

          LifecycleSplit(
            fuse(
              this,
              Lifecycle(eventsFromTheOtherForEarlierLifecycle,
                        eventsFromTheOtherForLaterLifecycle,
                        another.clazzes,
                        another.itemStateUpdateTimesByEventId)
            ),
            Lifecycle(
              eventsFromTheOtherForLaterLifecycle,
              eventsFromTheOtherForEarlierLifecycle,
              another.clazzes,
              another.itemStateUpdateTimesByEventId
            )
          )
        case ((whenTheLastEventInThisLifecycleTakesPlace, _),
              (whenTheOtherLifecycleEnds, _: EndOfLifecycle))
            if Ordering[ItemStateUpdateKey].lt(
              whenTheOtherLifecycleEnds,
              whenTheLastEventInThisLifecycleTakesPlace) =>
          val eventsFromThisForEarlierLifecycle =
            this.eventsArrangedInReverseTimeOrder
              .from(whenTheOtherLifecycleEnds)
          val eventsFromThisForLaterLifecycle =
            this.eventsArrangedInReverseTimeOrder
              .until(whenTheOtherLifecycleEnds)

          LifecycleSplit(
            fuse(
              another,
              Lifecycle(eventsFromThisForEarlierLifecycle,
                        eventsFromThisForLaterLifecycle,
                        another.clazzes,
                        another.itemStateUpdateTimesByEventId)
            ),
            Lifecycle(
              eventsFromThisForLaterLifecycle,
              eventsFromThisForEarlierLifecycle,
              another.clazzes,
              another.itemStateUpdateTimesByEventId
            )
          )
        case _ =>
          LifecycleMerge(fuse(this, another))
      }

    def retainUpTo(when: Unbounded[Instant]): Option[Lifecycle] = {
      val inclusionPredicate =
        Ordering[ItemStateUpdateTime]
          .lteq(_: ItemStateUpdateTime, UpperBoundOfTimeslice(when))

      val (retainedEvents, trimmedEvents) =
        // TODO: make use of the intrinsic ordering to do the partition in logarithmic time.
        eventsArrangedInReverseTimeOrder.partition {
          case (itemStateUpdateTime, _) =>
            inclusionPredicate(itemStateUpdateTime)
        }

      if (retainedEvents.nonEmpty) {
        Some(
          Lifecycle(retainedEvents,
                    trimmedEvents,
                    clazzes,
                    itemStateUpdateTimesByEventId))
      } else None
    }

    def isFusibleWith(another: Lifecycle): Boolean =
      this.lowerClazzIsConsistentWith(another) && this
        .upperClazzIsConsistentWith(another) && this.overlapsWith(another)

    def isIsolatedAnnihilation: Boolean =
      // Don't use sequence pattern matching here, it is too much overhead due to needing to build a sequence.
      1 == eventsArrangedInReverseTimeOrder.size && PartialFunction.cond(
        eventsArrangedInReverseTimeOrder.head) {
        case (_, EndOfLifecycle(_)) => true
      }

    def isRelevantTo(eventId: EventId): Boolean =
      itemStateUpdateTimesByEventId
        .get(eventId)
        .fold(false)(_.exists(eventsArrangedInReverseTimeOrder.contains))

    def typeBoundsAreInconsistentWith(another: Lifecycle): Boolean =
      this.upperClazzIsConsistentWith(another) ^ this
        .lowerClazzIsConsistentWith(another)

    def itemStateUpdates(lifecyclesById: LifecyclesById,
                         bestPatchSelection: BestPatchSelection)
      : Set[(ItemStateUpdateKey, ItemStateUpdate)] =
      Timer.timed(category = "itemStateUpdates") {
        type ResultsWriter[X] =
          Writer[Set[(ItemStateUpdateKey, ItemStateUpdate)], X]

        class PatchAccumulationState(
            accumulatedPatchesByExemplarMethod: Map[
              Method,
              List[(AbstractPatch, ItemStateUpdateKey)]] = Map.empty) {
          def recordChangePatch(
              itemStateUpdateKey: ItemStateUpdateKey,
              patch: AbstractPatch): ResultsWriter[PatchAccumulationState] =
            for {
              patchAccumulationStateWithChangePatch <- this
                .recordMeasurementPatch(itemStateUpdateKey, patch)
              patchAccumulationStateAfterFlushing <- patchAccumulationStateWithChangePatch
                .writeBestPatch(patch.method)
            } yield patchAccumulationStateAfterFlushing

          def recordMeasurementPatch(
              itemStateUpdateKey: ItemStateUpdateKey,
              patch: AbstractPatch): ResultsWriter[PatchAccumulationState] = {
            val (exemplarMethod, associatedPatches) =
              exemplarMethodAndPatchesFor(patch.method)
                .getOrElse(patch.method -> List.empty)

            val updatedCandidatePatches = (patch, itemStateUpdateKey) :: associatedPatches

            val updatedAccumulatedPatchesByExemplarMethod =
              if (WorldImplementationCodeFactoring
                    .firstMethodIsOverrideCompatibleWithSecond(exemplarMethod,
                                                               patch.method))
                accumulatedPatchesByExemplarMethod - exemplarMethod + (patch.method -> updatedCandidatePatches)
              else
                accumulatedPatchesByExemplarMethod + (exemplarMethod -> updatedCandidatePatches)

            new PatchAccumulationState(
              accumulatedPatchesByExemplarMethod =
                updatedAccumulatedPatchesByExemplarMethod)
              .pure[ResultsWriter]
          }

          def recordAnnihilation(itemStateUpdateKey: ItemStateUpdateKey,
                                 annihilation: Annihilation)
            : ResultsWriter[PatchAccumulationState] =
            for {
              _ <- Set(
                itemStateUpdateKey -> (ItemStateAnnihilation(annihilation
                  .rewriteItemClass(lowerBoundClazz)): ItemStateUpdate)).tell
            } yield
              this // We can get away with this (ha-ha) because an annihilation must be the latest event, so comes *first*, so there will be no patches to select from.

          def writeBestPatch(method: Method)
            : Writer[Set[(ItemStateUpdateKey, ItemStateUpdate)],
                     PatchAccumulationState] = {
            val Some((exemplarMethod, candidatePatches)) =
              exemplarMethodAndPatchesFor(method)

            val (bestPatch, itemStateUpdateKeyForBestPatch) =
              bestPatchSelection(candidatePatches)

            val (_, itemStateUpdateKeyForAnchorPatchRepresentingTheEvent) =
              candidatePatches.head

            val relatedItems: Seq[UniqueItemSpecification] =
              bestPatch.argumentItemSpecifications

            val lifecyclesForRelatedItemsFromThePerspectiveOfTheBestPatch
              : Set[Lifecycle] = relatedItems
              .map(
                uniqueItemSpecification =>
                  Lifecycle.lifecycleFor(itemStateUpdateKeyForBestPatch,
                                         uniqueItemSpecification,
                                         lifecyclesById))
              .toSet

            val lifecyclesForRelatedItemsFromThePerspectiveOfTheAnchorPatch
              : Set[Lifecycle] = relatedItems
              .map(
                uniqueItemSpecification =>
                  Lifecycle.lifecycleFor(
                    itemStateUpdateKeyForAnchorPatchRepresentingTheEvent,
                    uniqueItemSpecification,
                    lifecyclesById))
              .toSet

            val lifecyclesStartingAfterTheAnchorPatch = lifecyclesForRelatedItemsFromThePerspectiveOfTheBestPatch diff lifecyclesForRelatedItemsFromThePerspectiveOfTheAnchorPatch

            if (lifecyclesStartingAfterTheAnchorPatch.nonEmpty) {
              throw new RuntimeException(
                s"Attempt to execute patch involving items: '$id': '${lifecyclesStartingAfterTheAnchorPatch map (_.uniqueItemSpecification)}' whose lifecycles start later than: $itemStateUpdateKeyForBestPatch.")
            }

            new PatchAccumulationState(
              accumulatedPatchesByExemplarMethod = accumulatedPatchesByExemplarMethod - exemplarMethod)
              .writer(
                Set(
                  itemStateUpdateKeyForAnchorPatchRepresentingTheEvent -> ItemStatePatch(
                    bestPatch)))
          }

          def writeBestPatches
            : Writer[Set[(ItemStateUpdateKey, ItemStateUpdate)],
                     PatchAccumulationState] =
            Foldable[Iterable]
              .foldLeftM(accumulatedPatchesByExemplarMethod.keys, this) {
                case (patchAccumulationState: PatchAccumulationState,
                      method: Method) =>
                  patchAccumulationState.writeBestPatch(method)
              }

          private def exemplarMethodAndPatchesFor(method: Method)
            : Option[(Method, List[(AbstractPatch, ItemStateUpdateKey)])] =
            accumulatedPatchesByExemplarMethod.get(method) map (method -> _) orElse {
              accumulatedPatchesByExemplarMethod.find {
                case (exemplarMethod, _) =>
                  WorldImplementationCodeFactoring
                    .firstMethodIsOverrideCompatibleWithSecond(
                      method,
                      exemplarMethod) ||
                    WorldImplementationCodeFactoring
                      .firstMethodIsOverrideCompatibleWithSecond(exemplarMethod,
                                                                 method)
              }
            }
        }

        val writtenState: ResultsWriter[PatchAccumulationState] =
          Foldable[Iterable].foldLeftM(
            eventsArrangedInReverseTimeOrder,
            new PatchAccumulationState(): PatchAccumulationState) {
            case (patchAccumulationState: PatchAccumulationState,
                  (itemStateUpdateKey: ItemStateUpdateKey,
                   indivisibleEvent: IndivisibleEvent)) =>
              indivisibleEvent match {
                case _: ArgumentReference =>
                  patchAccumulationState.pure[ResultsWriter]
                case IndivisibleChange(patch) =>
                  patchAccumulationState.recordChangePatch(
                    itemStateUpdateKey,
                    patch.rewriteItemClazzes(
                      refineClazzFor(itemStateUpdateKey, _, lifecyclesById)))
                case IndivisibleMeasurement(patch) =>
                  patchAccumulationState.recordMeasurementPatch(
                    itemStateUpdateKey,
                    patch.rewriteItemClazzes(
                      refineClazzFor(itemStateUpdateKey, _, lifecyclesById)))
                case EndOfLifecycle(annihilation) =>
                  require(endTime == itemStateUpdateKey)
                  patchAccumulationState.recordAnnihilation(itemStateUpdateKey,
                                                            annihilation)
              }
          }

        val writtenStateWithFinalBestPatchesWritten =
          writtenState.flatMap(_.writeBestPatches)

        writtenStateWithFinalBestPatchesWritten.run._1
      }

    def referencingLifecycles(lifecyclesById: LifecyclesById): Set[Lifecycle] =
      Timer.timed(category = "referencingLifecycles") {
        eventsArrangedInReverseTimeOrder.collect {
          case (itemStateUpdateTime,
                ArgumentReference(_, targetUniqueItemSpecification))
              if targetUniqueItemSpecification != uniqueItemSpecification =>
            lifecycleFor(itemStateUpdateTime,
                         targetUniqueItemSpecification,
                         lifecyclesById)
        }.toSet
      }

    private def overlapsWith(another: Lifecycle): Boolean =
      Ordering[ItemStateUpdateTime]
        .lteq(this.startTime, another.endTime) && Ordering[ItemStateUpdateTime]
        .lteq(another.startTime, this.endTime)

    private def lowerClazzIsConsistentWith(another: Lifecycle): Boolean =
      another.lowerBoundClazz.isAssignableFrom(this.lowerBoundClazz) || this.lowerBoundClazz
        .isAssignableFrom(another.lowerBoundClazz)

    private def upperClazzIsConsistentWith(another: Lifecycle): Boolean =
      this.upperBoundClazz
        .isAssignableFrom(another.upperBoundClazz) || another.upperBoundClazz
        .isAssignableFrom(this.upperBoundClazz)
  }

  trait LifecycleContracts extends Lifecycle {
    require(clazzes.nonEmpty)

    require(eventsArrangedInReverseTimeOrder.nonEmpty)

    require(
      !eventsArrangedInReverseTimeOrder.tail.exists(PartialFunction.cond(_) {
        case (_, _: EndOfLifecycle) => true
      }))

    require(
      itemStateUpdateTimesByEventId.nonEmpty && itemStateUpdateTimesByEventId
        .forall { case (_, times) => times.nonEmpty })

    require(eventsArrangedInReverseTimeOrder.keys.forall(itemStateUpdateKey =>
      itemStateUpdateTimesByEventId.exists {
        case (_, itemStateUpdateTimes) =>
          itemStateUpdateTimes.contains(itemStateUpdateKey)
    }))

    require(upperBoundClazz.isAssignableFrom(lowerBoundClazz))

    require(Ordering[ItemStateUpdateTime].lteq(startTime, endTime))

    abstract override def annul(eventId: EventId): Option[Lifecycle] = {
      require(isRelevantTo(eventId))
      super.annul(eventId)
    }

    abstract override def fuseWith(another: Lifecycle): FusionResult = {
      require(isFusibleWith(another))
      super.fuseWith(another)
    }

    abstract override def isFusibleWith(another: Lifecycle): Boolean = {
      require(this.id == another.id)
      super.isFusibleWith(another)
    }

    abstract override def itemStateUpdates(
        lifecyclesById: LifecyclesById,
        bestPatchSelection: BestPatchSelection)
      : Set[(ItemStateUpdateKey, ItemStateUpdate)] = {
      val id = uniqueItemSpecification.id
      require(lifecyclesById.contains(id))
      require(
        lifecyclesById(id)
          .filterOverlaps(this)
          .contains(this))
      super.itemStateUpdates(lifecyclesById, bestPatchSelection)
    }
  }

  type Lifecycles = RangedSeq[Lifecycle, Split[ItemStateUpdateTime]]

  val noLifecycles = RangedSeq.empty[Lifecycle, Split[ItemStateUpdateTime]]

  type LifecyclesById = Map[Any, Lifecycles]

  // NOTE: an event footprint an cover several item state updates, each of which in turn can affect several items.
  case class EventFootprint(when: Unbounded[Instant], itemIds: Set[Any])

  object bestPatchSelection extends BestPatchSelectionImplementation

  case class CalculationState(defunctLifecycles: Set[Lifecycle],
                              newLifecycles: Set[Lifecycle],
                              lifecyclesById: LifecyclesById) {
    require(defunctLifecycles.intersect(newLifecycles).isEmpty)

    def flatMap(step: LifecyclesById => CalculationState): CalculationState = {
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
          ItemStateUpdateTime] =
          PriorityMap[Lifecycle, ItemStateUpdateTime](
            newLifecycles.toSeq.map(lifecycle =>
              lifecycle -> lifecycle.startTime): _*)): CalculationState = {
      if (priorityQueueOfLifecyclesConsideredForFusionOrAddition.nonEmpty) {
        val (candidateForFusion, _) =
          priorityQueueOfLifecyclesConsideredForFusionOrAddition.head

        val itemId = candidateForFusion.id

        val overlappingLifecycles = lifecyclesById
          .get(itemId)
          .fold(Seq.empty[Lifecycle]) { lifecycle => // NASTY HACK - the 'RangedSeq' API has a strange way of dealing with intervals - have to work around it here...
            val startTime = Split.alignedWith(
              candidateForFusion.startTime: ItemStateUpdateTime)
            val endTime = Split.alignedWith(candidateForFusion.endTime)
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
            s"There is at least one item of id: '${itemId}' that would be inconsistent with type: '${candidateForFusion.lowerBoundClazz}', these have types: '${conflictingLifecycles map (_.lowerBoundClazz)}'.")
        }

        val fusibleLifecycles =
          overlappingLifecycles
            .filter(_.isFusibleWith(candidateForFusion))

        val fusibleLifecyclesThatAreNotOccludedByAnnihilations = {
          def filterOccluded(fusibleLifecycles: List[Lifecycle],
                             cutoff: ItemStateUpdateTime): List[Lifecycle] =
            fusibleLifecycles match {
              case Nil => Nil
              case head :: tail
                  if Ordering[ItemStateUpdateTime].lteq(head.startTime,
                                                        cutoff) =>
                head :: filterOccluded(
                  tail,
                  Ordering[ItemStateUpdateTime].min(cutoff, head.endTime))
              case _ => Nil
            }
          filterOccluded(fusibleLifecycles, candidateForFusion.endTime)
        }

        fusibleLifecyclesThatAreNotOccludedByAnnihilations match {
          case Nil =>
            this.fuseLifecycles(
              priorityQueueOfLifecyclesConsideredForFusionOrAddition.drop(1))
          case matchForFusion :: Nil =>
            candidateForFusion.fuseWith(matchForFusion) match {
              case LifecycleMerge(mergedLifecycle) =>
                val nextState = this.flatMap(
                  lifecyclesById =>
                    CalculationState(
                      defunctLifecycles =
                        Set(candidateForFusion, matchForFusion),
                      newLifecycles = Set(mergedLifecycle),
                      lifecyclesById = lifecyclesById.updated(
                        itemId,
                        lifecyclesById(itemId) - matchForFusion - candidateForFusion + mergedLifecycle)
                  ))

                nextState.fuseLifecycles(
                  priorityQueueOfLifecyclesConsideredForFusionOrAddition
                    .drop(1) + (mergedLifecycle -> mergedLifecycle.startTime))
              case LifecycleSplit(firstLifecycle, secondLifecycle) =>
                val nextState = this.flatMap(
                  lifecyclesById =>
                    CalculationState(
                      defunctLifecycles =
                        Set(candidateForFusion, matchForFusion),
                      newLifecycles = Set(firstLifecycle, secondLifecycle),
                      lifecyclesById = lifecyclesById.updated(
                        itemId,
                        lifecyclesById(itemId) - matchForFusion - candidateForFusion + firstLifecycle + secondLifecycle)
                  ))

                nextState.fuseLifecycles(
                  priorityQueueOfLifecyclesConsideredForFusionOrAddition
                    .drop(1) + (firstLifecycle -> firstLifecycle.startTime) + (secondLifecycle -> secondLifecycle.startTime))
            }

          case matchesForFusion =>
            if (candidateForFusion.isIsolatedAnnihilation) {
              val eventId =
                candidateForFusion.itemStateUpdateTimesByEventId.head._1
              val (itemStateUpdateTime, endOfLifecycle: EndOfLifecycle) =
                candidateForFusion.eventsArrangedInReverseTimeOrder.last
              val candidatesForFusionWithRefinedLowerBound =
                matchesForFusion.zipWithIndex.map {
                  case (matchForFusion, intraEventIndex) =>
                    Lifecycle(
                      eventId = eventId,
                      itemStateUpdateKey = itemStateUpdateTime.copy(
                        intraEventIndex = intraEventIndex),
                      indivisibleEvent = endOfLifecycle.copy(
                        annihilation = endOfLifecycle.annihilation
                          .rewriteItemClass(matchForFusion.lowerBoundClazz))
                    )
                }.toSet
              val nextState = this.flatMap(
                lifecyclesById =>
                  CalculationState(
                    defunctLifecycles = Set(candidateForFusion),
                    newLifecycles = candidatesForFusionWithRefinedLowerBound,
                    lifecyclesById = lifecyclesById.updated(
                      itemId,
                      ((lifecyclesById(itemId) - candidateForFusion) /: candidatesForFusionWithRefinedLowerBound)(
                        _ + _))
                ))

              nextState.fuseLifecycles(
                priorityQueueOfLifecyclesConsideredForFusionOrAddition
                  .drop(1) ++ (candidatesForFusionWithRefinedLowerBound map (
                    lifecycle => lifecycle -> lifecycle.startTime)))
            } else
              throw new scala.RuntimeException(
                s"There is more than one item of id: '${itemId}' compatible with type: '${candidateForFusion.lowerBoundClazz}', these have types: '${fusibleLifecycles map (_.lowerBoundClazz)}'.")
        }
      } else this
    }
  }
}

class AllEventsImplementation(
    nextRevision: Revision = initialRevision,
    lifecycleFootprintPerEvent: Map[EventId,
                                    AllEventsImplementation.EventFootprint] =
      Map.empty,
    lifecyclesById: LifecyclesById = Map.empty,
    bestPatchSelection: BestPatchSelection = bestPatchSelection)
    extends AllEvents {
  val sampleLifecyclesById = lifecyclesById.take(maxNumberOfIdsToSample)

  require(sampleLifecyclesById.values.forall(_.nonEmpty))

  sampleLifecyclesById.foreach {
    case (id, lifecycles: Lifecycles) =>
      for {
        (oneLifecycle, index) <- lifecycles.iterator.zipWithIndex
        _ = require(!oneLifecycle.isIsolatedAnnihilation)
        anotherLifecycle <- lifecycles.iterator.take(index)
      } {
        require(
          !oneLifecycle.isFusibleWith(anotherLifecycle),
          s"Found counterexample where lifecycles should have been fused together, id: $id, one lifecycle is: $oneLifecycle, the other is: $anotherLifecycle"
        )
      }
  }

  override type AllEventsType = AllEventsImplementation

  override def revise(events: Map[_ <: EventId, Option[Event]])
    : ItemStateUpdatesDelta[AllEventsType] =
    Timer.timed(category = "AllEventsImplementation.revise") {
      val initialCalculationState =
        CalculationState(defunctLifecycles = Set.empty,
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

      val CalculationState(finalDefunctLifecycles,
                           finalNewLifecycles,
                           finalLifecyclesById) =
        calculationStateWithSimpleLifecyclesAddedIn.fuseLifecycles()

      // NOTE: is it really valid to use *'lifecycleById'* with 'finalDefunctLifecycles'? Yes, because any lifecycle *not* in 'lifecycleById'
      // either makes it all the way through in the above code to 'finalLifecyclesById', or is made defunct itself and thrown away by the
      // balancing done when flat-mapping a 'CalculationState'.
      val itemStateUpdatesFromDefunctLifecycles
        : Set[(ItemStateUpdateKey, ItemStateUpdate)] =
        Timer.timed(category = "itemStateUpdatesFromDefunctLifecycles") {
          (finalDefunctLifecycles ++ finalDefunctLifecycles.flatMap(
            _.referencingLifecycles(lifecyclesById)))
            .flatMap(_.itemStateUpdates(lifecyclesById, bestPatchSelection))
        }

      val itemStateUpdatesFromNewOrModifiedLifecycles
        : Set[(ItemStateUpdateKey, ItemStateUpdate)] =
        Timer.timed(category = "itemStateUpdatesFromNewOrModifiedLifecycles") {
          (finalNewLifecycles ++ finalNewLifecycles.flatMap(
            _.referencingLifecycles(finalLifecyclesById)))
            .flatMap(
              _.itemStateUpdates(finalLifecyclesById, bestPatchSelection))
        }

      val itemStateUpdateKeysThatNeedToBeRevoked: Set[ItemStateUpdateKey] =
        Timer.timed(category = "itemStateUpdateKeysThatNeedToBeRevoked") {
          (itemStateUpdatesFromDefunctLifecycles -- itemStateUpdatesFromNewOrModifiedLifecycles)
            .map(_._1)
        }

      val newOrModifiedItemStateUpdates
        : Map[ItemStateUpdateKey, ItemStateUpdate] =
        Timer.timed(category = "newOrModifiedItemStateUpdates") {
          (itemStateUpdatesFromNewOrModifiedLifecycles -- itemStateUpdatesFromDefunctLifecycles).toMap
        }

      val allEventIdsBookedIn: Set[EventId] =
        events.keySet.asInstanceOf[Set[EventId]]

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
        AllEventsImplementation.EventFootprint] = lifecycleFootprintPerEvent -- allEventIdsBookedIn

      val finalLifecycleFootprintPerEvent =
        (lifecycleFootprintPerEventWithoutEventIdsForThisRevisionBooking /: newAndModifiedEvents) {
          case (lifecycleFootprintPerEvent, (eventId, event)) =>
            lifecycleFootprintPerEvent + (eventId -> eventFootprintFrom(event))
        }

      ItemStateUpdatesDelta(
        allEvents = new AllEventsImplementation(
          nextRevision = 1 + this.nextRevision,
          lifecycleFootprintPerEvent = finalLifecycleFootprintPerEvent,
          lifecyclesById = finalLifecyclesById,
          bestPatchSelection = this.bestPatchSelection
        ),
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
          (lifecycles
            .filterIncludes(timespanUpToAndIncludingTheCutoff) ++ lifecycles
            .filterOverlaps(timespanUpToAndIncludingTheCutoff))
            .partition(lifecycle =>
              Ordering[ItemStateUpdateTime].lteq(lifecycle.endTime, cutoff))

        (noLifecycles /: (retainedUnchangedLifecycles ++ retainedTrimmedLifecycles
          .flatMap(_.retainUpTo(when))))(_ + _)
      } filter (_._2.nonEmpty),
      bestPatchSelection = this.bestPatchSelection
    )
  }

  private def annul(lifecyclesById: LifecyclesById,
                    eventId: EventId): CalculationState = {
    lifecycleFootprintPerEvent.get(eventId) match {
      case Some(EventFootprint(when, itemIds)) =>
        val lifecyclesWithRelevantIds
          : IterableView[(Any, Lifecycles), Iterable[_]] =
          itemIds.view.map(itemId => itemId -> lifecyclesById(itemId))
        val lifecyclesWithIrrelevantIds
          : LifecyclesById = lifecyclesById -- itemIds

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
          lifecyclesById = lifecyclesWithIrrelevantIds ++ lifecyclesByIdWithAnnulments
            .filter(_._2.nonEmpty)
            .toMap
        )

      case None =>
        CalculationState(defunctLifecycles = Set.empty,
                         newLifecycles = Set.empty,
                         lifecyclesById = lifecyclesById)
    }
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
                  patch = patch) +: patch.argumentItemSpecifications.collect {
                  case uniqueItemSpecification
                      if patch.targetItemSpecification != uniqueItemSpecification =>
                    Lifecycle.fromArgumentTypeReference(
                      eventId = eventId,
                      itemStateUpdateKey = itemStateUpdateKey,
                      uniqueItemSpecification = uniqueItemSpecification,
                      targetUniqueItemSpecification =
                        patch.targetItemSpecification
                    )
                }
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
                      uniqueItemSpecification = uniqueItemSpecification,
                      targetUniqueItemSpecification =
                        patch.targetItemSpecification
                  ))
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

  override def startOfFollowingLifecycleFor(
      uniqueItemSpecification: UniqueItemSpecification,
      itemStateUpdateTime: ItemStateUpdateTime): Option[ItemStateUpdateKey] = {
    val UniqueItemSpecification(itemId, itemClazz) = uniqueItemSpecification

    val timespanGoingBeyondItemStateUpdateKey = Split.alignedWith(
      itemStateUpdateTime: ItemStateUpdateTime) -> Split.upperBoundOf(
      sentinelForEndTimeOfLifecycleWithoutAnnihilation: ItemStateUpdateTime)

    lifecyclesById.get(itemId).flatMap { lifecycles =>
      val lifecycleIterator: Iterator[Lifecycle] = lifecycles
        .filterOverlaps(timespanGoingBeyondItemStateUpdateKey)
        .filter(lifecycle =>
          Ordering[ItemStateUpdateTime].gt(lifecycle.startTime,
                                           itemStateUpdateTime))
        .filter(itemClazz == _.lowerBoundClazz)

      if (lifecycleIterator.hasNext) Some(lifecycleIterator.next().startTime)
      else None
    }
  }
}
