package com.sageserpent.plutonium

import java.lang.reflect.Method
import java.time.Instant

import com.sageserpent.americium.{
  Finite,
  NegativeInfinity,
  PositiveInfinity,
  Unbounded
}
import com.sageserpent.plutonium.AllEvents.ItemStateUpdatesDelta
import com.sageserpent.plutonium.AllEventsImplementation.Lifecycle._
import com.sageserpent.plutonium.AllEventsImplementation._
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import com.sageserpent.plutonium.ItemStateUpdateTime.ordering
import com.sageserpent.plutonium.World.{Revision, initialRevision}
import de.sciss.fingertree.RangedSeq
import de.ummels.prioritymap.PriorityMap

import scala.annotation.tailrec
import scala.collection.immutable.{
  Bag,
  HashedBagConfiguration,
  Map,
  Set,
  SortedMap
}
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
        eventsArrangedInReverseTimeOrder =
          SortedMap(itemStateUpdateKey -> indivisibleEvent)(
            Ordering[ItemStateUpdateKey].reverse),
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

    def refineTypeFor(itemStateUpdateTime: ItemStateUpdateTime,
                      uniqueItemSpecification: UniqueItemSpecification,
                      lifecyclesById: LifecyclesById): TypeTag[_] = {
      lifecycleFor(itemStateUpdateTime, uniqueItemSpecification, lifecyclesById).lowerBoundTypeTag
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
            lifecycle.lowerBoundTypeTag.tpe <:< uniqueItemSpecification.typeTag.tpe)
          .toList
      relevantLifecycle
    }

    def fuse(firstLifecycle: Lifecycle, secondLifecycle: Lifecycle) = {
      val fusedTypeTags = firstLifecycle.typeTags ++ secondLifecycle.typeTags

      val fusedEventsArrangedInReverseTimeOrder: SortedMap[
        ItemStateUpdateKey,
        IndivisibleEvent] = firstLifecycle.eventsArrangedInReverseTimeOrder ++ secondLifecycle.eventsArrangedInReverseTimeOrder

      val fusedItemStateUpdateTimesByEventId
        : Map[EventId, Set[ItemStateUpdateKey]] =
        (firstLifecycle.itemStateUpdateTimesByEventId.keys ++ secondLifecycle.itemStateUpdateTimesByEventId.keys) map (
            eventId =>
              eventId ->
                firstLifecycle.itemStateUpdateTimesByEventId.getOrElse(
                  eventId,
                  Set.empty ++ secondLifecycle.itemStateUpdateTimesByEventId
                    .getOrElse(eventId, Set.empty))) toMap

      Lifecycle(
        typeTags = fusedTypeTags,
        eventsArrangedInReverseTimeOrder = fusedEventsArrangedInReverseTimeOrder,
        itemStateUpdateTimesByEventId = fusedItemStateUpdateTimesByEventId
      )
    }

    def apply(
        inclusionPredicate: ItemStateUpdateTime => Boolean,
        retainedEvents: SortedMap[ItemStateUpdateKey, IndivisibleEvent],
        trimmedEvents: SortedMap[ItemStateUpdateKey, IndivisibleEvent],
        typeTags: Bag[TypeTag[_]],
        itemStateUpdateTimesByEventId: Map[EventId, Set[ItemStateUpdateKey]])
      : Lifecycle = {
      val retainedTypeTags = (typeTags /: trimmedEvents) {
        case (typeTags, (_, trimmedEvent)) =>
          typeTags - trimmedEvent.uniqueItemSpecification.typeTag
      }

      // TODO - consider just using 'itemStateUpdateTimesByEventId' - this will result in
      // extraneous entries, however 'annul' can be made to tolerate these. Just a thought...
      val retainedItemStateUpdateTimesByEventId =
        itemStateUpdateTimesByEventId
          .mapValues(_.filter(inclusionPredicate))
          .filter {
            case (_, itemStateUpdateTimes) => itemStateUpdateTimes.nonEmpty
          }

      Lifecycle(typeTags = retainedTypeTags,
                eventsArrangedInReverseTimeOrder = retainedEvents,
                itemStateUpdateTimesByEventId =
                  retainedItemStateUpdateTimesByEventId)
    }
  }

  case class Lifecycle(
      typeTags: Bag[TypeTag[_]],
      eventsArrangedInReverseTimeOrder: SortedMap[ItemStateUpdateKey,
                                                  IndivisibleEvent],
      itemStateUpdateTimesByEventId: Map[EventId, Set[ItemStateUpdateKey]]) {
    require(typeTags.nonEmpty)

    require(eventsArrangedInReverseTimeOrder.nonEmpty)

    require(
      !eventsArrangedInReverseTimeOrder.tail.exists(PartialFunction.cond(_) {
        case (_, _: EndOfLifecycle) => true
      }))

    require(
      itemStateUpdateTimesByEventId.nonEmpty && itemStateUpdateTimesByEventId
        .forall { case (_, times) => times.nonEmpty })

    val startTime: ItemStateUpdateTime =
      eventsArrangedInReverseTimeOrder.lastKey

    // TODO: the way annihilations are just mixed in with all the other events in 'eventsArrangedInTimeOrder' feels hokey:
    // it necessitates a pesky invariant check, along with the special case logic below. Sort this out!
    val endTime: ItemStateUpdateTime =
      eventsArrangedInReverseTimeOrder.head match {
        case (itemStateUpdateTime, _: EndOfLifecycle) => itemStateUpdateTime
        case _                                        => sentinelForEndTimeOfLifecycleWithoutAnnihilation
      }

    require(Ordering[ItemStateUpdateTime].lteq(startTime, endTime))

    def isIsolatedAnnihilation: Boolean =
      PartialFunction.cond(eventsArrangedInReverseTimeOrder.toSeq) {
        case Seq((_, EndOfLifecycle(_))) => true
      }

    val endPoints: LifecycleEndPoints = startTime -> endTime

    def overlapsWith(another: Lifecycle): Boolean =
      Ordering[ItemStateUpdateTime]
        .lteq(this.startTime, another.endTime) && Ordering[ItemStateUpdateTime]
        .lteq(another.startTime, this.endTime)

    val uniqueItemSpecification: UniqueItemSpecification =
      UniqueItemSpecification(id, lowerBoundTypeTag)

    def id: Any =
      eventsArrangedInReverseTimeOrder.head._2.uniqueItemSpecification.id

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
      val inclusionPredicate =
        Ordering[ItemStateUpdateTime]
          .lteq(_: ItemStateUpdateTime, UpperBoundOfTimeslice(when))

      val (retainedEvents, trimmedEvents) =
        eventsArrangedInReverseTimeOrder.partition {
          case (itemStateUpdateTime, _) =>
            inclusionPredicate(itemStateUpdateTime)
        }

      if (retainedEvents.nonEmpty) {
        Some(
          Lifecycle(inclusionPredicate,
                    retainedEvents,
                    trimmedEvents,
                    typeTags,
                    itemStateUpdateTimesByEventId))
      } else None
    }

    def isRelevantTo(eventId: EventId): Boolean =
      itemStateUpdateTimesByEventId.contains(eventId)

    def annul(eventId: EventId): Option[Lifecycle] = {
      val itemStateUpdateTimes = itemStateUpdateTimesByEventId(eventId)
      val preservedEvents =
        (eventsArrangedInReverseTimeOrder /: itemStateUpdateTimes)(_ - _)
      if (preservedEvents.nonEmpty) {
        val annulledEvents = itemStateUpdateTimes map eventsArrangedInReverseTimeOrder.apply
        val preservedTypeTags = (typeTags /: annulledEvents) {
          case (typeTags, annulledEvent) =>
            typeTags - annulledEvent.uniqueItemSpecification.typeTag
        }
        val preservedItemStateUpdateTimesByEventId = itemStateUpdateTimesByEventId - eventId
        Some(
          Lifecycle(typeTags = preservedTypeTags,
                    eventsArrangedInReverseTimeOrder = preservedEvents,
                    itemStateUpdateTimesByEventId =
                      preservedItemStateUpdateTimesByEventId))
      } else None
    }

    // The lower type bounds are compatible and there is overlap.
    def isFusibleWith(another: Lifecycle): Boolean =
      this.lowerTypeIsConsistentWith(another) && this.upperTypeIsConsistentWith(
        another) && this.overlapsWith(another)

    def fuseWith(another: Lifecycle): FusionResult =
      this.eventsArrangedInReverseTimeOrder.head -> another.eventsArrangedInReverseTimeOrder.head match {
        case ((whenThisLifecycleEnds, _: EndOfLifecycle),
              (whenTheLastEventInTheOtherLifecycleTakesPlace, _))
            if Ordering[ItemStateUpdateTime].lt(
              whenThisLifecycleEnds,
              whenTheLastEventInTheOtherLifecycleTakesPlace) =>
          val inclusionPredicate =
            Ordering[ItemStateUpdateTime]
              .lteq(_: ItemStateUpdateTime, whenThisLifecycleEnds)

          val (eventsFromTheOtherForEarlierLifecycle,
               eventsFromTheOtherForLaterLifecycle) =
            another.eventsArrangedInReverseTimeOrder.partition {
              case (itemStateUpdateTime, _) =>
                inclusionPredicate(itemStateUpdateTime)
            }
          LifecycleSplit(
            fuse(
              this,
              Lifecycle(inclusionPredicate,
                        eventsFromTheOtherForEarlierLifecycle,
                        eventsFromTheOtherForLaterLifecycle,
                        another.typeTags,
                        another.itemStateUpdateTimesByEventId)
            ),
            Lifecycle(
              inclusionPredicate andThen (!_),
              eventsFromTheOtherForLaterLifecycle,
              eventsFromTheOtherForEarlierLifecycle,
              another.typeTags,
              another.itemStateUpdateTimesByEventId
            )
          )
        case ((whenTheLastEventInThisLifecycleTakesPlace, _),
              (whenTheOtherLifecycleEnds, _: EndOfLifecycle))
            if Ordering[ItemStateUpdateTime].lt(
              whenTheOtherLifecycleEnds,
              whenTheLastEventInThisLifecycleTakesPlace) =>
          val inclusionPredicate =
            Ordering[ItemStateUpdateTime]
              .lteq(_: ItemStateUpdateTime, whenTheOtherLifecycleEnds)

          val (eventsFromThisForEarlierLifecycle,
               eventsFromThisForLaterLifecycle) =
            this.eventsArrangedInReverseTimeOrder.partition {
              case (itemStateUpdateTime, _) =>
                inclusionPredicate(itemStateUpdateTime)
            }
          LifecycleSplit(
            fuse(
              another,
              Lifecycle(inclusionPredicate,
                        eventsFromThisForEarlierLifecycle,
                        eventsFromThisForLaterLifecycle,
                        another.typeTags,
                        another.itemStateUpdateTimesByEventId)
            ),
            Lifecycle(
              inclusionPredicate andThen (!_),
              eventsFromThisForLaterLifecycle,
              eventsFromThisForEarlierLifecycle,
              another.typeTags,
              another.itemStateUpdateTimesByEventId
            )
          )
        case _ =>
          LifecycleMerge(fuse(this, another))
      }

    def referencingLifecycles(lifecyclesById: LifecyclesById): Set[Lifecycle] =
      eventsArrangedInReverseTimeOrder.collect {
        case (itemStateUpdateTime,
              ArgumentReference(_, targetUniqueItemSpecification)) =>
          lifecycleFor(itemStateUpdateTime,
                       targetUniqueItemSpecification,
                       lifecyclesById)
      }.toSet

    def itemStateUpdates(lifecyclesById: LifecyclesById)
      : Set[(ItemStateUpdateKey, ItemStateUpdate)] = {
      // Welcome to hell...

      import scalaz.Writer
      import scalaz.Monad

      import scalaz.syntax.writer._
      import scalaz.syntax.foldable._
      import scalaz.syntax.monad._

      import scalaz.std.iterable._
      import scalaz.std.vector._

      type ResultsWriter[X] =
        Writer[Vector[(ItemStateUpdateKey, ItemStateUpdate)], X]

      class PatchAccumulationState(
          accumulatedPatchesByMethod: Map[Method,
                                          List[(AbstractPatch,
                                                ItemStateUpdateKey)]] =
            Map.empty) {
        def recordChangePatch(
            itemStateUpdateKey: ItemStateUpdateKey,
            patch: AbstractPatch): ResultsWriter[PatchAccumulationState] =
          for {
            patchAccumulationStateWithChangePatch <- this
              .recordMeasurementPatch(
                itemStateUpdateKey,
                patch.rewriteItemTypeTags(
                  refineTypeFor(itemStateUpdateKey, _, lifecyclesById)))
            // TODO: should only write the best patch for the specific method referenced by this change.
            patchAccumulationStateAfterFlushing <- patchAccumulationStateWithChangePatch
              .writeBestPatch(patch.method)
          } yield patchAccumulationStateAfterFlushing

        def recordMeasurementPatch(
            itemStateUpdateKey: ItemStateUpdateKey,
            patch: AbstractPatch): ResultsWriter[PatchAccumulationState] = {
          val method = patch.method
          // TODO - need to have flexibility on method lookup so that method signatures can be fused.
          new PatchAccumulationState(
            accumulatedPatchesByMethod = accumulatedPatchesByMethod.updated(
              method,
              (patch -> itemStateUpdateKey) :: accumulatedPatchesByMethod
                .getOrElse(method, List.empty)))
            .pure(implicitly[Monad[ResultsWriter]])
        }

        def recordAnnihilation(
            itemStateUpdateKey: ItemStateUpdateKey,
            annihilation: Annihilation): ResultsWriter[PatchAccumulationState] =
          for {
            _ <- Vector(itemStateUpdateKey -> (ItemStateAnnihilation(
              annihilation): ItemStateUpdate)).tell
          } yield
            this // We can get away with this (ha-ha) because an annihilation must be the latest event, so comes *first*, so there will be no patches to select from.

        def writeBestPatch(method: Method)
          : Writer[Vector[(ItemStateUpdateKey, ItemStateUpdate)],
                   PatchAccumulationState] = {
          // TODO - need to have flexibility on method lookup so that method signatures can be fused.
          val candidatePatches = accumulatedPatchesByMethod(method)

          // TODO - use the best patch selection strategy.
          val bestPatch = candidatePatches.last._1

          val itemStateUpdateKeyForRepresentativePatch =
            candidatePatches.head._2

          new PatchAccumulationState(
            accumulatedPatchesByMethod = accumulatedPatchesByMethod - method)
            .set(
              Vector(
                itemStateUpdateKeyForRepresentativePatch -> ItemStatePatch(
                  bestPatch)))
        }

        def writeBestPatches
          : Writer[Vector[(ItemStateUpdateKey, ItemStateUpdate)],
                   PatchAccumulationState] =
          accumulatedPatchesByMethod.keys.foldLeftM(this) {
            case (patchAccumulationState: PatchAccumulationState,
                  method: Method) =>
              patchAccumulationState.writeBestPatch(method)
          }
      }

      val writtenState: ResultsWriter[PatchAccumulationState] =
        eventsArrangedInReverseTimeOrder.toIterable.foldLeftM(
          new PatchAccumulationState(): PatchAccumulationState) {
          case (patchAccumulationState: PatchAccumulationState,
                (itemStateUpdateKey: ItemStateUpdateKey,
                 indivisibleEvent: IndivisibleEvent)) =>
            indivisibleEvent match {
              case _: ArgumentReference =>
                patchAccumulationState.pure(implicitly[Monad[ResultsWriter]])
              case IndivisibleChange(patch) =>
                patchAccumulationState.recordChangePatch(
                  itemStateUpdateKey,
                  patch.rewriteItemTypeTags(
                    refineTypeFor(itemStateUpdateKey, _, lifecyclesById)))
              case IndivisibleMeasurement(patch) =>
                patchAccumulationState.recordMeasurementPatch(
                  itemStateUpdateKey,
                  patch.rewriteItemTypeTags(
                    refineTypeFor(itemStateUpdateKey, _, lifecyclesById)))
              case EndOfLifecycle(annihilation) =>
                require(endTime == itemStateUpdateKey)
                patchAccumulationState.recordAnnihilation(itemStateUpdateKey,
                                                          annihilation)
            }
        }

      val writtenStateWithFinalBestPatchesWritten =
        for (patchAccumulationState <- writtenState)
          yield patchAccumulationState.writeBestPatches

      writtenStateWithFinalBestPatchesWritten.run._1.toSet // ... hope you had a pleasant stay.
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

    abstract override def fuseWith(another: Lifecycle): FusionResult = {
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

  object bestPatchSelection extends BestPatchSelectionImplementation
}

class AllEventsImplementation(
    nextRevision: Revision = initialRevision,
    lifecycleFootprintPerEvent: Map[EventId,
                                    AllEventsImplementation.EventFootprint] =
      Map.empty,
    lifecyclesById: LifecyclesById = Map.empty,
    bestPatchSelection: BestPatchSelection = bestPatchSelection)
    extends AllEvents {
  lifecyclesById.foreach {
    case (id, lifecycles: Lifecycles) =>
      for {
        oneLifecycle <- lifecycles.toList
        _ = require(!oneLifecycle.isIsolatedAnnihilation)
        anotherLifecycle <- lifecycles.toList
        if oneLifecycle != anotherLifecycle
      } {
        require(
          !oneLifecycle.isFusibleWith(anotherLifecycle),
          s"Found counterexample where lifecycles should have been fused together, id: $id, one lifecycle is: $oneLifecycle, the other is: $anotherLifecycle"
        )
      }
  }
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
                            .rewriteItemTypeTag(
                              matchForFusion.lowerBoundTypeTag))
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
              case (lifecycleId, _) => itemIds.contains(lifecycleId)
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
            lifecyclesById = lifecyclesWithIrrelevantIds ++ lifecyclesByIdWithAnnulments.toMap
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

    val CalculationState(finalDefunctLifecycles,
                         finalNewLifecycles,
                         finalLifecyclesById) =
      calculationStateWithSimpleLifecyclesAddedIn.fuseLifecycles()

    // NOTE: is it really valid to use *'lifecycleById'* with 'finalDefunctLifecycles'? Yes, because any lifecycle *not* in 'lifecycleById'
    // either makes it all the way through in the above code to 'finalLifecyclesById', or is made defunct itself and thrown away by the
    // balancing done when flat-mapping a 'CalculationState'.
    val itemStateUpdatesFromDefunctLifecycles
      : Set[(ItemStateUpdateKey, ItemStateUpdate)] =
      (finalDefunctLifecycles ++ finalDefunctLifecycles.flatMap(
        _.referencingLifecycles(lifecyclesById)))
        .flatMap(_.itemStateUpdates(lifecyclesById))

    val itemStateUpdatesFromNewOrModifiedLifecycles
      : Set[(ItemStateUpdateKey, ItemStateUpdate)] =
      (finalNewLifecycles ++ finalNewLifecycles.flatMap(
        _.referencingLifecycles(finalLifecyclesById)))
        .flatMap(_.itemStateUpdates(finalLifecyclesById))

    val itemStateUpdateKeysThatNeedToBeRevoked: Set[ItemStateUpdateKey] =
      (itemStateUpdatesFromDefunctLifecycles -- itemStateUpdatesFromNewOrModifiedLifecycles)
        .map(_._1)

    val newOrModifiedItemStateUpdates
      : Map[ItemStateUpdateKey, ItemStateUpdate] =
      (itemStateUpdatesFromNewOrModifiedLifecycles -- itemStateUpdatesFromDefunctLifecycles).toMap

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
      AllEventsImplementation.EventFootprint] = lifecycleFootprintPerEvent filterNot {
      case (eventId, _) => allEventIdsBookedIn.contains(eventId)
    }

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
          lifecycles
            .filterIncludes(timespanUpToAndIncludingTheCutoff)
            .partition(lifecycle =>
              Ordering[ItemStateUpdateTime].lteq(lifecycle.endTime, cutoff))

        (noLifecycles /: (retainedUnchangedLifecycles ++ retainedTrimmedLifecycles
          .flatMap(_.retainUpTo(when))))(_ + _)
      },
      bestPatchSelection = this.bestPatchSelection
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
                      uniqueItemSpecification = uniqueItemSpecification,
                      targetUniqueItemSpecification =
                        patch.targetItemSpecification
                  ))
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
}
