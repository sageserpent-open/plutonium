package com.sageserpent.plutonium

import java.lang.reflect.Method
import java.time.Instant

import com.sageserpent.americium.Unbounded

import scala.collection.mutable

object PatchRecorderImplementation {
  private type SequenceIndex = Long
  val initialSequenceIndex: SequenceIndex = 0L
}

abstract class PatchRecorderImplementation(
    eventsHaveEffectNoLaterThan: Unbounded[Instant])
    extends PatchRecorder {
  // This class makes no pretence at exception safety - it doesn't need to in the context
  // of the client 'WorldReferenceImplementation', which provides exception safety at a higher level.
  self: BestPatchSelection =>
  import PatchRecorderImplementation._

  private var _whenEventPertainedToByLastRecordingTookPlace
    : Option[Unbounded[Instant]] = None

  private var _allRecordingsAreCaptured = false

  override def whenEventPertainedToByLastRecordingTookPlace
    : Option[Unbounded[Instant]] =
    _whenEventPertainedToByLastRecordingTookPlace

  override def allRecordingsAreCaptured: Boolean = _allRecordingsAreCaptured

  override def recordPatchFromChange(eventId: EventId,
                                     when: Unbounded[Instant],
                                     patch: AbstractPatch): Unit = {
    _whenEventPertainedToByLastRecordingTookPlace = Some(when)

    val sequenceIndex: SequenceIndex = nextSequenceIndex()

    val itemState = refineRelevantItemStatesAndYieldTarget(patch, sequenceIndex)

    itemState.submitCandidatePatches(patch.method)

    itemState.addPatch(when, patch, eventId, sequenceIndex)
  }

  override def recordPatchFromMeasurement(eventId: EventId,
                                          when: Unbounded[Instant],
                                          patch: AbstractPatch): Unit = {
    _whenEventPertainedToByLastRecordingTookPlace = Some(when)

    val sequenceIndex: SequenceIndex = nextSequenceIndex()

    refineRelevantItemStatesAndYieldTarget(patch, sequenceIndex).addPatch(
      when,
      patch,
      eventId,
      sequenceIndex)
  }

  def annihilateItemFor_[SubclassOfItem <: Item, Item](
      when: Unbounded[Instant],
      annihilation: Annihilation,
      eventId: EventId): Unit = {
    updateConsumer.captureAnnihilation(eventId, annihilation)
  }

  override def recordAnnihilation(eventId: EventId,
                                  annihilation: Annihilation): Unit = {
    _whenEventPertainedToByLastRecordingTookPlace = Some(annihilation.when)

    val UniqueItemSpecification(id, expectedClazz) =
      annihilation.uniqueItemSpecification

    idToItemStatesMap
      .get(id)
      .toSeq
      .flatten filter (!_.itemAnnihilationHasBeenNoted) match {
      case Seq() =>
        throw new RuntimeException(
          s"Attempt to annihilate item of id: $id that does not exist at all at: ${annihilation.when}.")
      case itemStates =>
        val compatibleItemStates = itemStates filter (_.canBeAnnihilatedAs(
          expectedClazz))

        val _sequenceIndex = nextSequenceIndex()

        if (compatibleItemStates.nonEmpty) {
          for (itemState <- compatibleItemStates) {
            itemState.refineClazz(expectedClazz)
            itemState.submitCandidatePatches()
            itemState.noteAnnihilation(_sequenceIndex)
          }

          actionQueue.enqueue(new IndexedAction() {
            override val sequenceIndex            = _sequenceIndex
            override val when: Unbounded[Instant] = annihilation.when

            override def perform() {
              for (itemStateToBeAnnihilated <- compatibleItemStates) {
                annihilateItemFor_(when,
                                   annihilation.rewriteItemClass(
                                     itemStateToBeAnnihilated.lowerBoundClazz),
                                   eventId)

                val itemStates = idToItemStatesMap(id)

                itemStates -= itemStateToBeAnnihilated

                if (itemStates.isEmpty) {
                  idToItemStatesMap -= id
                }
              }
            }

            override def canProceed() = true
          })

          outstandingSequenceIndices -= _sequenceIndex

          applyPatches(drainDownQueue = false)
        } else
          throw new RuntimeException(
            s"Attempt to annihilate item of id: $id that does not exist with the expected type of '${expectedClazz}' at: ${annihilation.when}, the items that do exist have types: '${compatibleItemStates map (_.lowerBoundClazz) toList}'.")
    }
  }

  override def noteThatThereAreNoFollowingRecordings(): Unit = {
    _allRecordingsAreCaptured = true

    for (itemState <- idToItemStatesMap.values.flatten filter (!_.itemAnnihilationHasBeenNoted)) {
      itemState.submitCandidatePatches()
    }

    applyPatches(drainDownQueue = true)

    idToItemStatesMap.clear()
  }

  private def applyPatches(drainDownQueue: Boolean): Unit = {
    if (drainDownQueue) {
      assert(outstandingSequenceIndices.isEmpty)
    }

    while (actionQueue.nonEmpty && {
             val action = actionQueue.head

             val actionIsNotOutOfSequence     = outstandingSequenceIndices.isEmpty || action.sequenceIndex < outstandingSequenceIndices.min
             val actionIsRelevantToCutoffTime = action.when <= eventsHaveEffectNoLaterThan
             actionIsNotOutOfSequence && actionIsRelevantToCutoffTime && (drainDownQueue || action
               .canProceed())
           }) actionQueue.dequeue().perform()
  }

  type CandidatePatchTuple =
    (SequenceIndex, AbstractPatch, Unbounded[Instant], EventId)

  private type CandidatePatches = mutable.MutableList[CandidatePatchTuple]

  private class ItemState(
      initialClazz: Class[_],
      private var _itemWouldConflictWithEarlierLifecyclePriorTo: SequenceIndex) {
    def itemWouldConflictWithEarlierLifecyclePriorTo =
      _itemWouldConflictWithEarlierLifecyclePriorTo

    def noteAnnihilation(sequenceIndex: SequenceIndex) = {
      _sequenceIndexForAnnihilation = Some(sequenceIndex)
    }

    private var _lowerBoundClazz = initialClazz

    def lowerBoundClazz = _lowerBoundClazz

    private var _upperBoundClazz = initialClazz

    def isInconsistentWith(clazz: Class[_]) =
      this._upperBoundClazz.isAssignableFrom(clazz) && !isFusibleWith(clazz)

    def isFusibleWith(clazz: Class[_]) =
      clazz.isAssignableFrom(this._lowerBoundClazz) || this._lowerBoundClazz
        .isAssignableFrom(clazz)

    def canBeAnnihilatedAs(clazz: Class[_]) =
      isFusibleWith(clazz)

    def addPatch(when: Unbounded[Instant],
                 patch: AbstractPatch,
                 eventId: EventId,
                 sequenceIndex: SequenceIndex) = {
      val candidatePatchTuple = (sequenceIndex, patch, when, eventId)
      methodAndItsCandidatePatchTuplesFor(patch.method) match {
        case Some((exemplarMethod, candidatePatchTuples)) =>
          candidatePatchTuples += candidatePatchTuple
          if (WorldImplementationCodeFactoring
                .firstMethodIsOverrideCompatibleWithSecond(exemplarMethod,
                                                           patch.method)) {
            exemplarMethodToCandidatePatchesMap -= exemplarMethod
            exemplarMethodToCandidatePatchesMap += (patch.method -> candidatePatchTuples)
          }
        case None =>
          exemplarMethodToCandidatePatchesMap += (patch.method -> mutable
            .MutableList(candidatePatchTuple))
      }
    }

    def refineClazz(clazz: Class[_]): Unit = {
      if (this._lowerBoundClazz.isAssignableFrom(clazz)) {
        this._lowerBoundClazz = clazz
      } else if (clazz.isAssignableFrom(this._upperBoundClazz)) {
        this._upperBoundClazz = clazz
      }
    }

    private def methodAndItsCandidatePatchTuplesFor(
        method: Method): Option[(Method, CandidatePatches)] = {
      // Direct use of key into map...
      exemplarMethodToCandidatePatchesMap.get(method) map (method -> _) orElse
        // ... fallback to doing a linear search if the methods are not equal, but are related.
        exemplarMethodToCandidatePatchesMap.find {
          case (exemplarMethod, _) =>
            WorldImplementationCodeFactoring
              .firstMethodIsOverrideCompatibleWithSecond(method, exemplarMethod) ||
              WorldImplementationCodeFactoring
                .firstMethodIsOverrideCompatibleWithSecond(exemplarMethod,
                                                           method)
        }
    }

    def submitCandidatePatches(): Unit = {
      for ((exemplarMethod, candidatePatchTuples) <- exemplarMethodToCandidatePatchesMap) {
        enqueueBestCandidatePatchFrom(candidatePatchTuples)
      }
      exemplarMethodToCandidatePatchesMap.clear()
    }

    def submitCandidatePatches(method: Method): Unit =
      methodAndItsCandidatePatchTuplesFor(method) match {
        case Some((exemplarMethod, candidatePatchTuples)) =>
          enqueueBestCandidatePatchFrom(candidatePatchTuples)
          exemplarMethodToCandidatePatchesMap -= exemplarMethod
        case None =>
      }

    private val exemplarMethodToCandidatePatchesMap
      : mutable.Map[Method, CandidatePatches] = mutable.Map.empty

    def sequenceIndexForAnnihilation = _sequenceIndexForAnnihilation.get

    def itemAnnihilationHasBeenNoted = _sequenceIndexForAnnihilation.isDefined

    private var _sequenceIndexForAnnihilation: Option[SequenceIndex] = None
  }

  private val idToItemStatesMap =
    mutable.Map.empty[Any, mutable.Set[ItemState]]

  private type UniqueItemSpecificationToItemStateMap =
    mutable.Map[UniqueItemSpecification, ItemState]

  private val sequenceIndexToItemStatesMap =
    mutable.Map.empty[SequenceIndex, UniqueItemSpecificationToItemStateMap]

  private var _nextSequenceIndex: SequenceIndex = initialSequenceIndex

  private abstract trait IndexedAction {
    val sequenceIndex: SequenceIndex
    val when: Unbounded[Instant]
    def perform(): Unit
    def canProceed(): Boolean
  }

  private implicit val indexedActionOrdering =
    Ordering.by[IndexedAction, SequenceIndex](-_.sequenceIndex)

  private val actionQueue = mutable.PriorityQueue[IndexedAction]()

  private val outstandingSequenceIndices =
    mutable.SortedSet.empty[SequenceIndex]

  private def enqueueBestCandidatePatchFrom(
      candidatePatchTuples: CandidatePatches): Unit = {
    val (bestPatch, sequenceIndexForBestPatch) = self(candidatePatchTuples.map {
      case (sequenceIndex, patch, _, _) => patch -> sequenceIndex
    })

    val anchorPatchRepresentingTheEvent = candidatePatchTuples.head

    // The best patch has to be applied as if it occurred when the anchor patch representing
    // the event would have taken place - so it steals the anchor patch's sequence index and physical time.
    val (sequenceIndexForAnchorPatch,
         _,
         whenTheAnchorPatchOccurs,
         eventIdForAnchorPatch) =
      anchorPatchRepresentingTheEvent

    val reconstitutionDataToItemStateMap =
      sequenceIndexToItemStatesMap.remove(sequenceIndexForBestPatch).get

    for ((UniqueItemSpecification(id, _), itemState) <- reconstitutionDataToItemStateMap) {
      if (itemState.itemWouldConflictWithEarlierLifecyclePriorTo > sequenceIndexForAnchorPatch) {
        throw new RuntimeException(
          s"Attempt to execute patch involving id: '$id' of type: '${itemState.lowerBoundClazz}' for a later lifecycle that cannot exist at time: $whenTheAnchorPatchOccurs and sequence number: $sequenceIndexForAnchorPatch, as there is at least one item from a previous lifecycle up until sequence number: ${itemState.itemWouldConflictWithEarlierLifecyclePriorTo}.")
      }
    }

    val itemStatesReferencedByBestPatch =
      reconstitutionDataToItemStateMap.values

    actionQueue.enqueue(new IndexedAction {
      override val sequenceIndex: SequenceIndex = sequenceIndexForAnchorPatch
      override val when: Unbounded[Instant]     = whenTheAnchorPatchOccurs

      override def perform() {
        val bestPatchWithLoweredClazzes = bestPatch.rewriteItemClazzes(
          reconstitutionDataToItemStateMap.mapValues(_.lowerBoundClazz))
        updateConsumer.capturePatch(whenTheAnchorPatchOccurs,
                                    eventIdForAnchorPatch,
                                    bestPatchWithLoweredClazzes)
      }
      override def canProceed() =
        itemStatesReferencedByBestPatch.forall(_.itemAnnihilationHasBeenNoted)
    })

    for ((sequenceIndex, _, _, _) <- candidatePatchTuples) {
      outstandingSequenceIndices -= sequenceIndex
    }
  }

  private def refineRelevantItemStatesAndYieldTarget(
      patch: AbstractPatch,
      sequenceIndex: SequenceIndex): ItemState = {
    val itemStates: UniqueItemSpecificationToItemStateMap = mutable.Map.empty

    def refinedItemStateFor(reconstitutionData: UniqueItemSpecification) = {
      val itemState = itemStateFor(reconstitutionData)
      itemState.refineClazz(reconstitutionData.clazz)
      itemStates += reconstitutionData -> itemState
      itemState
    }

    for (argumentReconstitutionData <- patch.argumentItemSpecifications) {
      refinedItemStateFor(argumentReconstitutionData)
    }
    val targetItemState = refinedItemStateFor(patch.targetItemSpecification)

    sequenceIndexToItemStatesMap(sequenceIndex) = itemStates

    targetItemState
  }

  private def itemStateFor(
      uniqueItemSpecification: UniqueItemSpecification): ItemState = {
    val UniqueItemSpecification(id, clazz) = uniqueItemSpecification

    val (itemStatesFromPreviousLifecycles, itemStates) = idToItemStatesMap
      .get(id)
      .toSeq
      .flatten partition (_.itemAnnihilationHasBeenNoted)

    val clashingItemStates = itemStates filter (_.isInconsistentWith(clazz))

    if (clashingItemStates.nonEmpty) {
      throw new RuntimeException(
        s"There is at least one item of id: '${id}' that would be inconsistent with type '${clazz}', these have types: '${clashingItemStates map (_.lowerBoundClazz)}'.")
    }

    //TODO: there should be a way of purging item states whose items have had their annihilation recorded... Perhaps I can do that by detecting supertype matches here or when doing subsequent annihilations?

    val itemStatesFromPreviousLifecyclesThatAreNotConsistentWithTheTypeUnderConsideration = itemStatesFromPreviousLifecycles filter (_.isInconsistentWith(
      clazz))

    val itemStatesFromPreviousLifecyclesThatAreFusibleWithTheTypeUnderConsideration = itemStatesFromPreviousLifecycles filter (_.isFusibleWith(
      clazz))

    val itemStatesFromPreviousLifecyclesThatEstablishALowerBoundOnTheNewLifecycle = itemStatesFromPreviousLifecyclesThatAreNotConsistentWithTheTypeUnderConsideration ++ itemStatesFromPreviousLifecyclesThatAreFusibleWithTheTypeUnderConsideration

    val itemCannotExistEarlierThan = if (itemStatesFromPreviousLifecyclesThatEstablishALowerBoundOnTheNewLifecycle.nonEmpty)
      itemStatesFromPreviousLifecyclesThatEstablishALowerBoundOnTheNewLifecycle map (_.sequenceIndexForAnnihilation) max
    else initialSequenceIndex

    val compatibleItemStates = itemStates filter (_.isFusibleWith(clazz))

    val itemState =
      if (compatibleItemStates.nonEmpty) if (1 < compatibleItemStates.size) {
        throw new scala.RuntimeException(
          s"There is more than one item of id: '${id}' compatible with type '${clazz}', these have types: '${compatibleItemStates map (_.lowerBoundClazz)}'.")
      } else compatibleItemStates.head
      else {
        val itemState = new ItemState(clazz, itemCannotExistEarlierThan)
        val mutableItemStates =
          idToItemStatesMap.getOrElseUpdate(id, mutable.Set.empty)
        mutableItemStates += itemState
        itemState
      }

    itemState
  }

  private def nextSequenceIndex() = {
    val result = _nextSequenceIndex
    outstandingSequenceIndices += result
    _nextSequenceIndex += 1
    result
  }
}
