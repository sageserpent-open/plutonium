package com.sageserpent.plutonium

import java.lang.reflect.Method
import java.time.Instant

import com.sageserpent.americium.{Finite, Unbounded}
import com.sageserpent.plutonium.World.Revision
import com.sageserpent.plutonium.WorldReferenceImplementation.{IdentifiedItemsScopeImplementation, ScopeImplementation}
import resource.ExtractableManagedResource
import scala.collection.mutable
import scala.reflect.runtime._
import scala.reflect.runtime.universe._


/**
  * Created by Gerard on 10/01/2016.
  */
trait PatchRecorderImplementation extends PatchRecorder {
  // This class makes no pretence at exception safety - it doesn't need to in the context
  // of the client 'WorldReferenceImplementation', which provides exception safety at a higher level.
  self: BestPatchSelection =>
  val identifiedItemsScope: WorldReferenceImplementation.IdentifiedItemsScopeImplementation
  val asOf: Unbounded[Instant]
  val nextRevision: Revision
  val itemsAreLockedResource: ExtractableManagedResource[Unit]

  private var _whenEventPertainedToByLastRecordingTookPlace: Option[Unbounded[Instant]] = None

  private var _allRecordingsAreCaptured = false

  override def whenEventPertainedToByLastRecordingTookPlace: Option[Unbounded[Instant]] = _whenEventPertainedToByLastRecordingTookPlace

  override def allRecordingsAreCaptured: Boolean = _allRecordingsAreCaptured

  override def recordPatchFromChange(when: Unbounded[Instant], patch: AbstractPatch[_ <: Identified]): Unit = {
    _whenEventPertainedToByLastRecordingTookPlace = Some(when)

    val itemState = relevantItemStateFor(patch)

    itemState.submitCandidatePatches(patch.method)

    itemState.addPatch(when, patch)
  }

  override def recordPatchFromMeasurement(when: Unbounded[Instant], patch: AbstractPatch[_ <: Identified]): Unit = {
    _whenEventPertainedToByLastRecordingTookPlace = Some(when)

    relevantItemStateFor(patch).addPatch(when, patch)
  }

  def annihilateItemFor_[SubclassOfRaw <: Raw, Raw <: Identified](id: Raw#Id, typeTag: universe.TypeTag[SubclassOfRaw], when: Instant): Unit = {
    identifiedItemsScope.annihilateItemFor[SubclassOfRaw](id.asInstanceOf[SubclassOfRaw#Id], when)(typeTag)
  }

  override def recordAnnihilation[Raw <: Identified : TypeTag](when: Instant, id: Raw#Id): Unit = {
    _whenEventPertainedToByLastRecordingTookPlace = Some(Finite(when))

    idToItemStatesMap.get(id) match {
      case Some(itemStates) =>
        val expectedTypeTag = typeTag[Raw]
        val compatibleItemStates = itemStates filter (_.canBeAnnihilatedAs(expectedTypeTag))

        if (compatibleItemStates.nonEmpty) {
          for (itemState <- compatibleItemStates) {
            itemState.submitCandidatePatches()
          }

          itemStates --= compatibleItemStates

          val sequenceIndex = nextSequenceIndex()

          actionQueue.enqueue((sequenceIndex, Unit => for (itemStateToBeAnnihilated <- compatibleItemStates){
            val typeTagForSpecificItem = itemStateToBeAnnihilated.lowerBoundTypeTag
            annihilateItemFor_(id, typeTagForSpecificItem, when)
          }, Finite(when)))
        } else throw new RuntimeException(s"Attempt to annihilate item of id: $id that does not exist with the expected type of '${expectedTypeTag.tpe}' at: $when, the items that do exist have types: '${compatibleItemStates map (_.lowerBoundTypeTag.tpe) toList}'.")
      case None => throw new RuntimeException(s"Attempt to annihilate item of id: $id that does not exist at all at: $when.")
    }
  }

  override def noteThatThereAreNoFollowingRecordings(): Unit = {
    _allRecordingsAreCaptured = true

    for (itemState <- idToItemStatesMap.values.flatten) {
      itemState.submitCandidatePatches()
    }

    idToItemStatesMap.clear()
  }

  override def playPatchesUntil(when: Unbounded[Instant]): Unit = {
    while (actionQueue.nonEmpty && (actionQueue.head match {
      case (_, _, whenForAction) => whenForAction <= when
    })) {
      val (_, actionToBeExecuted, _) = actionQueue.dequeue()
      actionToBeExecuted()
    }
  }

  type CandidatePatchTuple = (SequenceIndex, AbstractPatch[_ <: Identified], Unbounded[Instant])

  private type CandidatePatches = mutable.MutableList[CandidatePatchTuple]

  private class ItemState(initialTypeTag: TypeTag[_ <: Identified]) extends IdentifiedItemAccess {
    private var _lowerBoundTypeTag = initialTypeTag

    def lowerBoundTypeTag = _lowerBoundTypeTag

    private var _upperBoundTypeTag = initialTypeTag

    def isInconsistentWith(typeTag: TypeTag[_ <: Identified]) = typeTag.tpe <:< this._upperBoundTypeTag.tpe && !isFusibleWith(typeTag)

    def isFusibleWith(typeTag: TypeTag[_ <: Identified]) = this._lowerBoundTypeTag.tpe <:< typeTag.tpe || typeTag.tpe <:< this._lowerBoundTypeTag.tpe

    def canBeAnnihilatedAs(typeTag: TypeTag[_ <: Identified]) =
      this._lowerBoundTypeTag.tpe <:< typeTag.tpe

    def addPatch(when: Unbounded[Instant], patch: AbstractPatch[_ <: Identified]) = {
      val candidatePatchTuple = (nextSequenceIndex(), patch, when)
      methodAndItsCandidatePatchTuplesFor(patch.method) match {
        case (Some((exemplarMethod, candidatePatchTuples))) =>
          candidatePatchTuples += candidatePatchTuple
          if (WorldReferenceImplementation.firstMethodIsOverrideCompatibleWithSecond(exemplarMethod, patch.method)) {
            exemplarMethodToCandidatePatchesMap -= exemplarMethod
            exemplarMethodToCandidatePatchesMap += (patch.method -> candidatePatchTuples)
          }
        case None =>
          exemplarMethodToCandidatePatchesMap += (patch.method -> mutable.MutableList(candidatePatchTuple))
      }

      if (patch.capturedTypeTag.tpe <:< this._lowerBoundTypeTag.tpe) {
        this._lowerBoundTypeTag = patch.capturedTypeTag
      } else if (this._upperBoundTypeTag.tpe <:< patch.capturedTypeTag.tpe) {
        this._upperBoundTypeTag = patch.capturedTypeTag
      }
    }

    private def methodAndItsCandidatePatchTuplesFor(method: Method): Option[(Method, CandidatePatches)] = {
      exemplarMethodToCandidatePatchesMap.find {
        case (exemplarMethod, _) => WorldReferenceImplementation.firstMethodIsOverrideCompatibleWithSecond(method, exemplarMethod) ||
          WorldReferenceImplementation.firstMethodIsOverrideCompatibleWithSecond(exemplarMethod, method)
      }
    }

    def submitCandidatePatches(): Unit =
      {
        for ((exemplarMethod, candidatePatchTuples) <- exemplarMethodToCandidatePatchesMap) {
          enqueueBestCandidatePatchFrom(candidatePatchTuples)
        }
        exemplarMethodToCandidatePatchesMap.clear()
      }

    def submitCandidatePatches(method: Method): Unit = methodAndItsCandidatePatchTuplesFor(method) match {
      case Some((exemplarMethod, candidatePatchTuples)) =>
        enqueueBestCandidatePatchFrom(candidatePatchTuples)
        exemplarMethodToCandidatePatchesMap -= exemplarMethod
      case None =>
    }

    private def enqueueBestCandidatePatchFrom(candidatePatchTuples: CandidatePatches): Unit = {
      val bestPatch = self(candidatePatchTuples.map(_._2))

      // The best patch has to be applied as if it occurred when the original
      // patch would have taken place - so it steals the latter's sequence index.
      // TODO: is there a test that demonstrates the need for this? Come to think
      // of it though, I'm not sure if a mutator could legitimately make bitemporal
      // queries of other bitemporal items; the only way an inter-item relationship
      // makes a difference is when a query is executed - and that doesn't care about
      // the precise interleaving of events on related items, only that the correct
      // ones have been applied to each item. So does this mean that the action queue
      // can be split across items?
      val (sequenceIndex, _, whenPatchOccurs) = candidatePatchTuples.head

      actionQueue.enqueue((sequenceIndex, Unit => {
        bestPatch(this)
        for (_ <- itemsAreLockedResource) {
          val scopeForInvariantCheck = new ScopeImplementation {
            override val identifiedItemsScope: IdentifiedItemsScopeImplementation = PatchRecorderImplementation.this.identifiedItemsScope
            override val nextRevision: Revision = PatchRecorderImplementation.this.nextRevision
            override val asOf: Unbounded[Instant] = PatchRecorderImplementation.this.asOf
            override val when: Unbounded[Instant] = whenPatchOccurs
          }
          bestPatch.checkInvariant(scopeForInvariantCheck)
        }
      }, whenPatchOccurs))
    }

    private var cachedItem: Option[Any] = None

    private val exemplarMethodToCandidatePatchesMap: mutable.Map[Method, CandidatePatches] = mutable.Map.empty

    def itemFor_[SubclassOfRaw <: Raw, Raw <: Identified](id: Raw#Id, typeTag: universe.TypeTag[SubclassOfRaw]): SubclassOfRaw = {
      PatchRecorderImplementation.this.identifiedItemsScope.itemFor[SubclassOfRaw](id.asInstanceOf[SubclassOfRaw#Id])(typeTag)
    }

    override def itemFor[Raw <: Identified : universe.TypeTag](id: Raw#Id): Raw = {
      cachedItem match {
        case None =>
          val typeTag = this._lowerBoundTypeTag
          val result = itemFor_(id, typeTag).asInstanceOf[Raw]
          cachedItem = Some(result)
          result
        case Some(item) =>
          item.asInstanceOf[Raw]
      }
    }
  }

  private val idToItemStatesMap = mutable.Map.empty[Any, mutable.Set[ItemState]]

  private type SequenceIndex = Long

  private var _nextSequenceIndex: SequenceIndex = 0L

  private type IndexedAction = (SequenceIndex, Unit => Unit, Unbounded[Instant])

  implicit val indexedActionOrdering = Ordering.by[IndexedAction, SequenceIndex](-_._1)

  private val actionQueue = mutable.PriorityQueue[IndexedAction]()


  private def relevantItemStateFor(patch: AbstractPatch[_ <: Identified]) = {
    val itemStates = idToItemStatesMap.getOrElseUpdate(patch.id, mutable.Set.empty)

    val clashingItemStates = itemStates filter (_.isInconsistentWith(patch.capturedTypeTag))

    if (clashingItemStates.nonEmpty){
      throw new RuntimeException(s"There is at least one item of id: '${patch.id}' that would be inconsistent with type '${patch.capturedTypeTag.tpe}', these have types: '${clashingItemStates map (_.lowerBoundTypeTag.tpe)}'.")
    }

    val compatibleItemStates = itemStates filter (_.isFusibleWith(patch.capturedTypeTag))

    if (compatibleItemStates.nonEmpty) if (1 < compatibleItemStates.size) {
      throw new scala.RuntimeException(s"There is more than one item of id: '${patch.id}' compatible with type '${patch.capturedTypeTag.tpe}', these have types: '${compatibleItemStates map (_.lowerBoundTypeTag.tpe)}'.")
    } else {
      compatibleItemStates.head
    }
    else {
      val itemState = new ItemState(patch.capturedTypeTag)
      itemStates += itemState
      itemState
    }
  }

  private def nextSequenceIndex() = {
    val result = _nextSequenceIndex
    _nextSequenceIndex += 1
    result
  }
}
