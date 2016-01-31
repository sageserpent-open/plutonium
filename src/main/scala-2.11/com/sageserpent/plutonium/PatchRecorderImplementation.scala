package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{Finite, Unbounded}
import scala.collection.mutable
import scala.reflect.runtime._
import scala.reflect.runtime.universe._


/**
  * Created by Gerard on 10/01/2016.
  */
trait PatchRecorderImplementation extends PatchRecorder {
  // TODO: this implementation is a disaster regarding exception safety!
  self: BestPatchSelection with IdentifiedItemFactory =>
  private var _whenEventPertainedToByLastRecordingTookPlace: Option[Unbounded[Instant]] = None

  private var _allRecordingsAreCaptured = false

  override def whenEventPertainedToByLastRecordingTookPlace: Option[Unbounded[Instant]] = _whenEventPertainedToByLastRecordingTookPlace

  override def allRecordingsAreCaptured: Boolean = _allRecordingsAreCaptured

  override def recordPatchFromChange(when: Unbounded[Instant], patch: AbstractPatch[Identified]): Unit = {
    _whenEventPertainedToByLastRecordingTookPlace = Some(when)

    val (_, candidatePatches) = relevantItemStateFor(patch)

    submitCandidatePatches(candidatePatches)

    candidatePatches += nextSequenceIndex() -> patch
  }

  override def recordPatchFromMeasurement(when: Unbounded[Instant], patch: AbstractPatch[Identified]): Unit = {
    _whenEventPertainedToByLastRecordingTookPlace = Some(when)

    relevantItemStateFor(patch)._2 += nextSequenceIndex() -> patch
  }

  override def recordAnnihilation[Raw <: Identified : TypeTag](when: Instant, id: Raw#Id): Unit = {
    _whenEventPertainedToByLastRecordingTookPlace = Some(Finite(when))

    idToItemStatesMap.get(id) match {
      case Some(itemStates) => {
        val compatibleItemStates = itemStates filter { case (itemType, _) => itemType <:< typeOf[Raw] }

        if (compatibleItemStates.nonEmpty) {
          for (itemState <- compatibleItemStates) {
            submitCandidatePatches(itemState._2)
          }

          itemStates --= compatibleItemStates

          val sequenceIndex = nextSequenceIndex()

          actionQueue.enqueue(sequenceIndex -> (Unit => {
            annihilateItemsFor(id, when)
          }))
        } else throw new RuntimeException(s"Attempt to annihilate item of id: $id that does not exist at: $when.")
      }
      case None => throw new RuntimeException(s"Attempt to annihilate item of id: $id that does not exist at: $when.")
    }
  }

  override def noteThatThereAreNoFollowingRecordings(): Unit = {
    _allRecordingsAreCaptured = true

    for (itemState <- idToItemStatesMap.values.flatten){
      submitCandidatePatches(itemState._2)
    }

    idToItemStatesMap.clear()

    while (actionQueue.nonEmpty) {
      val (_, actionToBeExecuted) = actionQueue.dequeue()
      actionToBeExecuted()
    }
  }

  private type CandidatePatches = mutable.Map[SequenceIndex, AbstractPatch[Identified]]

  private type ItemState = (Type, CandidatePatches)

  private val idToItemStatesMap = scala.collection.mutable.Map.empty[Any, scala.collection.mutable.Set[ItemState]]

  private type SequenceIndex = Long

  private var _nextSequenceIndex: SequenceIndex = 0L;

  private type IndexedAction = (SequenceIndex, Unit => Unit)

  implicit val indexedActionOrdering = Ordering.by[IndexedAction, SequenceIndex](_._1)

  private val actionQueue = mutable.PriorityQueue[IndexedAction]()


  private def relevantItemStateFor(patch: AbstractPatch[Identified]) = {
    val itemStates = idToItemStatesMap.getOrElseUpdate(patch.id, scala.collection.mutable.Set.empty)

    val compatibleItemStates = itemStates filter { case (itemType, _) => itemType <:< patch.itemType }

    if (compatibleItemStates.nonEmpty) if (1 < compatibleItemStates.size) {
      throw new scala.RuntimeException(s"There is more than one item of id: '${patch.id}' compatible with type '${patch.itemType}', these have types: '${compatibleItemStates map (_._1)}'.")
    } else {
      compatibleItemStates.head
    }
    else {
      val itemState = patch.itemType -> mutable.Map.empty[SequenceIndex, AbstractPatch[Identified]]
      itemStates += itemState
      itemState
    }
  }

  private def submitCandidatePatches(candidatePatches: CandidatePatches): Unit = {
    if (candidatePatches.nonEmpty) {
      val bestPatch = self(candidatePatches.values.toSeq)

      // Ugh...
      val sequenceIndex = candidatePatches.find({case (_, patch) => patch == bestPatch}).get._1

      actionQueue.enqueue(sequenceIndex -> (Unit => {
        bestPatch(self)
      }))

      candidatePatches.clear()
    }
  }

  private def nextSequenceIndex() = {
    val result = _nextSequenceIndex
    _nextSequenceIndex += 1
    result
  }

  // TODO - this is for the future...
  def createItemOfType(itemType: Type, id: Any): Any = {
    val clazz = currentMirror.runtimeClass(itemType.typeSymbol.asClass)
    val proxyClassSymbol = currentMirror.classSymbol(clazz)
    val classMirror = currentMirror.reflectClass(proxyClassSymbol.asClass)
    val constructor = itemType.decls.find(_.isConstructor).get
    classMirror.reflectConstructor(constructor.asMethod)(id)
  }
}
