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

    candidatePatches += patch
  }

  override def recordPatchFromMeasurement(when: Unbounded[Instant], patch: AbstractPatch[Identified]): Unit = {
    _whenEventPertainedToByLastRecordingTookPlace = Some(when)

    relevantItemStateFor(patch)._2 += patch
  }

  override def recordAnnihilation[Raw <: Identified : TypeTag](when: Instant, id: Raw#Id): Unit = {
    _whenEventPertainedToByLastRecordingTookPlace = Some(Finite(when))

    idToItemStatesMap.get(id) match {
      case Some(itemStates) => {
        val compatibleItemStates = itemStates filter { case (itemType, _) => itemType <:< typeOf[Raw] }

        for (itemState <- compatibleItemStates){
          submitCandidatePatches(itemState._2)
        }

        itemStates --= compatibleItemStates
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
  }

  private type ItemState = (Type, mutable.MutableList[AbstractPatch[Identified]])

  private val idToItemStatesMap = scala.collection.mutable.Map.empty[Any, scala.collection.mutable.Set[ItemState]]

  private var nextActionIndex = 0L;
  private var highestActionExecuted = -1L;

  private type IndexedAction = (Long, Unit => Unit)

  implicit val indexedActionOrdering = Ordering.by[IndexedAction, Long](_._1)

  private val actionQueue = mutable.PriorityQueue[IndexedAction]()


  private def relevantItemStateFor(patch: AbstractPatch[Identified]) = {
    val itemStates = idToItemStatesMap.getOrElseUpdate(patch.id, scala.collection.mutable.Set.empty)

    val compatibleItemStates = itemStates filter { case (itemType, _) => itemType <:< patch.itemType }

    if (compatibleItemStates.nonEmpty) if (1 < compatibleItemStates.size) {
      throw new scala.RuntimeException("There is more than one item of id: '${patch.id}' compatible with type '${patch.itemType}', these have types: '${compatibleItemStates map (_._1)}'.")
    } else {
      compatibleItemStates.head
    }
    else {
      val itemState = patch.itemType -> mutable.MutableList.empty[AbstractPatch[Identified]]
      itemStates += itemState
      itemState
    }
  }

  private def submitCandidatePatches(candidatePatches: mutable.MutableList[AbstractPatch[Identified]]): Unit = {
    if (candidatePatches.nonEmpty) {
      val bestPatch = self(candidatePatches)

      val actionIndex = nextActionIndex

      nextActionIndex += 1

      actionQueue.enqueue(actionIndex -> (Unit => {
        bestPatch(self)
      }))

      candidatePatches.clear()

      // So given that the best patch has been added, does this permit the action queue to be drained?
      while (1 + highestActionExecuted == actionQueue.head._1) {
        val (index, actionToBeExecuted) = actionQueue.dequeue()
        actionToBeExecuted()
        highestActionExecuted = index
      }
    }
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
