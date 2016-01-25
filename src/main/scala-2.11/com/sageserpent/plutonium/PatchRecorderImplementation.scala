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
  self: BestPatchSelection with IdentifiedItemFactory =>
  override val whenEventPertainedToByLastRecordingTookPlace: Option[Unbounded[Instant]] = _whenEventPertainedToByLastRecordingTookPlace

  override val allRecordingsAreCaptured: Boolean = _allRecordingsAreCaptured

  override def recordPatchFromChange(when: Unbounded[Instant], patch: AbstractPatch[Identified]): Unit = {
    _whenEventPertainedToByLastRecordingTookPlace = Some(when)
  }

  override def recordPatchFromMeasurement(when: Unbounded[Instant], patch: AbstractPatch[Identified]): Unit = {
    _whenEventPertainedToByLastRecordingTookPlace = Some(when)

    val itemStates = idToItemStatesMap.getOrElseUpdate(patch.id, scala.collection.mutable.Set.empty)

    // TODO - linear search through item states, looking for either no matches or a single match.
  }

  override def recordAnnihilation[Raw <: Identified : TypeTag](when: Instant, id: Raw#Id): Unit = {
    _whenEventPertainedToByLastRecordingTookPlace = Some(Finite(when))
  }

  override def noteThatThereAreNoFollowingRecordings(): Unit = {
    _allRecordingsAreCaptured = true
  }

  private var _whenEventPertainedToByLastRecordingTookPlace: Option[Unbounded[Instant]] = None

  private var _allRecordingsAreCaptured = false

  private type ItemState = (Type, List[AbstractPatch[Identified]])

  private val idToItemStatesMap = scala.collection.mutable.Map.empty[Any, scala.collection.mutable.Set[ItemState]]

  private val nextActionIndex = 0L;
  private val highestActionExecuted = -1L;

  private type IndexedAction = (Int, Unit => Unit)

  implicit val indexedActionOrdering = Ordering.by[IndexedAction, Int](_._1)

  private val actionQueue = mutable.PriorityQueue[IndexedAction]()

  def createItemOfType(itemType: Type, id: Any): Any = {
    val clazz = currentMirror.runtimeClass(itemType.typeSymbol.asClass)
    val proxyClassSymbol = currentMirror.classSymbol(clazz)
    val classMirror = currentMirror.reflectClass(proxyClassSymbol.asClass)
    val constructor = itemType.decls.find(_.isConstructor).get
    classMirror.reflectConstructor(constructor.asMethod)(id)
  }
}
