package com.sageserpent.plutonium

import java.lang.reflect.Method
import java.time.Instant

import net.sf.cglib.proxy.MethodProxy

import scalaz.std.option.optionSyntax._

/**
  * Created by Gerard on 09/01/2016.
  */

trait BestPatchSelection {
  def apply(relatedPatches: Seq[PatchRecorder#Patch]): PatchRecorder#Patch
}

trait BestPatchSelectionContracts extends BestPatchSelection {
  abstract override def apply(relatedPatches: Seq[PatchRecorder#Patch]): PatchRecorder#Patch = {
    require(relatedPatches.nonEmpty)
    super.apply(relatedPatches)
  }
}

trait PatchRecorder {
  self: BestPatchSelection =>

  case class Patch(target: Any, method: Method, arguments: Array[AnyRef], methodProxy: MethodProxy){
    def apply(): Unit = ???
  }

  val whenEventPertainedToByLastRecordingTookPlace: Option[Instant]

  val allRecordingsAreCaptured: Boolean

  def recordPatchFromChange(when: Instant, patch:Patch): Unit

  def recordPatchFromObservation(when: Instant, patch: Patch): Unit

  def recordAnnihilation(when: Instant, target: Any): Unit

  def noteThatThereAreNoFollowingRecordings(): Unit
}

trait PatchRecorderContracts extends PatchRecorder {
  self: BestPatchSelectionContracts =>

  require(whenEventPertainedToByLastRecordingTookPlace.isEmpty)
  require(!allRecordingsAreCaptured)

  abstract override def recordPatchFromChange(when: Instant, patch:Patch): Unit = {
    require(whenEventPertainedToByLastRecordingTookPlace.cata(some = !when.isAfter(_), none = true))
    require(!allRecordingsAreCaptured)
    super.recordPatchFromChange(when, patch)
  }

  abstract override def recordPatchFromObservation(when: Instant, patch: Patch): Unit = {
    require(whenEventPertainedToByLastRecordingTookPlace.cata(some = !when.isAfter(_), none = true))
    require(!allRecordingsAreCaptured)
    super.recordPatchFromObservation(when, patch)
  }

  abstract override def recordAnnihilation(when: Instant, target: Any): Unit = {
    require(whenEventPertainedToByLastRecordingTookPlace.cata(some = !when.isAfter(_), none = true))
    require(!allRecordingsAreCaptured)
    super.recordAnnihilation(when, target)
  }

  abstract override def noteThatThereAreNoFollowingRecordings(): Unit = {
    require(!allRecordingsAreCaptured)
    super.noteThatThereAreNoFollowingRecordings()
  }
}