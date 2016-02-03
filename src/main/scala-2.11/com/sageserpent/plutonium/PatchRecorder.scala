package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{Finite, Unbounded}

import scalaz.std.option.optionSyntax._

import scala.reflect.runtime.universe._

/**
  * Created by Gerard on 09/01/2016.
  */

trait BestPatchSelection {
  def apply(relatedPatches: Seq[AbstractPatch[_ <: Identified]]): AbstractPatch[_ <: Identified]
}


trait BestPatchSelectionContracts extends BestPatchSelection {
  abstract override def apply(relatedPatches: Seq[AbstractPatch[_ <: Identified]]): AbstractPatch[_ <: Identified] = {
    require(relatedPatches.nonEmpty)
    require(1 == (relatedPatches map (_.id) distinct).size)
    require((for {lhs <- relatedPatches
                  rhs <- relatedPatches if lhs != rhs} yield AbstractPatch.bothPatchesReferToTheSameItem(lhs, rhs)).forall(identity))
    super.apply(relatedPatches)
  }
}


trait PatchRecorder {
  self: BestPatchSelection with IdentifiedItemFactory =>

  def whenEventPertainedToByLastRecordingTookPlace: Option[Unbounded[Instant]]

  def allRecordingsAreCaptured: Boolean

  def recordPatchFromChange(when: Unbounded[Instant], patch: AbstractPatch[_ <: Identified]): Unit

  def recordPatchFromMeasurement(when: Unbounded[Instant], patch: AbstractPatch[_ <: Identified]): Unit

  def recordAnnihilation[Raw <: Identified : TypeTag](when: Instant, id: Raw#Id): Unit

  def noteThatThereAreNoFollowingRecordings(): Unit

  def playPatchesUntil(when: Unbounded[Instant])
}

trait PatchRecorderContracts extends PatchRecorder {
  self: BestPatchSelectionContracts with IdentifiedItemFactory =>

  require(whenEventPertainedToByLastRecordingTookPlace.isEmpty)
  require(!allRecordingsAreCaptured)

  abstract override def recordPatchFromChange(when: Unbounded[Instant], patch: AbstractPatch[_ <: Identified]): Unit = {
    require(whenEventPertainedToByLastRecordingTookPlace.cata(some = when >= _, none = true))
    require(!allRecordingsAreCaptured)
    super.recordPatchFromChange(when, patch)
  }

  abstract override def recordPatchFromMeasurement(when: Unbounded[Instant], patch: AbstractPatch[_ <: Identified]): Unit = {
    require(whenEventPertainedToByLastRecordingTookPlace.cata(some = when >= _, none = true))
    require(!allRecordingsAreCaptured)
    super.recordPatchFromMeasurement(when, patch)
  }

  abstract override def recordAnnihilation[Raw <: Identified : TypeTag](when: Instant, id: Raw#Id): Unit = {
    require(whenEventPertainedToByLastRecordingTookPlace.cata(some = Finite(when) >= _, none = true))
    require(!allRecordingsAreCaptured)
    super.recordAnnihilation(when, id)
  }

  abstract override def noteThatThereAreNoFollowingRecordings(): Unit = {
    require(!allRecordingsAreCaptured)
    super.noteThatThereAreNoFollowingRecordings()
  }

  abstract override def playPatchesUntil(when: Unbounded[Instant]): Unit = {
    require(allRecordingsAreCaptured)
    super.playPatchesUntil(when)
  }
}

