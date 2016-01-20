package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{Finite, Unbounded}

import scalaz.std.option.optionSyntax._

import scala.reflect.runtime.universe._

/**
  * Created by Gerard on 09/01/2016.
  */

trait BestPatchSelection {
  // The tacit assumption is that 'relatedPatches' contains patches
  // in the order they are picked up by the patch recorder.
  def apply(relatedPatches: Seq[Patch[Identified]]): Patch[Identified]
}


trait BestPatchSelectionContracts extends BestPatchSelection {
  abstract override def apply(relatedPatches: Seq[Patch[Identified]]): Patch[Identified] = {
    require(relatedPatches.nonEmpty)
    super.apply(relatedPatches)
  }
}


trait PatchRecorder {
  self: BestPatchSelection with IdentifiedItemFactory =>

  val whenEventPertainedToByLastRecordingTookPlace: Option[Unbounded[Instant]]

  val allRecordingsAreCaptured: Boolean

  def recordPatchFromChange(when: Unbounded[Instant], patch: Patch[Identified]): Unit

  def recordPatchFromMeasurement(when: Unbounded[Instant], patch: Patch[Identified]): Unit

  // TODO - this needs to play well with 'WorldReferenceImplementation' - may need
  // some explicit dependencies, or could fold them into the implementing subclass.
  def recordAnnihilation[Raw <: Identified: TypeTag](when: Instant, id: Raw#Id): Unit

  def noteThatThereAreNoFollowingRecordings(): Unit
}

trait PatchRecorderContracts extends PatchRecorder {
  self: BestPatchSelectionContracts with IdentifiedItemFactory =>

  require(whenEventPertainedToByLastRecordingTookPlace.isEmpty)
  require(!allRecordingsAreCaptured)

  abstract override def recordPatchFromChange(when: Unbounded[Instant], patch: Patch[Identified]): Unit = {
    require(whenEventPertainedToByLastRecordingTookPlace.cata(some = when >= _, none = true))
    require(!allRecordingsAreCaptured)
    super.recordPatchFromChange(when, patch)
  }

  abstract override def recordPatchFromMeasurement(when: Unbounded[Instant], patch: Patch[Identified]): Unit = {
    require(whenEventPertainedToByLastRecordingTookPlace.cata(some = when >= _, none = true))
    require(!allRecordingsAreCaptured)
    super.recordPatchFromMeasurement(when, patch)
  }

  abstract override def recordAnnihilation[Raw <: Identified: TypeTag](when: Instant, id: Raw#Id): Unit = {
    require(whenEventPertainedToByLastRecordingTookPlace.cata(some = Finite(when) >= _, none = true))
    require(!allRecordingsAreCaptured)
    super.recordAnnihilation(when, id)
  }

  abstract override def noteThatThereAreNoFollowingRecordings(): Unit = {
    require(!allRecordingsAreCaptured)
    super.noteThatThereAreNoFollowingRecordings()
  }
}

