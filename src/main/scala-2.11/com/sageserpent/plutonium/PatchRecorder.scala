package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{Finite, Unbounded}

import scalaz.std.option.optionSyntax._

import scala.reflect.runtime.universe._

/**
  * Created by Gerard on 09/01/2016.
  */

trait BestPatchSelection {
  def apply(relatedPatches: Seq[AbstractPatch]): AbstractPatch
}


trait BestPatchSelectionContracts extends BestPatchSelection {
  abstract override def apply(relatedPatches: Seq[AbstractPatch]): AbstractPatch = {
    require(relatedPatches.nonEmpty)
    require(1 == (relatedPatches map (_.targetId) distinct).size)
    require((for {lhs <- relatedPatches
                  rhs <- relatedPatches if lhs != rhs} yield AbstractPatch.patchesAreRelated(lhs, rhs)).forall(identity))
    super.apply(relatedPatches)
  }
}


trait PatchRecorder {
  def whenEventPertainedToByLastRecordingTookPlace: Option[Unbounded[Instant]]

  def allRecordingsAreCaptured: Boolean

  def recordPatchFromChange(when: Unbounded[Instant], patch: AbstractPatch): Unit

  def recordPatchFromMeasurement(when: Unbounded[Instant], patch: AbstractPatch): Unit

  def recordAnnihilation[Raw <: Identified : TypeTag](when: Instant, id: Raw#Id): Unit

  // TODO - fuse this with 'playPatchesUntil', but keep the contract checking...
  def noteThatThereAreNoFollowingRecordings(): Unit

}

trait PatchRecorderContracts extends PatchRecorder {
  require(whenEventPertainedToByLastRecordingTookPlace.isEmpty)
  require(!allRecordingsAreCaptured)

  abstract override def recordPatchFromChange(when: Unbounded[Instant], patch: AbstractPatch): Unit = {
    require(whenEventPertainedToByLastRecordingTookPlace.cata(some = when >= _, none = true))
    require(!allRecordingsAreCaptured)
    super.recordPatchFromChange(when, patch)
  }

  abstract override def recordPatchFromMeasurement(when: Unbounded[Instant], patch: AbstractPatch): Unit = {
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
}

