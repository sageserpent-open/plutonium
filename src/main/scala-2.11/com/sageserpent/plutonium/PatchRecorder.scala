package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{Finite, Unbounded}

import scalaz.std.option.optionSyntax._

import scala.reflect.runtime.universe._

/**
  * Created by Gerard on 09/01/2016.
  */

trait BestPatchSelection[FBoundedOperationClassifier <: OperationClassifier[FBoundedOperationClassifier]] {
  def apply(relatedPatches: Seq[AbstractPatch[FBoundedOperationClassifier]]): AbstractPatch[FBoundedOperationClassifier]
}


trait BestPatchSelectionContracts[FBoundedOperationClassifier <: OperationClassifier[FBoundedOperationClassifier]] extends BestPatchSelection[FBoundedOperationClassifier] {
  abstract override def apply(relatedPatches: Seq[AbstractPatch[FBoundedOperationClassifier]]): AbstractPatch[FBoundedOperationClassifier] = {
    require(relatedPatches.nonEmpty)
    require(1 == (relatedPatches map (_.targetId) distinct).size)
    require((for {lhs <- relatedPatches
                  rhs <- relatedPatches if lhs != rhs} yield AbstractPatch.patchesAreRelated(lhs, rhs)).forall(identity))
    super.apply(relatedPatches)
  }
}


trait PatchRecorder[FBoundedOperationClassifier <: OperationClassifier[FBoundedOperationClassifier]] {
  def whenEventPertainedToByLastRecordingTookPlace: Option[Unbounded[Instant]]

  def allRecordingsAreCaptured: Boolean

  def recordPatchFromChange(when: Unbounded[Instant], patch: AbstractPatch[FBoundedOperationClassifier]): Unit

  def recordPatchFromMeasurement(when: Unbounded[Instant], patch: AbstractPatch[FBoundedOperationClassifier]): Unit

  def recordAnnihilation[Raw <: Identified : TypeTag](when: Instant, id: Raw#Id): Unit

  def noteThatThereAreNoFollowingRecordings(): Unit
}

trait PatchRecorderContracts[FBoundedOperationClassifier <: OperationClassifier[FBoundedOperationClassifier]] extends PatchRecorder[FBoundedOperationClassifier] {
  require(whenEventPertainedToByLastRecordingTookPlace.isEmpty)
  require(!allRecordingsAreCaptured)

  abstract override def recordPatchFromChange(when: Unbounded[Instant], patch: AbstractPatch[FBoundedOperationClassifier]): Unit = {
    require(whenEventPertainedToByLastRecordingTookPlace.cata(some = when >= _, none = true))
    require(!allRecordingsAreCaptured)
    super.recordPatchFromChange(when, patch)
  }

  abstract override def recordPatchFromMeasurement(when: Unbounded[Instant], patch: AbstractPatch[FBoundedOperationClassifier]): Unit = {
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

