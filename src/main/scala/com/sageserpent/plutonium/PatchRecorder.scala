package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{Finite, Unbounded}

import scalaz.std.option.optionSyntax._

import scala.reflect.runtime.universe._

trait BestPatchSelection {
  def apply(relatedPatches: Seq[AbstractPatch]): AbstractPatch
}

trait BestPatchSelectionContracts extends BestPatchSelection {
  abstract override def apply(
      relatedPatches: Seq[AbstractPatch]): AbstractPatch = {
    require(relatedPatches.nonEmpty)
    require(1 == (relatedPatches map (_.targetId) distinct).size)
    require((for {
      lhs <- relatedPatches
      rhs <- relatedPatches if lhs != rhs
    } yield AbstractPatch.patchesAreRelated(lhs, rhs)).forall(identity))
    super.apply(relatedPatches)
  }
}

trait PatchRecorder[EventId] {
  def whenEventPertainedToByLastRecordingTookPlace: Option[Unbounded[Instant]]

  def allRecordingsAreCaptured: Boolean

  def recordPatchFromChange(eventId: EventId,
                            when: Unbounded[Instant],
                            patch: AbstractPatch): Unit

  def recordPatchFromMeasurement(eventId: EventId,
                                 when: Unbounded[Instant],
                                 patch: AbstractPatch): Unit

  def recordAnnihilation[Item: TypeTag](eventId: EventId,
                                        when: Instant,
                                        id: Any): Unit

  def noteThatThereAreNoFollowingRecordings(): Unit
}

trait PatchRecorderContracts[EventId] extends PatchRecorder[EventId] {
  require(whenEventPertainedToByLastRecordingTookPlace.isEmpty)
  require(!allRecordingsAreCaptured)

  abstract override def recordPatchFromChange(eventId: EventId,
                                              when: Unbounded[Instant],
                                              patch: AbstractPatch): Unit = {
    require(
      whenEventPertainedToByLastRecordingTookPlace.cata(some = when >= _,
                                                        none = true))
    require(!allRecordingsAreCaptured)
    val result = super.recordPatchFromChange(eventId, when, patch)
    require(whenEventPertainedToByLastRecordingTookPlace == Some(when))
    result
  }

  abstract override def recordPatchFromMeasurement(
      eventId: EventId,
      when: Unbounded[Instant],
      patch: AbstractPatch): Unit = {
    require(
      whenEventPertainedToByLastRecordingTookPlace.cata(some = when >= _,
                                                        none = true))
    require(!allRecordingsAreCaptured)
    val result = super.recordPatchFromMeasurement(eventId, when, patch)
    require(whenEventPertainedToByLastRecordingTookPlace == Some(when))
    result
  }

  abstract override def recordAnnihilation[Item: TypeTag](eventId: EventId,
                                                          when: Instant,
                                                          id: Any): Unit = {
    require(
      whenEventPertainedToByLastRecordingTookPlace
        .cata(some = Finite(when) >= _, none = true))
    require(!allRecordingsAreCaptured)
    val result = super.recordAnnihilation(eventId, when, id)
    require(whenEventPertainedToByLastRecordingTookPlace.contains(Finite(when)))
    result
  }

  abstract override def noteThatThereAreNoFollowingRecordings(): Unit = {
    require(!allRecordingsAreCaptured)
    val result = super.noteThatThereAreNoFollowingRecordings()
    require(allRecordingsAreCaptured)
    result
  }
}
