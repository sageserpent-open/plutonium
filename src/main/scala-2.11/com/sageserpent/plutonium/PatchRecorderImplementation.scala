package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{Finite, Unbounded}
import scala.reflect.runtime.universe._




/**
  * Created by Gerard on 10/01/2016.
  */
trait PatchRecorderImplementation extends PatchRecorder {
  self: BestPatchSelection with IdentifiedItemFactory =>
  override val whenEventPertainedToByLastRecordingTookPlace: Option[Unbounded[Instant]] = _whenEventPertainedToByLastRecordingTookPlace

  private var _whenEventPertainedToByLastRecordingTookPlace: Option[Unbounded[Instant]] = None

  override val allRecordingsAreCaptured: Boolean = false

  override def recordPatchFromChange(when: Unbounded[Instant], patch: AbstractPatch[Identified]): Unit = {
    _whenEventPertainedToByLastRecordingTookPlace = Some(when)
  }

  override def recordPatchFromMeasurement(when: Unbounded[Instant], patch: AbstractPatch[Identified]): Unit = {
    _whenEventPertainedToByLastRecordingTookPlace = Some(when)
  }

  override def recordAnnihilation[Raw <: Identified: TypeTag](when: Instant, id: Raw#Id): Unit = {
    _whenEventPertainedToByLastRecordingTookPlace = Some(Finite(when))
  }

  override def noteThatThereAreNoFollowingRecordings(): Unit = ???
}
