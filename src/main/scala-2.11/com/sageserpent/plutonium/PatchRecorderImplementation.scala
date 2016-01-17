package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded


/**
  * Created by Gerard on 10/01/2016.
  */
trait PatchRecorderImplementation extends PatchRecorder {
  self: BestPatchSelection =>
  override val whenEventPertainedToByLastRecordingTookPlace: Option[Unbounded[Instant]] = None
  override val allRecordingsAreCaptured: Boolean = true

  override def recordPatchFromChange(when: Unbounded[Instant], patch: Patch[Identified]): Unit = ???

  override def recordPatchFromMeasurement(when: Unbounded[Instant], patch: Patch[Identified]): Unit = ???

  override def recordAnnihilation(when: Instant, target: Any): Unit = ???

  override def noteThatThereAreNoFollowingRecordings(): Unit = ???
}
