package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import scala.reflect.runtime.universe._




/**
  * Created by Gerard on 10/01/2016.
  */
trait PatchRecorderImplementation extends PatchRecorder {
  self: BestPatchSelection with IdentifiedItemFactory =>
  override val whenEventPertainedToByLastRecordingTookPlace: Option[Unbounded[Instant]] = None
  override val allRecordingsAreCaptured: Boolean = false

  override def recordPatchFromChange(when: Unbounded[Instant], patch: Patch[Identified]): Unit = ???

  override def recordPatchFromMeasurement(when: Unbounded[Instant], patch: Patch[Identified]): Unit = ???

  override def recordAnnihilation[Raw <: Identified: TypeTag](when: Instant, id: Raw#Id): Unit = ???

  override def noteThatThereAreNoFollowingRecordings(): Unit = ???
}
