package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded

// NASTY HACK: This all reeks of coupled hierarchies - 'Event' versus 'SerializableEvent'. What to do?
sealed abstract class SerializableEvent {
  def recordOnTo(patchRecorder: PatchRecorder): Unit
}

case class SerializableChange(when: Unbounded[Instant], patches: Seq[AbstractPatch]) extends SerializableEvent {
  override def recordOnTo(patchRecorder: PatchRecorder): Unit = for (patch <- patches) {
    patchRecorder.recordPatchFromChange(when, patch)
  }
}

case class SerializableMeasurement(when: Unbounded[Instant], patches: Seq[AbstractPatch]) extends SerializableEvent {
  override def recordOnTo(patchRecorder: PatchRecorder): Unit = for (patch <- patches) {
    patchRecorder.recordPatchFromMeasurement(when, patch)
  }
}

case class SerializableAnnihilation(annihilation: Annihilation[_ <: Identified]) extends SerializableEvent {
  override def recordOnTo(patchRecorder: PatchRecorder): Unit = annihilation match {
    case workaroundForUseOfExistentialTypeInAnnihilation@Annihilation(when, id) =>
      implicit val typeTag = workaroundForUseOfExistentialTypeInAnnihilation.capturedTypeTag
      patchRecorder.recordAnnihilation(when, id)
  }
}