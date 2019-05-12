package com.sageserpent.plutonium

object BlobStorageOnH2 {
  val empty: BlobStorageOnH2 = ???
}

class BlobStorageOnH2 extends Timeline.BlobStorage {
  override implicit val timeOrdering: Ordering[ItemStateUpdateTime] = ???

  override def openRevision(): RevisionBuilder = ???

  override def timeSlice(when: ItemStateUpdateTime, inclusive: Boolean)
    : BlobStorage.Timeslice[ItemStateStorage.SnapshotBlob] = ???

  override def retainUpTo(when: ItemStateUpdateTime): Timeline.BlobStorage =
    ???
}
