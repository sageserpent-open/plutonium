package com.sageserpent.plutonium

class BlobStorageOnH2 extends Timeline.BlobStorage {
  override implicit val timeOrdering: Ordering[ItemStateUpdateTime] = ???

  override def openRevision(): RevisionBuilder = ???

  override def timeSlice(when: ItemStateUpdateTime, inclusive: Boolean)
    : BlobStorage.Timeslice[ItemStateStorage.SnapshotBlob] = ???

  override def retainUpTo(when: ItemStateUpdateTime): Timeline.BlobStorage =
    ???
}
