package com.sageserpent.plutonium

import scalikejdbc.ConnectionPool

object BlobStorageOnH2 {
  def empty(connectionPool: ConnectionPool): BlobStorageOnH2 = ???
}

class BlobStorageOnH2(connectionPool: ConnectionPool)
    extends Timeline.BlobStorage {
  override implicit val timeOrdering: Ordering[ItemStateUpdateTime] = ???

  override def openRevision(): RevisionBuilder = ???

  override def timeSlice(when: ItemStateUpdateTime, inclusive: Boolean)
    : BlobStorage.Timeslice[ItemStateStorage.SnapshotBlob] = ???

  override def retainUpTo(when: ItemStateUpdateTime): Timeline.BlobStorage =
    ???
}
