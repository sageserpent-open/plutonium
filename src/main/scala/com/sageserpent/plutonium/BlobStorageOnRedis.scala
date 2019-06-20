package com.sageserpent.plutonium

import java.util.concurrent.Executor

import io.lettuce.core.RedisClient

object BlobStorageOnRedis {
  def empty(redisClient: RedisClient, executor: Executor): BlobStorageOnRedis =
    ???
}

class BlobStorageOnRedis(redisClient: RedisClient)
    extends Timeline.BlobStorage {
  override implicit val timeOrdering: Ordering[ItemStateUpdateTime] = ???

  override def openRevision(): RevisionBuilder = ???

  override def timeSlice(when: ItemStateUpdateTime, inclusive: Boolean)
    : BlobStorage.Timeslice[ItemStateStorage.SnapshotBlob] = ???

  override def retainUpTo(when: ItemStateUpdateTime)
    : BlobStorage[ItemStateUpdateTime, ItemStateStorage.SnapshotBlob] = ???
}
