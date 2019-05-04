package com.sageserpent.plutonium

import java.time.Instant

import cats.Id
import com.sageserpent.plutonium.World.Revision

class WorldEfficientInMemoryImplementation(
    var timelineStorage: Array[(Instant, Timeline)],
    var numberOfTimelines: Int)
    extends WorldEfficientImplementation[Id] {
  def this() =
    this(Array.empty[(Instant, Timeline)], World.initialRevision)

  protected def allTimelinesPriorTo(
      nextRevision: World.Revision): Id[Array[(Instant, Timeline)]] =
    timelineStorage.take(nextRevision)

  protected def consumeNewTimeline(newTimeline: Id[Timeline],
                                   asOf: Instant): Unit = {
    if (nextRevision == timelineStorage.length) {
      val sourceOfCopy = timelineStorage
      timelineStorage = Array.ofDim(4 max 2 * timelineStorage.length)
      sourceOfCopy.copyToArray(timelineStorage)
    }

    timelineStorage(nextRevision) = asOf -> newTimeline

    numberOfTimelines += 1
  }

  protected def forkWorld(timelines: Id[Array[(Instant, Timeline)]],
                          numberOfTimelines: Int): World =
    new WorldEfficientInMemoryImplementation(timelines, numberOfTimelines)

  protected def timelinePriorTo(nextRevision: Revision): Id[Option[Timeline]] =
    if (World.initialRevision < nextRevision)
      Some(timelineStorage(nextRevision - 1)._2)
    else None

  override def revisionAsOfs: Array[Instant] =
    timelineStorage.slice(0, numberOfTimelines).map(_._1)

  override def nextRevision: Revision = numberOfTimelines

  override protected def itemCacheOf(itemCache: Id[ItemCache]): ItemCache =
    itemCache
}
