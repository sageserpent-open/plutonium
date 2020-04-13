package com.sageserpent.plutonium

import java.time.Instant

import cats.Id
import com.sageserpent.plutonium.World.Revision

class WorldEfficientInMemoryImplementation(
    var timelineStorage: Vector[(Instant, Timeline)])
    extends WorldEfficientImplementation[Id] {
  def this() =
    this(Vector.empty)

  protected def allTimelinesPriorTo(
      nextRevision: World.Revision): Id[Vector[(Instant, Timeline)]] =
    timelineStorage.take(nextRevision)

  protected def consumeNewTimeline(newTimeline: Id[Timeline],
                                   asOf: Instant): Unit = {
    timelineStorage = timelineStorage :+ (asOf -> newTimeline)
  }

  protected def forkWorld(timelines: Id[Vector[(Instant, Timeline)]]): World =
    new WorldEfficientInMemoryImplementation(timelines)

  protected def timelinePriorTo(nextRevision: Revision): Id[Option[Timeline]] =
    if (World.initialRevision < nextRevision)
      Some(timelineStorage(nextRevision - 1)._2)
    else None

  protected def blobStoragePriorTo(
      nextRevision: Revision): Id[Option[Timeline.BlobStorage]] =
    if (World.initialRevision < nextRevision)
      Some(timelineStorage(nextRevision - 1)._2.blobStorage)
    else None

  override def revisionAsOfs: Array[Instant] =
    timelineStorage.map(_._1).toArray

  override def nextRevision: Revision = timelineStorage.size

  override protected def itemCacheOf(itemCache: Id[ItemCache]): ItemCache =
    itemCache

  override protected def emptyTimeline(): Timeline = Timeline.emptyTimeline
}
