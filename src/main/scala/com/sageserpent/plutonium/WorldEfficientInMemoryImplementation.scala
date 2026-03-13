package com.sageserpent.plutonium

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob
import com.sageserpent.plutonium.World.Revision

import java.time.Instant

class WorldEfficientInMemoryImplementation(
    var timelineStorage: Vector[(Instant, Timeline)]
) extends WorldImplementationCodeFactoring {

  def this() = this(Vector.empty)

  override def close(): Unit = {}

  override def revisionAsOfs: Array[Instant] =
    timelineStorage.map(_._1).toArray

  def revise_(
      events: collection.Map[_ <: EventId, Option[Event]],
      asOf: Instant
  ): Revision = {
    val resultCapturedBeforeMutation = nextRevision

    val newTimeline = timelinePriorTo(nextRevision)
      .getOrElse(Timeline.emptyTimeline)
      .revise(events)

    timelineStorage = timelineStorage :+ (asOf -> newTimeline)

    resultCapturedBeforeMutation
  }

  override def nextRevision: Revision = timelineStorage.size

  private def timelinePriorTo(nextRevision: Revision): Option[Timeline] =
    if (World.initialRevision < nextRevision)
      Some(timelineStorage(nextRevision - 1)._2)
    else None

  override def forkExperimentalWorld(scope: javaApi.Scope): World = {
    val timelines = timelineStorage
      .take(scope.nextRevision)
      .map { case (asOf, timeline) =>
        asOf -> timeline.retainUpTo(scope.when)
      }
    new WorldEfficientInMemoryImplementation(timelines)
  }

  override def scopeFor(
      when: Unbounded[Instant],
      nextRevision: Revision
  ): Scope =
    new ScopeBasedOnNextRevision(when, nextRevision) with ScopeUsingStorage

  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope =
    new ScopeBasedOnAsOf(when, asOf) with ScopeUsingStorage

  private def blobStoragePriorTo(
      nextRevision: Revision
  ): Option[Timeline.BlobStorage] =
    if (World.initialRevision < nextRevision)
      Some(timelineStorage(nextRevision - 1)._2.blobStorage)
    else None

  trait ScopeUsingStorage extends com.sageserpent.plutonium.Scope {
    lazy val itemCache: ItemCache = {
      val blobStorage = blobStoragePriorTo(nextRevision)
        .getOrElse(BlobStorageInMemory.empty[ItemStateUpdateTime, SnapshotBlob])
      ItemCacheUsingBlobStorage.itemCacheAt(when, blobStorage)
    }

    override def render[Item](bitemporal: Bitemporal[Item]): Stream[Item] =
      itemCache.render(bitemporal)

    override def numberOf[Item](bitemporal: Bitemporal[Item]): Revision =
      itemCache.numberOf(bitemporal)
  }
}
