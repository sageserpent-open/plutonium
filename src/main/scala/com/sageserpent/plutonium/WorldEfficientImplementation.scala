package com.sageserpent.plutonium
import java.time.Instant

import cats.Monad
import cats.implicits._
import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob
import com.sageserpent.plutonium.World.Revision

abstract class WorldEfficientImplementation[F[_]: Monad]
    extends WorldImplementationCodeFactoring {
  protected def blobStoragePriorTo(
      nextRevision: Revision): F[Option[Timeline.BlobStorage]]
  protected def timelinePriorTo(nextRevision: Revision): F[Option[Timeline]]

  protected def allTimelinesPriorTo(
      nextRevision: Revision): F[Vector[(Instant, Timeline)]]

  protected def consumeNewTimeline(newTimeline: F[Timeline],
                                   asOf: Instant): Unit

  protected def forkWorld(timelines: F[Vector[(Instant, Timeline)]]): World

  protected def itemCacheOf(itemCache: F[ItemCache]): ItemCache

  override def close(): Unit = {}

  protected def emptyTimeline(): Timeline

  def revise(events: Map[_ <: EventId, Option[Event]],
             asOf: Instant): Revision = {
    val resultCapturedBeforeMutation = nextRevision

    val computation: F[Timeline] = timelinePriorTo(nextRevision)
      .map(_.getOrElse(emptyTimeline()))
      .map(_.revise(events))

    consumeNewTimeline(computation, asOf)

    resultCapturedBeforeMutation
  }

  override def forkExperimentalWorld(scope: javaApi.Scope): World = {
    val computation: F[Vector[(Instant, Timeline)]] =
      allTimelinesPriorTo(scope.nextRevision)
        .map(_.map {
          case (asOf, timeline) => asOf -> timeline.retainUpTo(scope.when)
        })

    forkWorld(computation)
  }

  trait ScopeUsingStorage extends com.sageserpent.plutonium.Scope {
    lazy val itemCache: ItemCache = {
      val computation: F[ItemCache] = blobStoragePriorTo(nextRevision)
        .map(
          _.getOrElse(
            BlobStorageInMemory.empty[ItemStateUpdateTime, SnapshotBlob]))
        .map(blobStorage =>
          ItemCacheUsingBlobStorage.itemCacheAt(when, blobStorage))

      itemCacheOf(computation)
    }

    override def render[Item](bitemporal: Bitemporal[Item]): Stream[Item] =
      itemCache.render(bitemporal)

    override def numberOf[Item](bitemporal: Bitemporal[Item]): Revision =
      itemCache.numberOf(bitemporal)
  }

  override def scopeFor(when: Unbounded[Instant],
                        nextRevision: Revision): Scope =
    new ScopeBasedOnNextRevision(when, nextRevision) with ScopeUsingStorage

  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope =
    new ScopeBasedOnAsOf(when, asOf) with ScopeUsingStorage
}
