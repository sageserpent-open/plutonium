package com.sageserpent.plutonium
import java.time.Instant

import cats.Monad
import cats.implicits._
import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.World.Revision

abstract class WorldEfficientImplementation[F[_]: Monad]
    extends WorldImplementationCodeFactoring {
  // TODO - one subclass prefers 'Array', other 'Vector'. Sort this out...

  protected def timelinePriorTo(nextRevision: Revision): F[Option[Timeline]]

  protected def allTimelinesPriorTo(
      nextRevision: Revision): F[Array[(Instant, Timeline)]]

  protected def consumeNewTimeline(newTimeline: F[Timeline],
                                   asOf: Instant): Unit

  protected def forkWorld(timelines: F[Array[(Instant, Timeline)]],
                          numberOfTimelines: Int): World

  protected def itemCacheOf(itemCache: F[ItemCache]): ItemCache

  override def close(): Unit = {}

  def revise(events: Map[_ <: EventId, Option[Event]],
             asOf: Instant): Revision = {
    val resultCapturedBeforeMutation = nextRevision

    val computation: F[Timeline] = timelinePriorTo(nextRevision)
      .map(_.getOrElse(Timeline.emptyTimeline))
      .map(_.revise(events))

    consumeNewTimeline(computation, asOf)

    resultCapturedBeforeMutation
  }

  override def forkExperimentalWorld(scope: javaApi.Scope): World = {
    val computation: F[Array[(Instant, Timeline)]] =
      allTimelinesPriorTo(scope.nextRevision)
        .map(_.map {
          case (asOf, timeline) => asOf -> timeline.retainUpTo(scope.when)
        })

    forkWorld(computation, scope.nextRevision)
  }

  trait ScopeUsingStorage extends com.sageserpent.plutonium.Scope {
    lazy val itemCache: ItemCache = {
      val computation: F[ItemCache] = timelinePriorTo(nextRevision)
        .map(_.getOrElse(Timeline.emptyTimeline))
        .map(_.itemCacheAt(when))

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
