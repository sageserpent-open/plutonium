package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.World.Revision

import scala.collection.mutable.MutableList

class WorldEfficientInMemoryImplementation(
    timelines: MutableList[(Instant, Timeline)])
    extends WorldImplementationCodeFactoring {
  def this() = this(MutableList.empty[(Instant, Timeline)])

  override def revisionAsOfs: Array[Instant] = timelines.map(_._1).toArray

  override def nextRevision: Revision = timelines.size

  override def revise(events: Map[_ <: EventId, Option[Event]],
                      asOf: Instant): Revision = {
    val resultCapturedBeforeMutation = nextRevision

    val baseTimeline
      : Timeline = timelines.lastOption map (_._2) getOrElse emptyTimeline()

    val newTimeline = baseTimeline.revise(events)

    timelines += (asOf -> newTimeline)

    resultCapturedBeforeMutation
  }

  override def forkExperimentalWorld(scope: javaApi.Scope): World = {
    val relevantTimelines = timelines.take(scope.nextRevision)

    val relevantTimelinesEachWithHistorySharedWithThis = relevantTimelines map {
      case (asOf, timeline) => asOf -> timeline.retainUpTo(scope.when)
    }

    new WorldEfficientInMemoryImplementation(
      relevantTimelinesEachWithHistorySharedWithThis)
  }

  trait ScopeUsingStorage extends com.sageserpent.plutonium.Scope {
    lazy val itemCache: ItemCache = {
      val timeline = if (nextRevision > World.initialRevision) {
        timelines(nextRevision - 1)._2
      } else emptyTimeline()

      timeline.itemCacheAt(when)
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
