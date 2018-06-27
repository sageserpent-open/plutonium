package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.World.Revision

import scala.collection.mutable.MutableList

class WorldEfficientInMemoryImplementation(
    var timelineStorage: Array[(Instant, Timeline)],
    var numberOfTimelines: Revision)
    extends WorldImplementationCodeFactoring {
  def this() = this(Array.ofDim[(Instant, Timeline)](4), World.initialRevision)

  override def revisionAsOfs: Array[Instant] =
    timelineStorage.slice(0, numberOfTimelines).map(_._1)

  override def nextRevision: Revision = numberOfTimelines

  override def revise(events: Map[_ <: EventId, Option[Event]],
                      asOf: Instant): Revision = {
    val resultCapturedBeforeMutation = nextRevision

    val baseTimeline: Timeline =
      if (World.initialRevision < nextRevision)
        timelineStorage(nextRevision - 1)._2
      else emptyTimeline()

    val newTimeline = baseTimeline.revise(events)

    if (nextRevision == timelineStorage.length) {
      val sourceOfCopy = timelineStorage
      timelineStorage = Array.ofDim(2 * timelineStorage.length)
      sourceOfCopy.copyToArray(timelineStorage)
    }

    timelineStorage(nextRevision) = asOf -> newTimeline

    numberOfTimelines += 1

    resultCapturedBeforeMutation
  }

  override def forkExperimentalWorld(scope: javaApi.Scope): World = {
    val relevantTimelines = timelineStorage.take(scope.nextRevision)

    val relevantTimelinesEachWithHistorySharedWithThis = relevantTimelines map {
      case (asOf, timeline) => asOf -> timeline.retainUpTo(scope.when)
    }

    new WorldEfficientInMemoryImplementation(
      relevantTimelinesEachWithHistorySharedWithThis,
      scope.nextRevision)
  }

  trait ScopeUsingStorage extends com.sageserpent.plutonium.Scope {
    lazy val itemCache: ItemCache = {
      val timeline = if (nextRevision > World.initialRevision) {
        timelineStorage(nextRevision - 1)._2
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
