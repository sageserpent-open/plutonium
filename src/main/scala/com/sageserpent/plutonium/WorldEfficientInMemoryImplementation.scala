package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.World.Revision

import scala.collection.mutable.MutableList
import scala.reflect.runtime.universe.{Super => _, This => _, _}

class WorldEfficientInMemoryImplementation[EventId]
    extends WorldImplementationCodeFactoring[EventId] {
  override def revisionAsOfs: Array[Instant] = timelines.map(_._1).toArray

  override def nextRevision: Revision = timelines.size

  override def revise(events: Map[EventId, Option[Event]],
                      asOf: Instant): Revision = {
    val resultCapturedBeforeMutation = nextRevision

    val baseTimeline
      : Timeline[EventId] = timelines.lastOption map (_._2) getOrElse emptyTimeline()

    val newTimeline = baseTimeline.revise(events)

    timelines += (asOf -> newTimeline)

    resultCapturedBeforeMutation
  }

  override def forkExperimentalWorld(scope: javaApi.Scope): World[EventId] =
    ??? // TODO - but much later....

  private val timelines: MutableList[(Instant, Timeline[EventId])] =
    MutableList.empty

  trait ScopeUsingStorage extends com.sageserpent.plutonium.Scope {
    private def itemCache(): ItemCache = {
      val timeline = timelines.get(nextRevision) map (_._2) getOrElse emptyTimeline()

      timeline.itemCacheAt(when)
    }

    override def render[Item](bitemporal: Bitemporal[Item]): Stream[Item] =
      itemCache().render(bitemporal)

    override def numberOf[Item](bitemporal: Bitemporal[Item]): Revision =
      itemCache().numberOf(bitemporal)
  }

  override def scopeFor(when: Unbounded[Instant],
                        nextRevision: Revision): Scope =
    new ScopeBasedOnNextRevision(when, nextRevision) with ScopeUsingStorage

  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope =
    new ScopeBasedOnAsOf(when, asOf) with ScopeUsingStorage
}
