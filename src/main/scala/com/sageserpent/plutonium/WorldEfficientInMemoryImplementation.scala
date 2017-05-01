package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.World.Revision
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.IdentifiedItemsScope

import scala.collection.mutable.MutableList
import scala.reflect.runtime.universe.{TypeTag, typeTag}

class WorldEfficientInMemoryImplementation[EventId]
    extends WorldImplementationCodeFactoring[EventId] {
  override def revisionAsOfs: Array[Instant] = timelines.map(_._1).toArray

  override def nextRevision: Revision = timelines.size

  override def revise(events: Map[EventId, Option[Event]],
                      asOf: Instant): Revision = {
    // TODO: sort out this noddy implementation - no exception safety etc...
    val resultCapturedBeforeMutation = nextRevision

    val baseTimeline =
      if (World.initialRevision == nextRevision) emptyTimeline
      else timelines.last._2

    val (newTimeline, itemStateSnapshotBookings) = baseTimeline.revise(events)

    val builder = itemStateSnapshotStorage.openRevision()

    for ((id, when, snapshot) <- itemStateSnapshotBookings) {
      builder.recordSnapshot(id, when, snapshot)
    }

    timelines += (asOf -> newTimeline)

    itemStateSnapshotStorage = builder.build()

    resultCapturedBeforeMutation
  }

  override def scopeFor(when: Unbounded[Instant],
                        nextRevision: Revision): Scope =
    new ScopeBasedOnNextRevision(when, nextRevision) with ScopeUsingStorage {}

  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope =
    new ScopeBasedOnAsOf(when, asOf) with ScopeUsingStorage

  override def forkExperimentalWorld(scope: javaApi.Scope): World[EventId] =
    ??? // TODO - but much later....

  // TODO - consider use of mutable state object instead of having separate bits and pieces.
  private val timelines: MutableList[(Instant, Timeline)] = MutableList.empty

  // These can be in any order, as they are just fed to a builder.
  type ItemStateSnapshotBookings[Item <: Identified] =
    Seq[(Item#Id, Instant, ItemStateSnapshot)]

  // No notion of what revision a timeline is on, nor the 'asOf' - that is for enclosing 'World' to handle.
  trait Timeline {
    def revise(events: Map[EventId, Option[Event]])
      : (Timeline, ItemStateSnapshotBookings[_ <: Identified])

    def retainUpTo(when: Unbounded[Instant]): Timeline
  }

  object emptyTimeline extends Timeline {
    override def revise(events: Map[EventId, Option[Event]])
      : (Timeline, ItemStateSnapshotBookings[_ <: Identified]) = ???

    override def retainUpTo(when: Unbounded[Instant]): Timeline = this
  }
}
