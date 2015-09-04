package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.infrastructure.Unbounded
import com.sageserpent.plutonium.World.Revision


object World {
  type Revision = Int
  val initialRevision: Revision = 0 // NOTE: this is the revision defined when the world is first revised.
  // For now, this implies that if we want to book in events that have always been
  // known (think of reference data), this should be done by the subclass constructor
  // of 'World', and has no revision, nor any appearance on the version timeline.
}

trait World {
  type Scope <: com.sageserpent.plutonium.Scope


  def nextRevision: Revision

  val versionTimeline: collection.Seq[Instant] // NOTE: the next revision is the number of versions.

  // Can have duplicated instants associated with different events - more than one thing can happen at a given time.
  // Question: does the order of appearance of the events matter, then? - Hmmm - the answer is that they take effect in order
  // of instant key (obviously), using the order of appearance in 'events' as a tiebreaker.
  // Next question: what if two events belonging to different event groups collide in time? Hmmm - the answer is that whichever
  // event group was when originally recorded the later version of the world's timeline contributes the secondary event, regardless
  // of any subsequent revision to either event group.
  // Hmmm - could generalise this to specify a precedence enumeration - 'OrderUsingOriginalVersion', 'First', 'Last'.

  // NOTE: this increments 'currentRevision' if it succeeds, associating the new revision with 'revisionTime'.
  // NOTE: there is a precondition that 'asOf' must be greater than or equal 'versonTimeline.last'.
  // On success, the new revision defined by the recording is returned, which as a postcondition is one less than the updated value of 'nextRevision' at method exit.
  // NOTE: however, it doesn't have to succeed - the events may produce an inconsistency, or may cause collision of bitemporal ids for related types
  // - in which case an admissible failure exception is thrown.
  // NOTE: if an optional event value associated with an event id is 'None', that event is annulled in the world revised history. It may be
  // reinstated by a later revision, though.
  // Supplying an event id key for the first time to the world via this method defines a brand new event. Subsequent calls that reuse this event id
  // either correct the event or annul it.
  def revise[EventId](events: Map[EventId, Option[Event]], asOf: Instant): Revision

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  // I can imagine queries being set to 'the beginning of time' and 'past the latest event'...
  // NOTE: precondition that 'revision' <= 'this.nextRevision' - which implies that a scope makes a *snapshot* of the world when it is created - subsequent revisions to the world are disregarded.
  def scopeFor(when: Unbounded[Instant], nextRevision: World.Revision): Scope

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  // I can imagine queries being set to 'the beginning of time' and 'past the latest event'...
  // NOTE: again, the scope makes a *snapshot*, so the 'asOf' is interpreted wrt the state of the world at the point of creation of the scope.
  def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope
}