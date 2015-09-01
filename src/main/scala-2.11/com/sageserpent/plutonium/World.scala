package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.infrastructure.Unbounded
import com.sageserpent.plutonium.World.Revision

import scala.collection.generic.Sorted





object World
{
  type Revision = Int
  val initialRevision: Revision = 0;
}

trait World{
  type Scope <: com.sageserpent.plutonium.Scope


  def currentRevision: Revision // TODO - need a value class with an invariant of being no less than the initial revision value here.
  // NOTE: this a revision of the entire world modelled by the 'World'.

  def versionTimeline: Sorted[Instant, _]  // The revision number is an index into this timeline.

  // Can have duplicated instants associated with different events - more than one thing can happen at a given time.
  // Question: does the order of appearance of the events matter, then? - Hmmm - the answer is that they take effect in order
  // of instant key (obviously), using the order of appearance in 'events' as a tiebreaker.
  // Next question: what if two events belonging to different event groups collide in time? Hmmm - the answer is that whichever
  // event group was when originally recorded the later version of the world's timeline contributes the secondary event, regardless
  // of any subsequent revision to either event group.
  // Hmmm - could generalise this to specify a precedence enumeration - 'OrderUsingOriginalVersion', 'First', 'Last'.

  // NOTE: this increments 'currentRevision' if it succeeds, associating the new revision with 'revisionTime'.
  // NOTE: there is a precondition that 'revisionTime' must be greater than or equal to the revision time of the current revision.
  // On success, the new revision defined by the recording is returned, which as a postcondition is the updated value of 'currentRevision' at method exit.
  // NOTE: however, it doesn't have to succeed - the events may produce an inconsistency, or may cause collision of bitemporal ids for related types
  // - in which case an admissible failure exception is thrown.
  // NOTE: if an optional event value associated with an event id is 'None', that event is annulled in the world revised history. It many be
  // reinstated by a later revision, though.
  // Supplying an event id key for the first time to the world via this method defines a brand new event. Subsequent calls that reuse this event id
  // either correct the event or annul it.
  def revise[EventId](events: Map[EventId, Option[Event]], revisionTime: Instant): Revision

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  // I can imagine queries being set to 'the beginning of time' and 'past the latest event'...
  // NOTE: precondition that 'revision' <= 'this.currentRevision'.
  def scopeFor(when: Unbounded[Instant], revision: World.Revision): Scope

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  // I can imagine queries being set to 'the beginning of time' and 'past the latest event'...
  def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope
}