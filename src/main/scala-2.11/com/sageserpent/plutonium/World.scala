package com.sageserpent.plutonium

import java.time.Instant
import java.util.Optional

import com.sageserpent.americium.{Finite, NegativeInfinity, Unbounded}
import com.sageserpent.plutonium.World.Revision


object  World {
  type Revision = Int
  val initialRevision: Revision = 0 // NOTE: this is the revision defined when the world is first revised.
  // For now, this implies that if we want to book in events that have always been
  // known (think of reference data), this should be done by the subclass constructor
  // of 'World', and has no revision, nor any appearance on the version timeline.
}

trait World[EventId] {
  def nextRevision: Revision  // NOTE: this is the number of *revisions* that have all been made via 'revise'.

  def revisionAsOfs: collection.Seq[Instant]  // Adjacent duplicates are permitted - this is taken to mean that successive revisions were booked in faster than than the time resolution.

  // Can have duplicated instants associated with different events - more than one thing can happen at a given time.
  // Question: does the order of appearance of the events matter, then? - Hmmm - the answer is that they take effect in order
  // of their 'when' value (obviously), using the order of appearance in 'events' as a tiebreaker if they were contributed
  // as part of the same shared revision.
  // Next question: what if several events given in different revisions coincide in time?
  // Hmmm - the answer is that the order of the revisions contributing the events, as seen from the point of view of some scope
  // determines the order of the coincident events - those from earlier revisions take effect before those from later revisions.

  // NOTE: this increments 'nextRevision' if it succeeds, associating the new revision with 'asOf'.
  // NOTE: there is a precondition that 'asOf' must be greater than or equal 'versonTimeline.last'.
  // On success, the new revision defined by the recording is returned, which as a postcondition is one less than the updated value of 'nextRevision' at method exit.
  // NOTE: however, it doesn't have to succeed - the events may produce an inconsistency, or may cause collision of bitemporal ids for related types
  // - in which case a precondition or an admissible postcondition failure exception is thrown.
  // NOTE: if an optional event value associated with an event id is 'None', that event is annulled in the world revised history. It may be
  // reinstated by a later revision, though.
  // NOTE: supplying an event id key for the first time to the world via this method defines a brand new event. Subsequent calls that reuse this event id
  // either correct the event or annul it.
  // NOTE: an event id key may be used to annul an event that has *not* been defined in a previous revision - there is no precondition on this. The idea
  // is to make it easy for clients to do annulments en-bloc without querying to see what events are in force in the world's current state. Furthermore,
  // the API issues no constraints on when to define an event id key for the first time and when to use it for correction, so why not treat the annulment
  // case the same way?
  def revise(events: Map[EventId, Option[Event]], asOf: Instant): Revision

  // Alien intruder from planet Java!
  def revise(events: java.util.Map[EventId, Optional[Event]], asOf: Instant): Revision

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  // I can imagine queries being set to 'the beginning of time' and 'past the latest event'...
  // NOTE: precondition that 'nextRevision' <= 'this.nextRevision' - which implies that a scope makes a *snapshot* of the world when it is created - subsequent revisions to the world are disregarded.
  def scopeFor(when: Unbounded[Instant], nextRevision: World.Revision): Scope

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  // I can imagine queries being set to 'the beginning of time' and 'past the latest event'...
  // NOTE: again, the scope makes a *snapshot*, so the 'asOf' is interpreted wrt the state of the world at the point of creation of the scope.
  def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope

  // This creates a fresh world instance anew whose revision history is the same as the receiver world, with the important
  // differences that a) the revision history is truncated after the scope's revision and b) that only events coming no
  // later than the scope's 'when' are included in each revision. Of course, the experimental world can itself be revised
  // in just the same way as any other world, including the definition of events beyond the defining scope's 'when'.
  def forkExperimentalWorld(scope: javaApi.Scope): World[EventId]
}

trait WorldContracts[EventId] extends World[EventId] {
  def checkInvariant: Unit = {
    require(revisionAsOfs.size == nextRevision)
    require(revisionAsOfs.isEmpty || (revisionAsOfs zip revisionAsOfs.tail forall {case (first, second) => !second.isBefore(first)}))
  }

  // NOTE: this increments 'nextRevision' if it succeeds, associating the new revision with 'asOf'.
  abstract override def revise(events: Map[EventId, Option[Event]], asOf: Instant): Revision = {
    require(revisionAsOfs.isEmpty || !asOf.isBefore(revisionAsOfs.last))
    val revisionAsOfsBeforehand = revisionAsOfs
    val nextRevisionBeforehand = nextRevision
    try {
      val result = super.revise(events, asOf)
      require(revisionAsOfs == revisionAsOfsBeforehand :+ asOf)
      require(result == nextRevisionBeforehand)
      require(nextRevision == 1 + result)
      result
    } finally checkInvariant
  }

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  abstract override def scopeFor(when: Unbounded[Instant], nextRevision: Revision): Scope = {
    require(nextRevision <= this.nextRevision)
    val result = super.scopeFor(when, nextRevision)
    require(result.nextRevision == nextRevision)
    require(result.nextRevision == 0 && result.asOf == NegativeInfinity() ||
      result.nextRevision > revisionAsOfs.count(Finite(_) < result.asOf) && result.nextRevision <= revisionAsOfs.count(Finite(_) <= result.asOf))
    result
  }

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  abstract override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope = {
    val result = super.scopeFor(when, asOf)
    require(result.asOf == Finite(asOf))
    require(result.nextRevision == revisionAsOfs.count(Finite(_) <= result.asOf))
    result
  }
}