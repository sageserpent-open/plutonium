package com.sageserpent.plutonium.javaApi
import java.time.Instant
import java.util.Optional

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.{Event, javaApi}

trait World[EventId] {
  def nextRevision: Int  // NOTE: this is the number of *revisions* that have all been made via 'revise'.

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
  def revise(events: Map[EventId, Option[Event]], asOf: Instant): Int

  // Alien intruder from planet Java!
  def revise(events: java.util.Map[EventId, Optional[Event]], asOf: Instant): Int

  def revise(eventId: EventId, event: Event, asOf: Instant): Int

  def annul(eventId: EventId, asOf: Instant): Int

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  // I can imagine queries being set to 'the beginning of time' and 'past the latest event'...
  // NOTE: precondition that 'nextRevision' <= 'this.nextRevision' - which implies that a scope makes a *snapshot* of the world when it is created - subsequent revisions to the world are disregarded.
  def scopeFor(when: Unbounded[Instant], nextRevision: Int): Scope

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
