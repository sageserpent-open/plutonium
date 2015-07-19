package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.infrastructure.Unbounded

import scala.collection.generic.Sorted

/**
 * Created by Gerard on 09/07/2015.
 */
object World
{
  type EventGroupId = Long
  def unique(): EventGroupId = ???
  type Revision = Long
  val initialRevision: Revision = 0L;
}

trait World{
  def currentRevision: World.Revision // TODO - need a value class with an invariant of being no less than the initial revision value here.
  // NOTE: this a revision of the entire world modelled by the 'World'.

  def versionTimeline: Sorted[Instant, _]  // The revision number is an index into this timeline.

  // Can use this to record a new event group, revise an existing one or withdraw an event by virtue of specifying no events.
  // Can have duplicated instant keys associated with different events - more than one thing can happen at a given time.
  // Question: does the order of appearance of the events matter, then? - Hmmm - the answer is that they take effect in order
  // of instant key (obviously), using the order of appearance in 'events' as a tiebreaker.
  // Next question: what if two events belonging to different event groups collide in time? Hmmm - the answer is that whichever
  // event group was when originally recorded the later version of the world's timeline contributes the secondary event, regardless
  // of any subsequent revision to either event group.
  // Hmmm - could generalise this to specify a precedence enumeration - 'OrderUsingOriginalVersion', 'First', 'Last'.

  // NOTE: this increments 'currentRevision' if it succeeds.
  // NOTE: however, it doesn't always succeed - the events may produce an inconsistency, or may cause collision of bitemporal ids for related types
  // - in which case an admissible failure exception is thrown.
  // NOTE: if an instant is not given, the event is taken to be 'at the beginning of time' - this is a way of introducing
  // timeless events, although it permits following events to modify the outcome, which may be quite handy. There is no notion
  // of an event occurring 'at the end of time', contrast this with the query methods below...
  def recordEvents(eventGroupId: World.EventGroupId, events: Iterable[(Option[Instant], Event)]): Unit = {

  }
  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  // I can imagine queries being set to 'the beginning of time' and 'past the latest event'...
  def scopeFor(when: Unbounded[Instant], revision: World.Revision): Scope = ???

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  // I can imagine queries being set to 'the beginning of time' and 'past the latest event'...
  def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope = ???
}