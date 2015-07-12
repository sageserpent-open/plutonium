package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.infrastructure.Unbounded
import com.sageserpent.plutonium.World.{EventGroupId, Revision}

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
  def currentRevision: Revision // TODO - need a value class with an invariant of being no less than the initial revision value here.
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
  // NOTE: however, it doesn't always succeed - the events may produce an inconsistency, or may cause collision of bitemoral ids for related types.
  def recordEvents(eventGroupId: EventGroupId, events: Iterable[(Unbounded[Instant], Event)]): Unit = {

  }

  def scopeFor(when: Unbounded[Instant], revision: Revision): Scope = ???

  def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope = ???
}