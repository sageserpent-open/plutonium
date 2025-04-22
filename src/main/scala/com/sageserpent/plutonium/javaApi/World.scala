package com.sageserpent.plutonium.javaApi
import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.{Event, EventId, javaApi}

import java.time.Instant
import java.util.Optional

/** A world populated by <i>items</i> that are defined and changed in time by
  * [[Events]].<p> [[World]] instances are imperative, being updated via
  * [[revise]] - each update defines a new <i>revision</i> of the world.<p>A
  * given revision defines a timeline of events occurring in the world that
  * encompasses all possible times.<p>Usually, new revisions add in new events
  * that fill out the timeline towards the future.<p>However, it is both
  * possible and expected to amend the timeline by placing additional events
  * that are back-dated wrt the latest event to fill out missing detail.<p>It is
  * also possible to annul or amend events that were booked in a previous
  * revision to correct the timeline.<p>Therefore there are two notions of time:
  * the time of an event within a revision's timeline, and the time that the
  * revision itself is made, referred to as the {@code asOf}. The latter is
  * usually the wall-clock time, captured explicitly in some high-level
  * operation in client code, say a REST call for example.<p>So typical use
  * would add new events in a revision that are later than all the existing
  * events in the timeline being revised, but possibly earlier than the
  * {@code asOf}.<p>However, there is nothing stopping an event from being
  * booked in speculatively at some time in advance of the
  * {@code asOf}.<p>Regardless of whether that a revision's {@code asOf} regards
  * a previosly booked event as in the past or future, it can annul or amend
  * that event.<p>The items referred to by events can be queried for via a
  * [[Scope]]; the scope acts as a selector into the world, focussing on a
  * timeline at a specific revision and then slicing into the timeline at some
  * specific time: the items yielded have state that corresponds to how they
  * would appear taking into account all the timeline's events up to and
  * including the time. A scope thus provides bitemporal access to the items.
  * @note
  *   A [[World]] instance accumulates all of its revisions, so while there is
  *   the latest revision from the point of view of calling [[revise]] to make a
  *   new one, all of them are available to make a [[Scope]].
  */
trait World extends WorldConstants with AutoCloseable {

  /** @return
    *   The number of revisions that have all been made via [[revise]].
    * @note
    *   This implies that revision numbers are zero-relative.
    */
  def nextRevision: Int

  /** @return
    *   The {@code asOf} times when revisions were made, in ascending order.
    * @note
    *   <i>Adjacent</i> duplicates are permitted - this is taken to mean that
    *   successive revisions were booked in faster than the client system's time
    *   resolution, this might be business days for example.
    */
  def revisionAsOfs: Array[Instant]

  /** Define a new revision as the latest one. On success, a new revision is
    * defined and the value yielded by [[nextRevision]] is incremented. If an
    * exception is thrown, the world remains unchanged; no new revision is made.
    * @param events
    *   Events specified by event ids and either an associated event (to add a
    *   new event or amend a previously-booked one) or no event (to annul a
    *   previously booked one).
    * @param asOf
    *   The time of the <i>revision</i> itself - this may be later than, earlier
    *   than or the same as the time of the latest event in either the latest
    *   revision's timeline or any of {@code events}.
    * @note
    *   The {@code asOf} must be no earlier than the maximum yielded by
    *   [[revisionAsOfs]].
    * @note
    *   [[EventId]] serves as the supertype of all event ids; an id allows a
    *   previously booked event to be either amended or annulled in the new
    *   revision's timeline.
    * @note
    *   An event is amended by supplying a new definition value: this may
    *   involve moving the time of the event in the timeline, or may change the
    *   character of the event, say by referring to new or different items, or
    *   changing the mutation operations made on the items.
    * @note
    *   More than one event may take place at the same time in a given revision,
    *   either because of a new or amended event landing at the same place on
    *   the timeline as an existing event that has not been annulled or amended,
    *   or because {@code events} specifies coincidental events. However, such
    *   events have a fine-grained ordering in terms of any possible effects
    *   from one on another; any coincidental events in the <i>new revision's
    *   timeline</i> will be ordered firstly by the revision number they were
    *   originally booked in (including the new revision) and secondly by the
    *   order of appearance of the events in {@code events} as a tiebreaker.
    * @note
    *   There is no notion of item creation in the API - events simply refer to
    *   an item via an id and the world implementation will make sure that it
    *   either already exists because of some earlier event, or comes into being
    *   on the timeline at the time of the earliest event referring to it.
    *   Unless an annulment is booked in later in the timeline, the item is
    *   treated as existing forever. It is possible to annul a given item by id
    *   and then resurrect it later in the timeline by a following event; the
    *   resurrected item does not bare any relationship to the annulled one and
    *   may be of a different type. Several such <i>lifecycles</i> may be
    *   defined in the same revision timeline.
    * @note
    *   Annulling an event is idempotent; in fact it is OK to annul using an
    *   event id that has not yet been used to book in an event in a previous
    *   revision. This is consistent with how events don't have to worry about
    *   whether the referenced items exist earlier in the timeline or not.
    */
  def revise(
      events: java.util.Map[_ <: EventId, Optional[Event]],
      asOf: Instant
  ): Int

  def revise(eventId: EventId, event: Event, asOf: Instant): Int

  def annul(eventId: EventId, asOf: Instant): Int

  // This produces a 'read-only' scope - objects that it renders from
  // bitemporals will fail at runtime if an attempt is made to mutate them,
  // subject to what the proxies can enforce.
  // I can imagine queries being set to 'the beginning of time' and 'past the
  // latest event'...
  // NOTE: precondition that 'nextRevision' <= 'this.nextRevision' - which
  // implies that a scope makes a *snapshot* of the world when it is created -
  // subsequent revisions to the world are disregarded.
  def scopeFor(when: Unbounded[Instant], nextRevision: Int): Scope

  def scopeFor(when: Instant, nextRevision: Int): Scope

  // This produces a 'read-only' scope - objects that it renders from
  // bitemporals will fail at runtime if an attempt is made to mutate them,
  // subject to what the proxies can enforce.
  // I can imagine queries being set to 'the beginning of time' and 'past the
  // latest event'...
  // NOTE: again, the scope makes a *snapshot*, so the 'asOf' is interpreted wrt
  // the state of the world at the point of creation of the scope.
  def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope

  def scopeFor(when: Instant, asOf: Instant): Scope

  // This creates a fresh world instance anew whose revision history is the same
  // as the receiver world, with the important
  // differences that a) the revision history is truncated after the scope's
  // revision and b) that only events coming no
  // later than the scope's 'when' are included in each revision. Of course, the
  // experimental world can itself be revised
  // in just the same way as any other world, including the definition of events
  // beyond the defining scope's 'when'.
  def forkExperimentalWorld(scope: javaApi.Scope): World
}
