package com.sageserpent.plutonium

import java.time.Instant
import java.util.Optional

import com.sageserpent.americium.{Finite, NegativeInfinity, Unbounded}
import com.sageserpent.plutonium.World.Revision

import scala.collection.JavaConversions._

object World {
  type Revision = Int
  val initialRevision
    : Revision = 0 // NOTE: this is the revision defined when the world is first revised.
}

trait World extends javaApi.World {
  def nextRevision
    : Revision // NOTE: this is the number of *revisions* that have all been made via 'revise'.

  def revisionAsOfs
    : Array[Instant] // Adjacent duplicates are permitted - this is taken to mean that successive revisions were booked in faster than than the time resolution.

  def revise(events: Map[_ <: EventId, Option[Event]], asOf: Instant): Revision

  def revise(events: java.util.Map[_ <: EventId, Optional[Event]],
             asOf: Instant): Revision = {
    val sam: java.util.function.Function[Event, Option[Event]] = event =>
      Some(event): Option[Event]
    val eventsAsScalaImmutableMap = Map(
      events mapValues (_.map[Option[Event]](sam).orElse(None)) toSeq: _*)
    revise(eventsAsScalaImmutableMap, asOf)
  }

  def revise(eventId: EventId, event: Event, asOf: Instant): Revision =
    revise(Map(eventId -> Some(event)), asOf)

  def annul(eventId: EventId, asOf: Instant): Revision =
    revise(Map(eventId -> None), asOf)

  def scopeFor(when: Unbounded[Instant], nextRevision: World.Revision): Scope

  def scopeFor(when: Instant, nextRevision: Int): Scope =
    scopeFor(Finite(when), nextRevision)

  def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope

  def scopeFor(when: Instant, asOf: Instant): Scope =
    scopeFor(Finite(when), asOf)

  def forkExperimentalWorld(scope: javaApi.Scope): World
}

trait WorldContracts extends World {
  def checkInvariant: Unit = {
    assert(revisionAsOfs.size == nextRevision)
    assert(
      revisionAsOfs.isEmpty || (revisionAsOfs zip revisionAsOfs.tail forall {
        case (first, second) => !second.isBefore(first)
      }))
  }

  // NOTE: this increments 'nextRevision' if it succeeds, associating the new revision with 'asOf'.
  abstract override def revise(events: Map[_ <: EventId, Option[Event]],
                               asOf: Instant): Revision = {
    require(revisionAsOfs.isEmpty || !asOf.isBefore(revisionAsOfs.last))
    val revisionAsOfsBeforehand = revisionAsOfs
    val nextRevisionBeforehand  = nextRevision
    try {
      val result = super.revise(events, asOf)
      assert(revisionAsOfs sameElements (revisionAsOfsBeforehand :+ asOf))
      assert(result == nextRevisionBeforehand)
      assert(nextRevision == 1 + result)
      result
    } finally checkInvariant
  }

  // This produces a 'read-only' scope - objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  abstract override def scopeFor(when: Unbounded[Instant],
                                 nextRevision: Revision): Scope = {
    require(nextRevision <= this.nextRevision)
    val result = super.scopeFor(when, nextRevision)
    assert(result.nextRevision == nextRevision)
    assert(result.nextRevision == 0 && result.asOf == NegativeInfinity() ||
      result.nextRevision > revisionAsOfs
        .count(Finite(_) < result.asOf) && result.nextRevision <= revisionAsOfs
        .count(Finite(_) <= result.asOf))
    result
  }

  // This produces a 'read-only' scope - objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  abstract override def scopeFor(when: Unbounded[Instant],
                                 asOf: Instant): Scope = {
    val result = super.scopeFor(when, asOf)
    assert(result.asOf == Finite(asOf))
    assert(result.nextRevision == revisionAsOfs.count(Finite(_) <= result.asOf))
    result
  }

  abstract override def forkExperimentalWorld(scope: javaApi.Scope): World = {
    require(scope.nextRevision <= this.nextRevision)
    super.forkExperimentalWorld(scope)
  }
}
