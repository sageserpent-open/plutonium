package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.infrastructure.{Finite, NegativeInfinity, Unbounded}
import com.sageserpent.plutonium.World.Revision

import scala.collection.mutable.MutableList

/**
 * Created by Gerard on 19/07/2015.
 */


class WorldReferenceImplementation extends World {
  type Scope = ScopeReferenceImplementation

  object ScopeReferenceImplementation {
    implicit val instantOrdering = new Ordering[Instant] {
      override def compare(lhs: Instant, rhs: Instant): Revision = lhs compareTo rhs
    }
  }

  class ScopeReferenceImplementation(val when: Unbounded[Instant], val asOf: Unbounded[Instant]) extends com.sageserpent.plutonium.Scope {
    // TODO: snapshot the state from the world on construction - the effects of further revisions should not be apparent.

    def this(when: Unbounded[Instant], nextRevision: Revision) = this(when, nextRevision match {
      case World.initialRevision => NegativeInfinity[Instant]
      case nextRevisionAfterAnActualRevision => Finite(versionTimeline(nextRevisionAfterAnActualRevision))
    })

    override val nextRevision: Revision = World.initialRevision/*(versionTimeline map (Finite(_: Instant))).search(asOf) match {
      case found @ Found(_) => 1 + found.foundIndex
      case notFound @ InsertionPoint(_) => notFound.insertionPoint
    }*/

    // NOTE: this should return proxies to raw values, rather than the raw values themselves. Depending on the kind of the scope (created by client using 'World', or implicitly in an event).
    override def render[Raw](bitemporal: Bitemporal[Raw]): Stream[Raw] = Stream.empty
  }

  private var _nextRevision = World.initialRevision

  override def nextRevision: Revision = _nextRevision

  override val versionTimeline: MutableList[Instant] = MutableList.empty

  def revise[EventId](events: Map[EventId, Option[Event]], asOf: Instant): Revision = {
    if (versionTimeline.nonEmpty && versionTimeline.last.isAfter(asOf)) throw new IllegalArgumentException(s"'asOf': ${asOf} should be no earlier than that of the last revision: ${versionTimeline.last}")

    // TODO: make exception safe - especially against the expected failures to apply events due to inconsistencies.

    versionTimeline += asOf
    val revision = nextRevision
    _nextRevision += 1
    revision
  }

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], nextRevision: Revision): Scope = new ScopeReferenceImplementation(NegativeInfinity[Instant], NegativeInfinity[Instant])//(when, nextRevision)

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope = new ScopeReferenceImplementation(NegativeInfinity[Instant], NegativeInfinity[Instant])//(when, Finite(asOf))
}
