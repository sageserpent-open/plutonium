package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.infrastructure.{Finite, Unbounded}
import com.sageserpent.plutonium.World.Revision

import scala.collection.mutable.MutableList

/**
 * Created by Gerard on 19/07/2015.
 */


class WorldReferenceImplementation extends World {
  type Scope = ScopeReferenceImplementation

  class ScopeReferenceImplementation(val when: Unbounded[Instant], val asOf: Instant) extends com.sageserpent.plutonium.Scope {
    override val nextRevision: Revision = 0

    // NOTE: this should return proxies to raw values, rather than the raw values themselves. Depending on the kind of the scope (created by client using 'World', or implicitly in an event).
    override def render[Raw](bitemporal: Bitemporal[Raw]): Stream[Raw] = Stream.empty
  }

  private var _nextRevision = World.initialRevision

  override def nextRevision: Revision = _nextRevision

  override val versionTimeline: MutableList[Instant] = MutableList.empty

  def revise[EventId](events: Map[EventId, Option[Event]], asOf: Instant): Revision = {
    if (versionTimeline.nonEmpty && versionTimeline.last.isAfter(asOf)) throw new IllegalArgumentException(s"'asOf': ${asOf} should be no earlier than that of the last revision: ${versionTimeline.last}")
    versionTimeline += asOf
    val revision = nextRevision
    _nextRevision += 1
    revision
  }

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], nextRevision: Revision): Scope = new ScopeReferenceImplementation(Finite(Instant.MIN), Instant.MIN)

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope = new ScopeReferenceImplementation(Finite(Instant.MIN), Instant.MIN)
}
