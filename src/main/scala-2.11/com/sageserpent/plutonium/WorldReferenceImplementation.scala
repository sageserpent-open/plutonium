package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.infrastructure.{Finite, Unbounded}
import com.sageserpent.plutonium.World.Revision

import scala.collection.immutable.TreeBag

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

  override def nextRevision: Revision = ???

  override def versionTimeline: TreeBag[Instant] = ???

  // NOTE: this increments 'currentRevision' if it succeeds.
  def revise[EventId](events: Map[EventId, Option[Event]], revisionTime: Instant): Revision = ???

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], nextRevision: Revision): Scope = new ScopeReferenceImplementation(Finite(Instant.MIN), Instant.MIN)

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope = new ScopeReferenceImplementation(Finite(Instant.MIN), Instant.MIN)
}
