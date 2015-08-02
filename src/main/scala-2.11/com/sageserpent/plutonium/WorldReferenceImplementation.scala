package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.infrastructure.{Finite, Unbounded}
import com.sageserpent.plutonium.World.Revision

import scala.collection.generic.Sorted

/**
 * Created by Gerard on 19/07/2015.
 */
class WorldReferenceImplementation extends World {
  class ScopeReferenceImplementation extends Scope{
    override val when: Unbounded[Instant] = Finite(Instant.MIN)
    override val asOf: Instant = Instant.MIN
    override val revision: Long = 0
  }

  override def currentRevision: Revision = ???

  override def versionTimeline: Sorted[Instant, _] = ???

  // NOTE: this increments 'currentRevision' if it succeeds.
  def revise[EventId](events: Map[EventId, Option[Event]], revisionTime: Instant): Revision = ???

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], revision: Revision): Scope = new ScopeReferenceImplementation

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope = new ScopeReferenceImplementation
}
