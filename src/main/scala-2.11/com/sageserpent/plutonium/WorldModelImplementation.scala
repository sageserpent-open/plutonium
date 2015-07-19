package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.infrastructure.Unbounded
import com.sageserpent.plutonium.World.{EventGroupId, Revision}

import scala.collection.generic.Sorted

/**
 * Created by Gerard on 19/07/2015.
 */
class WorldModelImplementation extends World {
  override def currentRevision: Revision = ???

  override def versionTimeline: Sorted[Instant, _] = ???

  // NOTE: this increments 'currentRevision' if it succeeds.
  override def recordEvents(eventGroupId: EventGroupId, events: Iterable[(Option[Instant], Event)]): Unit = ???

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], revision: Revision): Scope = ???

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope = ???
}
