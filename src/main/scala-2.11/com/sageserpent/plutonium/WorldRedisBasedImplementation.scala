package com.sageserpent.plutonium
import java.time.Instant

import com.redis.RedisClient
import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.World.Revision

/**
  * Created by Gerard on 27/05/2016.
  */
class WorldRedisBasedImplementation[EventId](redisClient: RedisClient, identityGuid: String) extends WorldImplementationCodeFactoring[EventId] {
  override def nextRevision: Revision = ???

  // NOTE: this increments 'nextRevision' if it succeeds, associating the new revision with 'revisionTime'.
  override def revise(events: Map[EventId, Option[Event]], asOf: Instant): Revision = ???

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], nextRevision: Revision): Scope = ???

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope = ???

  // This creates a fresh world instance anew whose revision history is the same as the receiver world, with the important
  override def forkExperimentalWorld(scope: javaApi.Scope): World[EventId] = ???

  override def revisionAsOfs: Seq[Instant] = ???
}
