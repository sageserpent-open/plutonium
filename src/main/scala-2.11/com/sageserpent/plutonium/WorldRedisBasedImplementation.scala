package com.sageserpent.plutonium
import java.time.Instant

import com.redis.RedisClient
import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.World.Revision
import com.sageserpent.plutonium.WorldImplementationCodeFactoring.AbstractEventData

/**
  * Created by Gerard on 27/05/2016.
  */
class WorldRedisBasedImplementation[EventId](redisClient: RedisClient, identityGuid: String) extends WorldImplementationCodeFactoring[EventId] {
  override def nextRevision: Revision = ???

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], nextRevision: Revision): Scope = ???

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope = ???

  // This creates a fresh world instance anew whose revision history is the same as the receiver world, with the important
  override def forkExperimentalWorld(scope: javaApi.Scope): World[EventId] = ???

  override def revisionAsOfs: Seq[Instant] = ???

  override protected def transactNewRevision(asOf: Instant, newEventDatums: Map[EventId, AbstractEventData])
                                            (buildAndValidateEventTimelineForProposedNewRevision: (Seq[AbstractEventData], Set[AbstractEventData]) => Unit): Unit = ???
}
