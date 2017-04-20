package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.World.Revision

/**
  * Created by gerardMurphy on 20/04/2017.
  */
class WorldEfficientInMemoryImplementation[EventId]
    extends WorldImplementationCodeFactoring[EventId] {
  override def nextRevision: Revision = ???

  override def revisionAsOfs: Array[Instant] = ???

  override def revise(events: Map[EventId, Option[Event]],
                      asOf: Instant): Revision = ???

  override def scopeFor(when: Unbounded[Instant],
                        nextRevision: Revision): Scope = ???

  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope = ???

  override def forkExperimentalWorld(scope: javaApi.Scope): World[EventId] =
    ???
}
