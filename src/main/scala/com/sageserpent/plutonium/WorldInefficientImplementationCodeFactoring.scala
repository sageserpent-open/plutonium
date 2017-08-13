package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.World.Revision

abstract class WorldInefficientImplementationCodeFactoring[EventId]
    extends WorldImplementationCodeFactoring[EventId] {

  import WorldImplementationCodeFactoring._

  trait SelfPopulatedScope extends ScopeImplementation {
    val identifiedItemsScope = {
      new IdentifiedItemsScope(when, nextRevision, eventTimeline(nextRevision))
    }
  }

  override def scopeFor(when: Unbounded[Instant],
                        nextRevision: Revision): Scope =
    new ScopeBasedOnNextRevision(when, nextRevision) with SelfPopulatedScope {}

  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope =
    new ScopeBasedOnAsOf(when, asOf) with SelfPopulatedScope

  protected def eventTimeline(nextRevision: Revision): Seq[Event]

  def revise(events: Map[EventId, Option[Event]], asOf: Instant): Revision = {
    def newEventDatumsFor(nextRevisionPriorToUpdate: Revision)
      : Map[EventId, AbstractEventData] = {
      events.zipWithIndex map {
        case ((eventId, event), tiebreakerIndex) =>
          eventId -> (event match {
            case Some(event) =>
              EventData(event, nextRevisionPriorToUpdate, tiebreakerIndex)
            case None => AnnulledEventData(nextRevisionPriorToUpdate)
          })
      }
    }

    def buildAndValidateEventTimelineForProposedNewRevision(
        newEventDatums: Map[EventId, AbstractEventData],
        nextRevisionPriorToUpdate: Revision,
        pertinentEventDatumsExcludingTheNewRevision: Seq[AbstractEventData])
      : Unit = {
      val eventTimelineIncludingNewRevision = eventTimelineFrom(
        pertinentEventDatumsExcludingTheNewRevision union newEventDatums.values.toStream)

      val nextRevisionAfterTransactionIsCompleted = 1 + nextRevisionPriorToUpdate

      // TODO - harvest snapshots here to pass to some storage, which is
      // presumably abstracted over the in-memory and Redis implementations....

      // This does a check for consistency of the world's history as per this new revision as part of construction.
      // We then throw away the resulting history if successful, the idea being for now to rebuild it as part of
      // constructing a scope to apply queries on.
      new IdentifiedItemsScope(PositiveInfinity[Instant],
                               nextRevisionAfterTransactionIsCompleted,
                               eventTimelineIncludingNewRevision)
    }

    transactNewRevision(asOf,
                        newEventDatumsFor,
                        buildAndValidateEventTimelineForProposedNewRevision)
  }

  protected def transactNewRevision(
      asOf: Instant,
      newEventDatumsFor: Revision => Map[EventId, AbstractEventData],
      buildAndValidateEventTimelineForProposedNewRevision: (
          Map[EventId, AbstractEventData],
          Revision,
          Seq[AbstractEventData]) => Unit): Revision

  protected def checkRevisionPrecondition(asOf: Instant,
                                          revisionAsOfs: Seq[Instant]): Unit = {
    if (revisionAsOfs.nonEmpty && revisionAsOfs.last.isAfter(asOf))
      throw new IllegalArgumentException(
        s"'asOf': ${asOf} should be no earlier than that of the last revision: ${revisionAsOfs.last}")
  }
}
