package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{Finite, PositiveInfinity, Unbounded}

import scala.Ordering.Implicits._
import scala.collection.mutable.MutableList

/**
  * Created by Gerard on 25/05/2016.
  */
class WorldReferenceImplementation[EventId](mutableState: MutableState[EventId]) extends WorldImplementationCodeFactoring[EventId] {
  import MutableState._
  import World._
  import WorldImplementationCodeFactoring._

  def this() = this(new MutableState[EventId])

  trait SelfPopulatedScopeUsingMutableState extends ScopeImplementation with SelfPopulatedScope {
    override protected def eventTimeline(nextRevision: Revision): Seq[SerializableEvent] = {
      eventTimelineFrom(mutableState.pertinentEventDatums(nextRevision))
    }
  }

  override def nextRevision: Revision = mutableState.nextRevision

  override def revisionAsOfs: Seq[Instant] = mutableState.revisionAsOfs

  def revise(events: Map[EventId, Option[Event]], asOf: Instant): Revision = {
    mutableState.synchronized {
      mutableState.idOfThreadMostRecentlyStartingARevision = Thread.currentThread.getId
      checkRevisionPrecondition(asOf)
    }

    val newEventDatums: Map[EventId, AbstractEventData] = events.zipWithIndex map { case ((eventId, event), tiebreakerIndex) =>
      eventId -> (event match {
        case Some(event) => EventData(serializableEventFrom(event), nextRevision, tiebreakerIndex)
        case None => AnnulledEventData(nextRevision)
      })
    }

    val obsoleteEventDatums = Set((for {
      eventId <- events.keys
      obsoleteEventData <- mutableState.mostRecentCorrectionOf(eventId)
    } yield obsoleteEventData).toStream: _*)

    val nextRevisionPostThisOne = 1 + nextRevision

    val pertinentEventDatumsExcludingTheNewRevision = mutableState.pertinentEventDatums(nextRevision)

    val eventTimelineIncludingNewRevision = eventTimelineFrom(pertinentEventDatumsExcludingTheNewRevision filterNot obsoleteEventDatums.contains union newEventDatums.values.toStream)

    // This does a check for consistency of the world's history as per this new revision as part of construction.
    // We then throw away the resulting history if successful, the idea being for now to rebuild it as part of
    // constructing a scope to apply queries on.
    new IdentifiedItemsScope(PositiveInfinity[Instant], nextRevisionPostThisOne, Finite(asOf), eventTimelineIncludingNewRevision)

    val revision = nextRevision

    mutableState.synchronized {
      if (mutableState.idOfThreadMostRecentlyStartingARevision != Thread.currentThread.getId){
        throw new RuntimeException("Concurrent revision attempt detected.")
      }

      for ((eventId, eventDatum) <- newEventDatums) {
        mutableState.eventIdToEventCorrectionsMap.getOrElseUpdate(eventId, MutableList.empty) += eventDatum
      }
      mutableState._revisionAsOfs += asOf
      mutableState.checkInvariant()
    }
    revision
  }

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], nextRevision: Revision): Scope = new ScopeBasedOnNextRevision(when, nextRevision) with SelfPopulatedScopeUsingMutableState

  // This produces a 'read-only' scope - raw objects that it renders from bitemporals will fail at runtime if an attempt is made to mutate them, subject to what the proxies can enforce.
  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope = new ScopeBasedOnAsOf(when, asOf) with SelfPopulatedScopeUsingMutableState

  override def forkExperimentalWorld(scope: javaApi.Scope): World[EventId] = {
    val forkedMutableState = new MutableState[EventId] {
      val baseMutableState = mutableState
      val numberOfRevisionsInCommon = scope.nextRevision
      val cutoffWhenAfterWhichHistoriesDiverge = scope.when

      override def nextRevision: Revision = numberOfRevisionsInCommon + super.nextRevision

      override def revisionAsOfs: Seq[Instant] = (baseMutableState.revisionAsOfs take numberOfRevisionsInCommon) ++ super.revisionAsOfs


      override def mostRecentCorrectionOf(eventId: EventId, cutoffRevision: Revision, cutoffWhen: Unbounded[Instant]): Option[AbstractEventData] = {
        val cutoffWhenForBaseWorld = cutoffWhen min cutoffWhenAfterWhichHistoriesDiverge
        if (cutoffRevision > numberOfRevisionsInCommon) {
          import scalaz.std.option.optionInstance
          import scalaz.syntax.monadPlus._
          val superResult = super.mostRecentCorrectionOf(eventId, cutoffRevision, cutoffWhen)
          superResult <+> baseMutableState.mostRecentCorrectionOf(eventId, numberOfRevisionsInCommon, cutoffWhenForBaseWorld)
        } else baseMutableState.mostRecentCorrectionOf(eventId, cutoffRevision, cutoffWhenForBaseWorld)
      }

      override def pertinentEventDatums(cutoffRevision: Revision, cutoffWhen: Unbounded[Instant], excludedEventIds: Set[EventId]): Seq[AbstractEventData] = {
        val cutoffWhenForBaseWorld = cutoffWhen min cutoffWhenAfterWhichHistoriesDiverge
        if (cutoffRevision > numberOfRevisionsInCommon) {
          val (eventIds, eventDatums) = eventIdsAndTheirDatums(cutoffRevision, cutoffWhen, excludedEventIds)
          eventDatums ++ baseMutableState.pertinentEventDatums(numberOfRevisionsInCommon, cutoffWhenForBaseWorld, excludedEventIds union eventIds.toSet)
        } else baseMutableState.pertinentEventDatums(cutoffRevision, cutoffWhenForBaseWorld, excludedEventIds)
      }
    }

    new WorldReferenceImplementation[EventId](forkedMutableState)
  }
}
