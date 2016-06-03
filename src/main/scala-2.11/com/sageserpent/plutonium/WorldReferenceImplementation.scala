package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{PositiveInfinity, Unbounded}

import scala.Ordering.Implicits._
import scala.collection.Searching._
import scala.collection.generic.IsSeqLike
import scala.collection.mutable.MutableList
import scala.collection.{SeqLike, SeqView, mutable}

/**
  * Created by Gerard on 25/05/2016.
  */

object MutableState {

  import World._
  import WorldImplementationCodeFactoring._

  type EventCorrections = MutableList[AbstractEventData]
  type EventIdToEventCorrectionsMap[EventId] = mutable.Map[EventId, EventCorrections]

  def eventCorrectionsPriorToCutoffRevision(eventCorrections: EventCorrections, cutoffRevision: Revision): EventCorrections =
    eventCorrections take numberOfEventCorrectionsPriorToCutoff(eventCorrections, cutoffRevision)

  implicit val isSeqLike = new IsSeqLike[SeqView[Revision, Seq[_]]] {
    type A = Revision
    override val conversion: SeqView[Revision, Seq[_]] => SeqLike[this.A, SeqView[Revision, Seq[_]]] = identity
  }

  def numberOfEventCorrectionsPriorToCutoff(eventCorrections: EventCorrections, cutoffRevision: Revision): EventOrderingTiebreakerIndex = {
    val revisionsView: SeqView[Revision, Seq[_]] = eventCorrections.view.map(_.introducedInRevision)

    revisionsView.search(cutoffRevision) match {
      case Found(foundIndex) => foundIndex
      case InsertionPoint(insertionPoint) => insertionPoint
    }
  }
}

class MutableState[EventId] {

  import MutableState._
  import World._
  import WorldImplementationCodeFactoring._

  var idOfThreadMostRecentlyStartingARevision: Long = -1L

  val eventIdToEventCorrectionsMap: EventIdToEventCorrectionsMap[EventId] = mutable.Map.empty
  val _revisionAsOfs: MutableList[Instant] = MutableList.empty

  def revisionAsOfs: Seq[Instant] = _revisionAsOfs

  def nextRevision: Revision = _revisionAsOfs.size

  def mostRecentCorrectionOf(eventId: EventId): Option[AbstractEventData] = {
    mostRecentCorrectionOf(eventId, nextRevision, PositiveInfinity())
  }

  def mostRecentCorrectionOf(eventId: EventId, cutoffRevision: Revision, cutoffWhen: Unbounded[Instant]): Option[AbstractEventData] = {
    eventIdToEventCorrectionsMap.get(eventId) flatMap {
      eventCorrections =>
        val onePastIndexOfRelevantEventCorrection = numberOfEventCorrectionsPriorToCutoff(eventCorrections, cutoffRevision)
        if (0 < onePastIndexOfRelevantEventCorrection)
          Some(eventCorrections(onePastIndexOfRelevantEventCorrection - 1))
        else
          None
    } filterNot (PartialFunction.cond(_) { case eventData: EventData => eventData.serializableEvent.when > cutoffWhen })
  }

  def pertinentEventDatums(cutoffRevision: Revision, cutoffWhen: Unbounded[Instant], excludedEventIds: Set[EventId]): Seq[AbstractEventData] =
    eventIdsAndTheirDatums(cutoffRevision, cutoffWhen, excludedEventIds)._2

  def eventIdsAndTheirDatums(cutoffRevision: Revision, cutoffWhen: Unbounded[Instant], excludedEventIds: Set[EventId]) = {
    val eventIdAndDataPairs = eventIdToEventCorrectionsMap collect {
      case (eventId, eventCorrections) if !excludedEventIds.contains(eventId) =>
        val onePastIndexOfRelevantEventCorrection = numberOfEventCorrectionsPriorToCutoff(eventCorrections, cutoffRevision)
        if (0 < onePastIndexOfRelevantEventCorrection)
          Some(eventId -> eventCorrections(onePastIndexOfRelevantEventCorrection - 1))
        else
          None
    } collect { case Some(idAndDataPair) => idAndDataPair }
    val (eventIds, eventDatums) = eventIdAndDataPairs.unzip

    eventIds -> eventDatums.filterNot(PartialFunction.cond(_) { case eventData: EventData => eventData.serializableEvent.when > cutoffWhen }).toStream
  }

  def pertinentEventDatums(cutoffRevision: Revision): Seq[AbstractEventData] =
    pertinentEventDatums(cutoffRevision, PositiveInfinity(), Set.empty)

  def checkInvariant() = {
    assert(revisionAsOfs zip revisionAsOfs.tail forall { case (first, second) => first <= second })
  }
}


class WorldReferenceImplementation[EventId](mutableState: MutableState[EventId]) extends WorldImplementationCodeFactoring[EventId] {

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

  override protected def transactNewRevision(asOf: Instant, newEventDatums: Map[EventId, AbstractEventData])
                                            (buildAndValidateEventTimelineForProposedNewRevision: (Seq[AbstractEventData], Set[AbstractEventData]) => Unit): Unit = {
    mutableState.synchronized {
      mutableState.idOfThreadMostRecentlyStartingARevision = Thread.currentThread.getId
      checkRevisionPrecondition(asOf)
    }

    val obsoleteEventDatums = Set((for {
      eventId <- newEventDatums.keys
      obsoleteEventData <- mutableState.mostRecentCorrectionOf(eventId)
    } yield obsoleteEventData).toStream: _*)

    val pertinentEventDatumsExcludingTheNewRevision = mutableState.pertinentEventDatums(nextRevision)

    buildAndValidateEventTimelineForProposedNewRevision(pertinentEventDatumsExcludingTheNewRevision, obsoleteEventDatums)

    mutableState.synchronized {
      if (mutableState.idOfThreadMostRecentlyStartingARevision != Thread.currentThread.getId) {
        throw new RuntimeException("Concurrent revision attempt detected.")
      }

      for ((eventId, eventDatum) <- newEventDatums) {
        mutableState.eventIdToEventCorrectionsMap.getOrElseUpdate(eventId, MutableList.empty) += eventDatum
      }
      mutableState._revisionAsOfs += asOf
      mutableState.checkInvariant()
    }
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
