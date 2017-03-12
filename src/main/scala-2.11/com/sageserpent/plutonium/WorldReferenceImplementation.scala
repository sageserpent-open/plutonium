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

  val readerThreadsThatHaveNotBeenBouncedByARevision: mutable.Set[Long] = mutable.Set.empty
  val writerThreadsThatHaveNotBeenBouncedByARevision: mutable.Set[Long] = mutable.Set.empty

  val eventIdToEventCorrectionsMap: EventIdToEventCorrectionsMap[EventId] = mutable.Map.empty
  val _revisionAsOfs: MutableList[Instant] = MutableList.empty

  def revisionAsOfs: Array[Instant] = _revisionAsOfs.toArray

  def nextRevision: Revision = _revisionAsOfs.size

  type EventIdInclusion = EventId => Boolean

  def pertinentEventDatums(cutoffRevision: Revision, cutoffWhen: Unbounded[Instant], eventIdInclusion: EventIdInclusion): Seq[AbstractEventData] =
    eventIdsAndTheirDatums(cutoffRevision, cutoffWhen, eventIdInclusion)._2

  def eventIdsAndTheirDatums(cutoffRevision: Revision, cutoffWhen: Unbounded[Instant], eventIdInclusion: EventIdInclusion) = {
    val eventIdAndDataPairs = eventIdToEventCorrectionsMap collect {
      case (eventId, eventCorrections) if eventIdInclusion(eventId) =>
        val onePastIndexOfRelevantEventCorrection = numberOfEventCorrectionsPriorToCutoff(eventCorrections, cutoffRevision)
        if (0 < onePastIndexOfRelevantEventCorrection)
          Some(eventId -> eventCorrections(onePastIndexOfRelevantEventCorrection - 1))
        else
          None
    } collect { case Some(idAndDataPair) => idAndDataPair }

    val (eventIds, eventDatums) = eventIdAndDataPairs.unzip

    eventIds -> eventDatums.filterNot(PartialFunction.cond(_) { case eventData: EventData => eventData.serializableEvent.when > cutoffWhen }).toStream
  }

  def pertinentEventDatums(cutoffRevision: Revision, eventIds: Iterable[EventId]): Seq[AbstractEventData] = {
    val eventIdsToBeExcluded = eventIds.toSet
    pertinentEventDatums(cutoffRevision, PositiveInfinity(), eventId => !eventIdsToBeExcluded.contains(eventId))
  }

  def pertinentEventDatums(cutoffRevision: Revision): Seq[AbstractEventData] =
    pertinentEventDatums(cutoffRevision, PositiveInfinity(), _ => true)

  def checkInvariant() = {
    assert(revisionAsOfs zip revisionAsOfs.tail forall { case (first, second) => first <= second })
  }
}


class WorldReferenceImplementation[EventId](mutableState: MutableState[EventId]) extends WorldInefficientImplementationCodeFactoring[EventId] {

  import World._
  import WorldImplementationCodeFactoring._

  def this() = this(new MutableState[EventId])

  override def nextRevision: Revision = mutableState.nextRevision

  override def forkExperimentalWorld(scope: javaApi.Scope): World[EventId] = {
    val forkedMutableState = new MutableState[EventId] {
      val baseMutableState = mutableState
      val numberOfRevisionsInCommon = scope.nextRevision
      val cutoffWhenAfterWhichHistoriesDiverge = scope.when

      override def nextRevision: Revision = numberOfRevisionsInCommon + super.nextRevision

      override def revisionAsOfs: Array[Instant] = (baseMutableState.revisionAsOfs take numberOfRevisionsInCommon) ++ super.revisionAsOfs

      override def pertinentEventDatums(cutoffRevision: Revision, cutoffWhen: Unbounded[Instant], eventIdInclusion: EventIdInclusion): Seq[AbstractEventData] = {
        val cutoffWhenForBaseWorld = cutoffWhen min cutoffWhenAfterWhichHistoriesDiverge
        if (cutoffRevision > numberOfRevisionsInCommon) {
          val (eventIds, eventDatums) = eventIdsAndTheirDatums(cutoffRevision, cutoffWhen, eventIdInclusion)
          val eventIdsToBeExcluded = eventIds.toSet
          eventDatums ++ baseMutableState.pertinentEventDatums(numberOfRevisionsInCommon, cutoffWhenForBaseWorld, eventId => !eventIdsToBeExcluded.contains(eventId) && eventIdInclusion(eventId))
        } else baseMutableState.pertinentEventDatums(cutoffRevision, cutoffWhenForBaseWorld, eventIdInclusion)
      }
    }

    new WorldReferenceImplementation[EventId](forkedMutableState)
  }

  override def revisionAsOfs: Array[Instant] = mutableState.revisionAsOfs

  override protected def eventTimeline(cutoffRevision: Revision): Seq[SerializableEvent] = {
    val idOfThreadThatMostlyRecentlyStartedARevisionBeforehand = mutableState.synchronized {
      mutableState.readerThreadsThatHaveNotBeenBouncedByARevision += Thread.currentThread.getId
    }
    val result = eventTimelineFrom(mutableState.pertinentEventDatums(cutoffRevision))
    mutableState.synchronized {
      if (!mutableState.readerThreadsThatHaveNotBeenBouncedByARevision.contains(Thread.currentThread.getId)) {
        throw new RuntimeException("Concurrent revision attempt detected in query.")
      }
    }
    result
  }

  override protected def transactNewRevision(asOf: Instant,
                                             newEventDatumsFor: Revision => Map[EventId, AbstractEventData],
                                             buildAndValidateEventTimelineForProposedNewRevision: (Map[EventId, AbstractEventData], Revision, Seq[AbstractEventData]) => Unit): Revision = {



    val (newEventDatums, nextRevisionPriorToUpdate, pertinentEventDatumsExcludingTheNewRevision) = mutableState.synchronized {
      mutableState.writerThreadsThatHaveNotBeenBouncedByARevision += Thread.currentThread.getId
      checkRevisionPrecondition(asOf, revisionAsOfs)
      val nextRevisionPriorToUpdate = nextRevision
      val newEventDatums: Map[EventId, AbstractEventData] = newEventDatumsFor(nextRevisionPriorToUpdate)
      val pertinentEventDatumsExcludingTheNewRevision = mutableState.pertinentEventDatums(nextRevisionPriorToUpdate, newEventDatums.keys)
      (newEventDatums, nextRevisionPriorToUpdate, pertinentEventDatumsExcludingTheNewRevision)
    }

    buildAndValidateEventTimelineForProposedNewRevision(newEventDatums, nextRevisionPriorToUpdate, pertinentEventDatumsExcludingTheNewRevision)

    mutableState.synchronized {
      if (!mutableState.writerThreadsThatHaveNotBeenBouncedByARevision.contains(Thread.currentThread.getId)) {
        throw new RuntimeException("Concurrent revision attempt detected in revision.")
      }

      mutableState.readerThreadsThatHaveNotBeenBouncedByARevision.clear()
      mutableState.writerThreadsThatHaveNotBeenBouncedByARevision.clear()

      for ((eventId, eventDatum) <- newEventDatums) {
        mutableState.eventIdToEventCorrectionsMap.getOrElseUpdate(eventId, MutableList.empty) += eventDatum
      }
      mutableState._revisionAsOfs += asOf
      mutableState.checkInvariant()
    }

    nextRevisionPriorToUpdate
  }
}
