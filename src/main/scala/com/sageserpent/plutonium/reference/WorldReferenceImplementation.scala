package com.sageserpent.plutonium.reference

import com.sageserpent.plutonium.utilities.{PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.{Event, EventId, ItemCacheImplementation, Scope, UniqueItemSpecification, World, WorldImplementationCodeFactoring}

import java.time.Instant
import scala.Ordering.Implicits._
import scala.collection.Searching._
import scala.collection.{IndexedSeqView, mutable}
import scala.language.postfixOps

private object MutableState {

  import World._
  import WorldImplementationCodeFactoring._

  private type EventCorrections = mutable.ArrayBuffer[AbstractEventData]
  private type EventIdToEventCorrectionsMap[EventId] =
    mutable.Map[EventId, EventCorrections]

  private def numberOfEventCorrectionsPriorToCutoff(
      eventCorrections: EventCorrections,
      cutoffRevision: Revision
  ): EventOrderingTiebreakerIndex = {
    val revisionsIndexedSeq: IndexedSeqView[Revision] =
      eventCorrections.view.map(_.introducedInRevision)

    revisionsIndexedSeq.search(cutoffRevision) match {
      case Found(foundIndex)              => foundIndex
      case InsertionPoint(insertionPoint) => insertionPoint
    }
  }
}

private class MutableState {

  import MutableState._
  import World._
  import WorldImplementationCodeFactoring._

  type EventIdInclusion = EventId => Boolean
  val readerThreadsThatHaveNotBeenBouncedByARevision: mutable.Set[Long] =
    mutable.Set.empty
  val writerThreadsThatHaveNotBeenBouncedByARevision: mutable.Set[Long] =
    mutable.Set.empty
  val eventIdToEventCorrectionsMap: EventIdToEventCorrectionsMap[EventId] =
    mutable.Map.empty
  val _revisionAsOfs: mutable.ArrayBuffer[Instant] = mutable.ArrayBuffer.empty

  def nextRevision: Revision = _revisionAsOfs.size

  def pertinentEventDatums(
      cutoffRevision: Revision,
      eventIds: Iterable[EventId]
  ): LazyList[(EventId, AbstractEventData)] = {
    val eventIdsToBeExcluded = eventIds.toSet
    pertinentEventDatums(
      cutoffRevision,
      PositiveInfinity,
      eventId => !eventIdsToBeExcluded.contains(eventId)
    )
  }

  def pertinentEventDatums(
      cutoffRevision: Revision
  ): LazyList[(EventId, AbstractEventData)] =
    pertinentEventDatums(cutoffRevision, PositiveInfinity, _ => true)

  def pertinentEventDatums(
      cutoffRevision: Revision,
      cutoffWhen: Unbounded[Instant],
      eventIdInclusion: EventIdInclusion
  ): LazyList[(EventId, AbstractEventData)] =
    eventIdsAndTheirDatums(cutoffRevision, eventIdInclusion)
      .filterNot(PartialFunction.cond(_) { case (_, eventData: EventData) =>
        eventData.serializableEvent.when > cutoffWhen
      })

  def eventIdsAndTheirDatums(
      cutoffRevision: Revision,
      eventIdInclusion: EventIdInclusion
  ): LazyList[(Any, AbstractEventData)] = {
    LazyList.from(eventIdToEventCorrectionsMap) collect {
      case (eventId, eventCorrections) if eventIdInclusion(eventId) =>
        val onePastIndexOfRelevantEventCorrection =
          numberOfEventCorrectionsPriorToCutoff(
            eventCorrections,
            cutoffRevision
          )
        if (0 < onePastIndexOfRelevantEventCorrection)
          Some(
            eventId -> eventCorrections(
              onePastIndexOfRelevantEventCorrection - 1
            )
          )
        else
          None
    } collect { case Some(idAndDataPair) =>
      idAndDataPair
    }
  }

  def checkInvariant() = {
    assert(revisionAsOfs zip revisionAsOfs.tail forall { case (first, second) =>
      first <= second
    })
  }

  def revisionAsOfs: Array[Instant] = _revisionAsOfs.toArray
}

class WorldReferenceImplementation(mutableState: MutableState)
    extends WorldImplementationCodeFactoring {

  import World._
  import WorldImplementationCodeFactoring._

  def this() = this(new MutableState)

  override def close(): Unit = {}

  override def scopeFor(
      when: Unbounded[Instant],
      nextRevision: Revision
  ): Scope =
    new ScopeBasedOnNextRevision(when, nextRevision) with SelfPopulatedScope {}

  override def scopeFor(when: Unbounded[Instant], asOf: Instant): Scope =
    new ScopeBasedOnAsOf(when, asOf) with SelfPopulatedScope

  def revise_(
      events: collection.Map[_ <: EventId, Option[Event]],
      asOf: Instant
  ): Revision = {
    def newEventDatumsFor(
        nextRevisionPriorToUpdate: Revision
    ): Iterable[(EventId, AbstractEventData)] = {
      events.zipWithIndex map { case ((eventId, event), tiebreakerIndex) =>
        eventId -> (event match {
          case Some(event) =>
            EventData(event, nextRevisionPriorToUpdate, tiebreakerIndex)
          case None => AnnulledEventData(nextRevisionPriorToUpdate)
        })
      }
    }

    def buildAndValidateEventTimelineForProposedNewRevision(
        newEventDatums: Seq[(EventId, AbstractEventData)],
        pertinentEventDatumsExcludingTheNewRevision: Seq[
          (EventId, AbstractEventData)
        ]
    ): Unit = {
      val eventTimelineIncludingNewRevision = eventTimelineFrom(
        pertinentEventDatumsExcludingTheNewRevision union newEventDatums
      )

      (new IdentifiedItemsScope)
        .populate(PositiveInfinity, eventTimelineIncludingNewRevision)
    }

    transactNewRevision(
      asOf,
      newEventDatumsFor,
      buildAndValidateEventTimelineForProposedNewRevision
    )
  }

  private def transactNewRevision(
      asOf: Instant,
      newEventDatumsFor: Revision => Iterable[(EventId, AbstractEventData)],
      buildAndValidateEventTimelineForProposedNewRevision: (
          Seq[(EventId, AbstractEventData)],
          Seq[(EventId, AbstractEventData)]
      ) => Unit
  ): Revision = {

    val (
      newEventDatums,
      nextRevisionPriorToUpdate,
      pertinentEventDatumsExcludingTheNewRevision
    ) =
      mutableState.synchronized {
        mutableState.writerThreadsThatHaveNotBeenBouncedByARevision += Thread.currentThread.getId
        checkRevisionPrecondition(asOf, revisionAsOfs)
        val nextRevisionPriorToUpdate = nextRevision
        val newEventDatums = newEventDatumsFor(nextRevisionPriorToUpdate)
        val pertinentEventDatumsExcludingTheNewRevision =
          mutableState.pertinentEventDatums(
            nextRevisionPriorToUpdate,
            newEventDatums.map(_._1)
          )
        (
          newEventDatums,
          nextRevisionPriorToUpdate,
          pertinentEventDatumsExcludingTheNewRevision
        )
      }

    buildAndValidateEventTimelineForProposedNewRevision(
      newEventDatums.toSeq,
      pertinentEventDatumsExcludingTheNewRevision
    )

    mutableState.synchronized {
      if (
        !mutableState.writerThreadsThatHaveNotBeenBouncedByARevision
          .contains(Thread.currentThread.getId)
      ) {
        throw new RuntimeException(
          "Concurrent revision attempt detected in revision."
        )
      }

      mutableState.readerThreadsThatHaveNotBeenBouncedByARevision.clear()
      mutableState.writerThreadsThatHaveNotBeenBouncedByARevision.clear()

      for ((eventId, eventDatum) <- newEventDatums) {
        mutableState.eventIdToEventCorrectionsMap
          .getOrElseUpdate(eventId, mutable.ArrayBuffer.empty) += eventDatum
      }
      mutableState._revisionAsOfs += asOf
      mutableState.checkInvariant()
    }

    nextRevisionPriorToUpdate
  }

  private def checkRevisionPrecondition(
      asOf: Instant,
      revisionAsOfs: Seq[Instant]
  ): Unit = {
    if (revisionAsOfs.nonEmpty && revisionAsOfs.last.isAfter(asOf))
      throw new IllegalArgumentException(
        s"'asOf': ${asOf} should be no earlier than that of the last revision: ${revisionAsOfs.last}"
      )
  }

  override def nextRevision: Revision = mutableState.nextRevision

  override def revisionAsOfs: Array[Instant] = mutableState.revisionAsOfs

  override def forkExperimentalWorld(scope: Scope): World = {
    val forkedMutableState = new MutableState {
      private val baseMutableState                     = mutableState
      private val numberOfRevisionsInCommon            = scope.nextRevision
      private val cutoffWhenAfterWhichHistoriesDiverge = scope.when

      override def nextRevision: Revision =
        numberOfRevisionsInCommon + super.nextRevision

      override def revisionAsOfs: Array[Instant] =
        (baseMutableState.revisionAsOfs take numberOfRevisionsInCommon) ++ super.revisionAsOfs

      override def pertinentEventDatums(
          cutoffRevision: Revision,
          cutoffWhen: Unbounded[Instant],
          eventIdInclusion: EventIdInclusion
      ): LazyList[(EventId, AbstractEventData)] = {
        val cutoffWhenForBaseWorld =
          cutoffWhen min cutoffWhenAfterWhichHistoriesDiverge
        if (cutoffRevision > numberOfRevisionsInCommon) {
          val allEventsUpToTheCutoffRevisionRegardlessOfEventWhen =
            eventIdsAndTheirDatums(cutoffRevision, eventIdInclusion)
          val eventIdsToBeExcluded =
            allEventsUpToTheCutoffRevisionRegardlessOfEventWhen.map(_._1).toSet

          allEventsUpToTheCutoffRevisionRegardlessOfEventWhen
            .filterNot(PartialFunction.cond(_) {
              case (_, eventData: EventData) =>
                eventData.serializableEvent.when > cutoffWhen
            }) lazyAppendedAll baseMutableState.pertinentEventDatums(
            numberOfRevisionsInCommon,
            cutoffWhenForBaseWorld,
            eventId =>
              !eventIdsToBeExcluded.contains(eventId) && eventIdInclusion(
                eventId
              )
          )
        } else
          baseMutableState.pertinentEventDatums(
            cutoffRevision,
            cutoffWhenForBaseWorld,
            eventIdInclusion
          )
      }
    }

    new WorldReferenceImplementation(forkedMutableState)
  }

  protected def eventTimeline(
      cutoffRevision: Revision
  ): Seq[(Event, EventId)] = {
    val idOfThreadThatMostlyRecentlyStartedARevisionBeforehand =
      mutableState.synchronized {
        mutableState.readerThreadsThatHaveNotBeenBouncedByARevision += Thread.currentThread.getId
      }
    val result = eventTimelineFrom(
      mutableState.pertinentEventDatums(cutoffRevision)
    )
    mutableState.synchronized {
      if (
        !mutableState.readerThreadsThatHaveNotBeenBouncedByARevision
          .contains(Thread.currentThread.getId)
      ) {
        throw new RuntimeException(
          "Concurrent revision attempt detected in query."
        )
      }
    }
    result
  }

  trait SelfPopulatedScope
      extends com.sageserpent.plutonium.Scope
      with ItemCacheImplementation {
    val identifiedItemsScope = new IdentifiedItemsScope

    override def itemsFor[Item](
        uniqueItemSpecification: UniqueItemSpecification
    ): LazyList[Item] =
      identifiedItemsScope.itemsFor(uniqueItemSpecification)

    override def allItems[Item](clazz: Class[Item]): LazyList[Item] =
      identifiedItemsScope.allItems(clazz)

    identifiedItemsScope.populate(when, eventTimeline(nextRevision))
  }
}
