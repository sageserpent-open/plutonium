package com.sageserpent.plutonium

import java.lang.reflect.Method
import java.time.Instant

import com.sageserpent.americium.{Finite, NegativeInfinity, Unbounded}
import com.sageserpent.plutonium.World.Revision
import net.bytebuddy.description.method.MethodDescription

import scala.collection.JavaConversions._
import scala.collection.Searching._
import scala.reflect.runtime.universe.{Super => _, This => _}

object WorldImplementationCodeFactoring {
  type EventOrderingTiebreakerIndex = Int

  sealed abstract class AbstractEventData extends java.io.Serializable {
    val introducedInRevision: Revision
  }

  case class EventData(
      serializableEvent: Event,
      override val introducedInRevision: Revision,
      eventOrderingTiebreakerIndex: EventOrderingTiebreakerIndex)
      extends AbstractEventData

  case class AnnulledEventData(override val introducedInRevision: Revision)
      extends AbstractEventData

  implicit val eventOrdering = Ordering.by((_: Event).when)

  implicit val eventDataOrdering: Ordering[EventData] = Ordering.by {
    case EventData(serializableEvent,
                   introducedInRevision,
                   eventOrderingTiebreakerIndex) =>
      (serializableEvent, introducedInRevision, eventOrderingTiebreakerIndex)
  }

  def eventTimelineFrom[EventId](
      eventDatums: Seq[(EventId, AbstractEventData)]): Seq[(Event, EventId)] =
    (eventDatums collect {
      case (eventId, eventData: EventData) => eventId -> eventData
    }).sortBy(_._2).map {
      case (eventId, eventData) => eventData.serializableEvent -> eventId
    }

  def recordPatches[EventId](eventTimeline: Seq[(Event, EventId)],
                             patchRecorder: PatchRecorder[EventId]) = {
    for ((event, eventId) <- eventTimeline) event match {
      case Change(when, patches) =>
        for (patch <- patches) {
          patchRecorder.recordPatchFromChange(eventId, when, patch)
        }

      case Measurement(when, patches) =>
        for (patch <- patches) {
          patchRecorder.recordPatchFromMeasurement(eventId, when, patch)
        }

      case annihilation @ Annihilation(when, id) =>
        patchRecorder.recordAnnihilation(eventId, annihilation)
    }

    patchRecorder.noteThatThereAreNoFollowingRecordings()
  }

  def firstMethodIsOverrideCompatibleWithSecond(
      firstMethod: MethodDescription,
      secondMethod: MethodDescription): Boolean =
    secondMethod.getName == firstMethod.getName &&
      secondMethod.getReceiverType.asErasure
        .isAssignableFrom(firstMethod.getReceiverType.asErasure) &&
      (secondMethod.getReturnType.asErasure
        .isAssignableFrom(firstMethod.getReturnType.asErasure) ||
        secondMethod.getReturnType.asErasure
          .isAssignableFrom(firstMethod.getReturnType.asErasure.asBoxed)) &&
      secondMethod.getParameters.size == firstMethod.getParameters.size &&
      secondMethod.getParameters.toSeq
        .map(_.getType) == firstMethod.getParameters.toSeq
        .map(_.getType) // What about contravariance? Hmmm...

  def firstMethodIsOverrideCompatibleWithSecond(
      firstMethod: Method,
      secondMethod: Method): Boolean = {
    firstMethodIsOverrideCompatibleWithSecond(
      new MethodDescription.ForLoadedMethod(firstMethod),
      new MethodDescription.ForLoadedMethod(secondMethod))
  }

  
}

abstract class WorldImplementationCodeFactoring[EventId]
    extends World[EventId] {
  abstract class ScopeBasedOnNextRevision(val when: Unbounded[Instant],
                                          val nextRevision: Revision)
      extends com.sageserpent.plutonium.Scope {
    val asOf = nextRevision match {
      case World.initialRevision => NegativeInfinity[Instant]()
      case _ =>
        if (nextRevision <= revisionAsOfs.size)
          Finite(revisionAsOfs(nextRevision - 1))
        else
          throw new RuntimeException(
            s"Scope based the revision prior to: $nextRevision can't be constructed - there are only ${revisionAsOfs.size} revisions of the world.")
    }
  }

  abstract class ScopeBasedOnAsOf(val when: Unbounded[Instant],
                                  unliftedAsOf: Instant)
      extends com.sageserpent.plutonium.Scope {
    override val asOf = Finite(unliftedAsOf)

    override val nextRevision: Revision = {
      revisionAsOfs.search(unliftedAsOf) match {
        case found @ Found(_) =>
          val versionTimelineNotIncludingAllUpToTheMatch = revisionAsOfs drop (1 + found.foundIndex)
          versionTimelineNotIncludingAllUpToTheMatch.indexWhere(
            implicitly[Ordering[Instant]].lt(unliftedAsOf, _)) match {
            case -1    => revisionAsOfs.length
            case index => found.foundIndex + 1 + index
          }
        case notFound @ InsertionPoint(_) => notFound.insertionPoint
      }
    }
  }

  
}
