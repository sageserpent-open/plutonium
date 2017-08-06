package com.sageserpent.plutonium

import com.sageserpent.americium.randomEnrichment._

import scala.util.Random
import scalaz.std.stream

object intersperseObsoleteEvents {
  type EventId = Int

  def apply[EventRelatedThing](
      random: Random,
      eventRelatedThings: Seq[EventRelatedThing],
      obsoleteEventRelatedThings: Seq[EventRelatedThing])
    : Stream[(Option[EventRelatedThing], EventId)] = {
    case class UnfoldState(eventRelatedThings: Seq[EventRelatedThing],
                           obsoleteEventRelatedThings: Seq[EventRelatedThing],
                           eventId: EventId,
                           eventsToBeCorrected: Set[EventId])
    val onePastMaximumEventId = eventRelatedThings.size

    def yieldEitherARecordingOrAnObsoleteRecording(unfoldState: UnfoldState) =
      unfoldState match {
        case unfoldState @ UnfoldState(eventRelatedThings,
                                       obsoleteEventRelatedThings,
                                       eventId,
                                       eventsToBeCorrected) =>
          if (eventRelatedThings.isEmpty) {
            if (eventsToBeCorrected.nonEmpty) {
              // Issue annulments correcting any outstanding obsolete events.
              val obsoleteEventId = random.chooseOneOf(eventsToBeCorrected)
              Some(
                (None, obsoleteEventId) -> unfoldState.copy(
                  eventsToBeCorrected = eventsToBeCorrected - obsoleteEventId))
            } else None // All done.
          } else if (obsoleteEventRelatedThings.nonEmpty && random
                       .nextBoolean()) {
            val Seq(obsoleteEventRelatedThing,
                    remainingObsoleteEventRelatedThings @ _*) =
              obsoleteEventRelatedThings
            if (eventsToBeCorrected.nonEmpty && random.nextBoolean()) {
              // Correct an obsolete event with another obsolete event.
              Some(
                (Some(obsoleteEventRelatedThing),
                 random.chooseOneOf(eventsToBeCorrected)) -> unfoldState.copy(
                  obsoleteEventRelatedThings =
                    remainingObsoleteEventRelatedThings))
            } else {
              // Take some event id that denotes a subsequent non-obsolete event and make an obsolete revision of it.
              val anticipatedEventId = eventId + random
                .chooseAnyNumberFromZeroToOneLessThan(
                  onePastMaximumEventId - eventId)
              Some((Some(obsoleteEventRelatedThing), anticipatedEventId) -> unfoldState
                .copy(
                  obsoleteEventRelatedThings =
                    remainingObsoleteEventRelatedThings,
                  eventsToBeCorrected = eventsToBeCorrected + anticipatedEventId))
            }
          } else if (eventsToBeCorrected.nonEmpty && random.nextBoolean()) {
            // Just annul an obsolete event for the sake of it, even though the non-obsolete correction is still yet to follow.
            val obsoleteEventId = random.chooseOneOf(eventsToBeCorrected)
            Some(
              (None, obsoleteEventId) -> unfoldState.copy(
                eventsToBeCorrected = eventsToBeCorrected - obsoleteEventId))
          } else {
            // Issue the definitive non-obsolete event; this will not be subsequently corrected.
            val Seq(eventRelatedThing, remainingEventRelatedThings @ _*) =
              eventRelatedThings
            Some(
              (Some(eventRelatedThing), eventId) -> unfoldState.copy(
                eventRelatedThings = remainingEventRelatedThings,
                eventId = 1 + eventId,
                eventsToBeCorrected = eventsToBeCorrected - eventId))
          }
      }

    stream.unfold(
      UnfoldState(eventRelatedThings,
                  obsoleteEventRelatedThings,
                  0,
                  Set.empty))(yieldEitherARecordingOrAnObsoleteRecording)
  }
}
