package com.sageserpent.plutonium

import com.sageserpent.americium.randomEnrichment._

import scala.util.Random

object intersperseObsoleteEvents {
  type EventId = Int

  def mixUpEnsuringObsoleteThingsAreEventuallySucceededByFinalThings[
      EventRelatedThing](random: Random,
                         finalEventRelatedThings: Seq[EventRelatedThing],
                         obsoleteEventRelatedThings: Seq[EventRelatedThing])
    : Stream[(Option[EventRelatedThing], EventId)] = {
    case class UnfoldState(finalEventRelatedThings: Seq[EventRelatedThing],
                           obsoleteEventRelatedThings: Seq[EventRelatedThing],
                           eventId: EventId,
                           eventsToBeCorrected: Set[EventId])
    val onePastMaximumEventId = finalEventRelatedThings.size

    def yieldEitherARecordingOrAnObsoleteRecording(unfoldState: UnfoldState)
      : Stream[(Option[EventRelatedThing], EventId)] =
      unfoldState match {
        case unfoldState @ UnfoldState(finalEventRelatedThings,
                                       obsoleteEventRelatedThings,
                                       eventId,
                                       eventsToBeCorrected) =>
          if (finalEventRelatedThings.isEmpty) {
            if (eventsToBeCorrected.nonEmpty) {
              // Issue annulments correcting any outstanding obsolete events.
              val obsoleteEventId = random.chooseOneOf(eventsToBeCorrected)

              (None, obsoleteEventId) #:: yieldEitherARecordingOrAnObsoleteRecording(
                unfoldState.copy(
                  eventsToBeCorrected = eventsToBeCorrected - obsoleteEventId))
            } else Stream.empty // All done.
          } else if (obsoleteEventRelatedThings.nonEmpty && random
                       .nextBoolean()) {
            val Seq(obsoleteEventRelatedThing,
                    remainingObsoleteEventRelatedThings @ _*) =
              obsoleteEventRelatedThings
            if (eventsToBeCorrected.nonEmpty && random.nextBoolean()) {
              // Correct an obsolete event with another obsolete event.
              (Some(obsoleteEventRelatedThing),
               random
                 .chooseOneOf(eventsToBeCorrected)) #:: yieldEitherARecordingOrAnObsoleteRecording(
                unfoldState.copy(obsoleteEventRelatedThings =
                  remainingObsoleteEventRelatedThings))
            } else {
              // Take some event id that denotes a subsequent non-obsolete event and make an obsolete revision of it.
              val anticipatedEventId = eventId + random
                .chooseAnyNumberFromZeroToOneLessThan(
                  onePastMaximumEventId - eventId)
              (Some(obsoleteEventRelatedThing), anticipatedEventId) #:: yieldEitherARecordingOrAnObsoleteRecording(
                unfoldState
                  .copy(
                    obsoleteEventRelatedThings =
                      remainingObsoleteEventRelatedThings,
                    eventsToBeCorrected = eventsToBeCorrected + anticipatedEventId))
            }
          } else if (eventsToBeCorrected.nonEmpty && random.nextBoolean()) {
            // Just annul an obsolete event for the sake of it, even though the non-obsolete correction is still yet to follow.
            val obsoleteEventId = random.chooseOneOf(eventsToBeCorrected)
            (None, obsoleteEventId) #:: yieldEitherARecordingOrAnObsoleteRecording(
              unfoldState.copy(
                eventsToBeCorrected = eventsToBeCorrected - obsoleteEventId))
          } else {
            // Issue the definitive non-obsolete event; this will not be subsequently corrected.
            val Seq(eventRelatedThing, remainingFinalEventRelatedThings @ _*) =
              finalEventRelatedThings
            (Some(eventRelatedThing), eventId) #:: yieldEitherARecordingOrAnObsoleteRecording(
              unfoldState.copy(
                finalEventRelatedThings = remainingFinalEventRelatedThings,
                eventId = 1 + eventId,
                eventsToBeCorrected = eventsToBeCorrected - eventId))
          }
      }

    yieldEitherARecordingOrAnObsoleteRecording(
      UnfoldState(finalEventRelatedThings,
                  obsoleteEventRelatedThings,
                  0,
                  Set.empty))
  }

  def chunkKeepingEventIdsUniquePerChunk[EventRelatedThing](
      random: Random,
      eventIdPieces: Stream[(EventRelatedThing, EventId)])
    : Stream[Stream[(EventRelatedThing, EventId)]] = {
    val trialSplit: Stream[Stream[(EventRelatedThing, EventId)]] =
      random.splitIntoNonEmptyPieces(eventIdPieces)

    trialSplit flatMap (chunk =>
      if (chunk.groupBy(_._2).exists {
            case (_, groupForAnEventId) => 1 < groupForAnEventId.size
          })
        chunkKeepingEventIdsUniquePerChunk(random, chunk)
      else Stream(chunk))
  }

  def apply[EventRelatedThing](
      random: Random,
      eventRelatedThings: Seq[EventRelatedThing],
      obsoleteEventRelatedThings: Seq[EventRelatedThing])
    : Stream[Stream[(Option[EventRelatedThing], EventId)]] = {
    chunkKeepingEventIdsUniquePerChunk(
      random,
      mixUpEnsuringObsoleteThingsAreEventuallySucceededByFinalThings(
        random,
        eventRelatedThings,
        obsoleteEventRelatedThings).force)
  }
}
