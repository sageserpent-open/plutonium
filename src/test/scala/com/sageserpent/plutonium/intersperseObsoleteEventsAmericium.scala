package com.sageserpent.plutonium

import com.sageserpent.americium.Trials
import com.sageserpent.americium.Trials.api

object intersperseObsoleteEventsAmericium {
  type EventId = Int

  def mixUpEnsuringObsoleteThingsAreEventuallySucceededByFinalThings[
      EventRelatedThing
  ](
      finalEventRelatedThings: Seq[EventRelatedThing],
      obsoleteEventRelatedThings: Seq[EventRelatedThing]
  ): Trials[List[(Option[EventRelatedThing], EventId)]] = {
    case class UnfoldState(
        finalEventRelatedThings: Seq[EventRelatedThing],
        obsoleteEventRelatedThings: Seq[EventRelatedThing],
        eventId: EventId,
        eventsToBeCorrected: Set[EventId]
    )
    val onePastMaximumEventId = finalEventRelatedThings.size

    def yieldEitherARecordingOrAnObsoleteRecording(
        unfoldState: UnfoldState
    ): Trials[List[(Option[EventRelatedThing], EventId)]] =
      api.delay {
        if (unfoldState.finalEventRelatedThings.isEmpty) {
          if (unfoldState.eventsToBeCorrected.nonEmpty) {
            // Issue annulments correcting any outstanding obsolete events.
            api
              .choose(unfoldState.eventsToBeCorrected)
              .flatMap(obsoleteEventId =>
                yieldEitherARecordingOrAnObsoleteRecording(
                  unfoldState.copy(
                    eventsToBeCorrected =
                      unfoldState.eventsToBeCorrected - obsoleteEventId
                  )
                ).map((None, obsoleteEventId) :: _)
              )
          } else api.only(Nil) // All done.
        } else {
          api.booleans.flatMap {
            case true if unfoldState.obsoleteEventRelatedThings.nonEmpty =>
              val obsoleteEventRelatedThing =
                unfoldState.obsoleteEventRelatedThings.head
              val remainingObsoleteEventRelatedThings =
                unfoldState.obsoleteEventRelatedThings.tail

              api.booleans.flatMap {
                case true if unfoldState.eventsToBeCorrected.nonEmpty =>
                  // Correct an obsolete event with another obsolete
                  // event.
                  api
                    .choose(unfoldState.eventsToBeCorrected)
                    .flatMap(obsoleteEventId =>
                      yieldEitherARecordingOrAnObsoleteRecording(
                        unfoldState.copy(obsoleteEventRelatedThings =
                          remainingObsoleteEventRelatedThings
                        )
                      ).map(
                        (
                          Some(obsoleteEventRelatedThing),
                          obsoleteEventId
                        ) :: _
                      )
                    )
                case _ =>
                  // Take some event id that denotes a subsequent
                  // non-obsolete event
                  // and make an obsolete revision of it.
                  val maxOffset =
                    onePastMaximumEventId - unfoldState.eventId - 1
                  api.integers(0, maxOffset).flatMap(offset => {
                    val anticipatedEventId = unfoldState.eventId + offset
                    yieldEitherARecordingOrAnObsoleteRecording(
                      unfoldState
                        .copy(
                          obsoleteEventRelatedThings =
                            remainingObsoleteEventRelatedThings,
                          eventsToBeCorrected =
                            unfoldState.eventsToBeCorrected + anticipatedEventId
                        )
                    ).map(
                      (
                        Some(obsoleteEventRelatedThing),
                        anticipatedEventId
                      ) :: _
                    )
                  })
              }
            case _ =>
              api.booleans.flatMap {
                case true if unfoldState.eventsToBeCorrected.nonEmpty =>
                  // Just annul an obsolete event for the sake of it, even though the
                  // non-obsolete correction is still yet to follow.
                  api
                    .choose(unfoldState.eventsToBeCorrected)
                    .flatMap(obsoleteEventId =>
                      yieldEitherARecordingOrAnObsoleteRecording(
                        unfoldState.copy(
                          eventsToBeCorrected =
                            unfoldState.eventsToBeCorrected - obsoleteEventId
                        )
                      ).map((None, obsoleteEventId) :: _)
                    )
                case _ =>
                  // Issue the definitive non-obsolete event; this will not be
                  // subsequently corrected.
                  val eventRelatedThing =
                    unfoldState.finalEventRelatedThings.head
                  val remainingFinalEventRelatedThings =
                    unfoldState.finalEventRelatedThings.tail
                  yieldEitherARecordingOrAnObsoleteRecording(
                    unfoldState.copy(
                      finalEventRelatedThings = remainingFinalEventRelatedThings,
                      eventId = 1 + unfoldState.eventId,
                      eventsToBeCorrected =
                        unfoldState.eventsToBeCorrected - unfoldState.eventId
                    )
                  ).map((Some(eventRelatedThing), unfoldState.eventId) :: _)
              }
          }
        }
      }

    yieldEitherARecordingOrAnObsoleteRecording(
      UnfoldState(
        finalEventRelatedThings = finalEventRelatedThings,
        obsoleteEventRelatedThings = obsoleteEventRelatedThings,
        eventId = 0,
        eventsToBeCorrected = Set.empty
      )
    )
  }

  def chunkKeepingEventIdsUniquePerChunk[EventRelatedThing](
      eventIdPieces: Seq[(Option[EventRelatedThing], EventId)]
  ): Trials[Seq[Seq[(Option[EventRelatedThing], EventId)]]] = {
    api.splitsIntoNonEmptyPieces(eventIdPieces).flatMap { chunks =>
      val processedChunksTrials = chunks.map { chunk =>
        val chunkSeq = chunk.toSeq
        if (
          chunkSeq.groupBy(_._2).exists { case (_, groupForAnEventId) =>
            1 < groupForAnEventId.size
          }
        )
          chunkKeepingEventIdsUniquePerChunk(chunkSeq)
        else
          api.only(Seq(chunkSeq))
      }.toSeq
      api.sequences(processedChunksTrials).map(_.flatten)
    }
  }

  def apply[EventRelatedThing](
      eventRelatedThings: Seq[EventRelatedThing],
      obsoleteEventRelatedThings: Seq[EventRelatedThing]
  ): Trials[Seq[Seq[(Option[EventRelatedThing], EventId)]]] = {
    mixUpEnsuringObsoleteThingsAreEventuallySucceededByFinalThings(
      eventRelatedThings,
      obsoleteEventRelatedThings
    ).flatMap(mixedUp => chunkKeepingEventIdsUniquePerChunk(mixedUp))
  }
}
