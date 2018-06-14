package com.sageserpent.plutonium

import org.scalacheck.{ShrinkLowPriority => NoShrinking}
import org.scalatest.exceptions.TestFailedException
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Inspectors, Matchers}

import scala.collection.immutable.TreeMap
import scala.util.Random

class TimelineSpec
    extends FlatSpec
    with Matchers
    with GeneratorDrivenPropertyChecks
    with NoShrinking
    with WorldSpecSupport {

  type SpecificEventId = Int

  "Booking in events in one block revision" should "capture the correct state in the blob storage" in {
    forAll(recordingsGroupedByIdGenerator(forbidAnnihilations = false),
           unboundedInstantGenerator) { (recordingsGroupedById, queryWhen) =>
      val events: Map[SpecificEventId, Some[Event]] = TreeMap(
        // Use 'TreeMap' as it won't rearrange the key ordering. We do this purely
        // to ensure that events that occur at the same time won't be shuffled as
        // they go into the map wrt the expected histories.
        (recordingsGroupedById.flatMap(_.events) map (_._2)).zipWithIndex map {
          case (event, index) => index -> Some(event)
        }: _*)

      val itemCache: ItemCache =
        emptyTimeline()
          .revise(events)
          .itemCacheAt(queryWhen)

      val checks = for {
        RecordingsNoLaterThan(
          historyId,
          historiesFrom,
          pertinentRecordings,
          _,
          _) <- recordingsGroupedById flatMap (_.thePartNoLaterThan(queryWhen))
        Seq(history) = historiesFrom(itemCache)
      } yield (historyId, history.datums, pertinentRecordings.map(_._1))

      Inspectors.forAll(checks) {
        case (historyId, actualDatums, expectedDatums) =>
          try {
            actualDatums should contain theSameElementsInOrderAs expectedDatums
          } catch {
            case testFailedException: TestFailedException =>
              throw testFailedException.modifyMessage(_.map(message =>
                s"for id: $historyId at: $queryWhen, $message"))
          }
      }
    }
  }

  "Booking in events over several revisions with other events that are revised or annulled" should "result in the same blob storage state as booking in the events via a single block revision" in {
    forAll(
      recordingsGroupedByIdGenerator(forbidAnnihilations = false),
      nonConflictingRecordingsGroupedByIdGenerator.map(
        _.flatMap(_.events) map (_._2)),
      seedGenerator,
      unboundedInstantGenerator
    ) { (recordingsGroupedById, obsoleteEvents, seed, queryWhen) =>
      val events = recordingsGroupedById.flatMap(_.events) map (_._2)

      val eventsInOneBlock: Map[SpecificEventId, Some[Event]] = TreeMap(
        // Use 'TreeMap' as it won't rearrange the key ordering. We do this purely
        // to ensure that events that occur at the same time won't be shuffled as
        // they go into the map wrt the expected histories.
        events.zipWithIndex map {
          case (event, index) => index -> Some(event)
        }: _*)

      val itemCacheFromBlockBooking =
        emptyTimeline()
          .revise(eventsInOneBlock)
          .itemCacheAt(queryWhen)

      val random = new Random(seed)

      val severalRevisionBookingsWithObsoleteEventsThrownIn
        : List[Map[SpecificEventId, Option[Event]]] =
        intersperseObsoleteEvents(random, events, obsoleteEvents) map (
            booking => TreeMap(booking.map(_.swap): _*)) toList

      val timelineResultingFromIncrementalBookings =
        ((emptyTimeline(): Timeline) /: severalRevisionBookingsWithObsoleteEventsThrownIn) {
          case (timeline, booking) =>
            timeline.revise(booking)
        }

      val itemCacheFromIncrementalBookings =
        timelineResultingFromIncrementalBookings.itemCacheAt(queryWhen)

      val checks = for {
        RecordingsNoLaterThan(historyId, historiesFrom, _, _, _) <- recordingsGroupedById flatMap (_.thePartNoLaterThan(
          queryWhen))
        Seq(historyFromBlockBooking) = historiesFrom(itemCacheFromBlockBooking)
        Seq(historyFromIncrementalBookings) = historiesFrom(
          itemCacheFromIncrementalBookings)
      } yield
        (historyId,
         historyFromIncrementalBookings.datums,
         historyFromBlockBooking.datums)

      Inspectors.forAll(checks) {
        case (historyId, actualDatums, expectedDatums) =>
          try {
            actualDatums should contain theSameElementsInOrderAs expectedDatums
          } catch {
            case testFailedException: TestFailedException =>
              throw testFailedException.modifyMessage(_.map(message =>
                s"for id: $historyId at: $queryWhen, $message"))
          }
      }
    }
  }
}
