package com.sageserpent.plutonium

import com.sageserpent.plutonium.ItemStateStorage.SnapshotBlob
import com.sageserpent.plutonium.LifecyclesState.noDependencies
import org.scalacheck.{ShrinkLowPriority => NoShrinking}
import org.scalatest.exceptions.TestFailedException
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Inspectors, Matchers}

import scala.collection.immutable
import scala.collection.immutable.TreeMap
import scala.util.Random

import scalaz.std.stream

object LifecyclesStateSpec {
  implicit class UpdatePlanEnhancement[EventId](
      val updatePlan: UpdatePlan[EventId])
      extends AnyVal {
    def supersededBy(
        supersedingUpdatePlan: UpdatePlan[EventId]): UpdatePlan[EventId] = {
      val newAndCorrectedEvents = supersedingUpdatePlan.updates.values
        .flatMap(_.flatMap(_._1))
        .toSet
      val eventsToKnockOut
        : Set[EventId] = supersedingUpdatePlan.annulments ++ newAndCorrectedEvents

      val (knockedOutUpdates, preservedUpdates) = updatePlan.updates.partition {
        case (_, itemStateUpdates) =>
          eventsToKnockOut
            .intersect(itemStateUpdates.map(_._1).reduce(_ union _))
            .nonEmpty
      }

      val eventsActuallyKnockedOut =
        knockedOutUpdates.values.flatMap(_.flatMap(_._1)).toSet

      val newAnnulments = (updatePlan.annulments diff eventsToKnockOut) ++ (supersedingUpdatePlan.annulments diff eventsActuallyKnockedOut)

      val newUpdates = preservedUpdates ++ supersedingUpdatePlan.updates

      UpdatePlan(annulments = newAnnulments, updates = newUpdates)
    }
  }
}

class LifecyclesStateSpec
    extends FlatSpec
    with Matchers
    with GeneratorDrivenPropertyChecks
    with NoShrinking
    with WorldSpecSupport {
  import LifecyclesStateSpec._

  type EventId = Int

  "Booking in events in one block revision" should "build a correct update plan" in {
    forAll(recordingsGroupedByIdGenerator(forbidAnnihilations = false),
           unboundedInstantGenerator) { (recordingsGroupedById, queryWhen) =>
      val events: Map[EventId, Some[Event]] = TreeMap(
        // Use 'TreeMap' as it won't rearrange the key ordering. We do this purely
        // to ensure that events that occur at the same time won't be shuffled as
        // they go into the map wrt the expected histories.
        (recordingsGroupedById.flatMap(_.events) map (_._2)).zipWithIndex map {
          case (event, index) => index -> Some(event)
        }: _*)

      def harvestUpdatePlan(updatePlan: UpdatePlan[EventId])
        : (LifecyclesState.Dependencies, ItemCache) = {
        updatePlan.annulments shouldBe empty

        val blobStorage =
          updatePlan(BlobStorageInMemory[EventId, SnapshotBlob]())
        val itemCache =
          new ItemCacheUsingBlobStorage[EventId](blobStorage, queryWhen)
        (noDependencies, itemCache) // TODO - shouldn't this work with arbitrary dependencies being returned? Shouldn't that be tested, then?
      }

      val itemCache: ItemCache =
        noLifecyclesState[EventId]().revise(events, harvestUpdatePlan)._2

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

  "Booking in events over several revisions with other events that are revised or annulled" should "result in the same update plan as booking in the events via a single block revision" in {
    forAll(
      recordingsGroupedByIdGenerator(forbidAnnihilations = false),
      nonConflictingRecordingsGroupedByIdGenerator.map(
        _.flatMap(_.events) map (_._2)),
      seedGenerator,
      unboundedInstantGenerator
    ) { (recordingsGroupedById, obsoleteEvents, seed, queryWhen) =>
      def harvestUpdatePlan(blobStorage: BlobStorage[EventId, SnapshotBlob])(
          updatePlan: UpdatePlan[EventId])
        : (LifecyclesState.Dependencies, BlobStorage[EventId, SnapshotBlob]) =
        noDependencies -> updatePlan(blobStorage)

      val events = recordingsGroupedById.flatMap(_.events) map (_._2)

      val eventsInOneBlock: Map[EventId, Some[Event]] = TreeMap(
        // Use 'TreeMap' as it won't rearrange the key ordering. We do this purely
        // to ensure that events that occur at the same time won't be shuffled as
        // they go into the map wrt the expected histories.
        events.zipWithIndex map {
          case (event, index) => index -> Some(event)
        }: _*)

      val blobStorageResultingFromBlockBooking
        : BlobStorage[EventId, SnapshotBlob] =
        noLifecyclesState[EventId]()
          .revise(
            eventsInOneBlock,
            harvestUpdatePlan(BlobStorageInMemory[EventId, SnapshotBlob]()))
          ._2

      val random = new Random(seed)

      val severalRevisionBookingsWithObsoleteEventsThrownIn
        : List[Map[EventId, Option[Event]]] =
        intersperseObsoleteEvents(random, events, obsoleteEvents) map (
            booking => TreeMap(booking.map(_.swap): _*)) toList

      val blobStorageResultingFromIncrementalBookings
        : BlobStorage[EventId, SnapshotBlob] =
        ((noLifecyclesState[EventId]() -> (BlobStorageInMemory[EventId,
                                                               SnapshotBlob](): BlobStorage[
          EventId,
          SnapshotBlob])) /: severalRevisionBookingsWithObsoleteEventsThrownIn) {
          case ((lifecyclesState, blobStorage), booking) =>
            lifecyclesState.revise(booking, harvestUpdatePlan(blobStorage))
        }._2

      val itemCacheFromBlockBooking =
        new ItemCacheUsingBlobStorage[EventId](
          blobStorageResultingFromBlockBooking,
          queryWhen)

      val itemCacheFromIncrementalBookings =
        new ItemCacheUsingBlobStorage[EventId](
          blobStorageResultingFromIncrementalBookings,
          queryWhen)

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
