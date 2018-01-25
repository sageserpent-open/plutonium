package com.sageserpent.plutonium

import java.time.Instant
import java.util.UUID

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification

import scala.reflect.runtime.universe.TypeTag

object BlobStorage {
  type SnapshotBlob = Array[Byte]

  trait Timeslice {
    def uniqueItemQueriesFor[Item: TypeTag]: Stream[UniqueItemSpecification]
    def uniqueItemQueriesFor[Item: TypeTag](
        id: Any): Stream[UniqueItemSpecification]

    def snapshotBlobFor(
        uniqueItemSpecification: UniqueItemSpecification): Option[SnapshotBlob]

    def snapshotBlobFor(uniqueItemSpecification: UniqueItemSpecification,
                        lifecycleUUID: UUID): SnapshotBlob
  }

  trait TimesliceContracts extends Timeslice {
    abstract override def snapshotBlobFor(
        uniqueItemSpecification: UniqueItemSpecification)
      : Option[SnapshotBlob] = {
      val uniqueItemSpecifications = uniqueItemQueriesFor(
        uniqueItemSpecification.id)(uniqueItemSpecification.typeTag)
      require(
        1 >= uniqueItemSpecifications.size,
        s"The item specification: '$uniqueItemSpecification', should pick out a unique item, these match it: ${uniqueItemSpecifications.toList}."
      )
      super.snapshotBlobFor(uniqueItemSpecification)
    }

    abstract override def snapshotBlobFor(
        uniqueItemSpecification: UniqueItemSpecification,
        lifecycleUUID: UUID): SnapshotBlob = {
      val uniqueItemSpecifications = uniqueItemQueriesFor(
        uniqueItemSpecification.id)(uniqueItemSpecification.typeTag)
      require(
        1 >= uniqueItemSpecifications.size,
        s"The item specification: '$uniqueItemSpecification', should pick out a unique item, these match it: ${uniqueItemSpecifications.toList}."
      )
      super.snapshotBlobFor(uniqueItemSpecification, lifecycleUUID)
    }
  }
}

trait BlobStorage[EventId] { blobStorage =>

  import BlobStorage._

  trait RevisionBuilder {
    // TODO - what does it mean if there are no snapshots? A no-op?
    // NOTE: the unique item specification must be exact and consistent for all of an item's snapshots. This implies that snapshots from a previous revision may have to be rewritten
    // if an item's greatest lower bound type changes.
    def recordSnapshotBlobsForEvent(
        eventIds: Set[EventId],
        when: Unbounded[Instant],
        snapshotBlobs: Map[UniqueItemSpecification,
                           Option[(SnapshotBlob, UUID)]]): Unit

    def annulEvent(eventId: EventId)

    def build(): BlobStorage[EventId]
  }

  def openRevision(): RevisionBuilder

  def timeSlice(when: Unbounded[Instant]): Timeslice
}
