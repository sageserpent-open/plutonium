package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification

import scala.reflect.runtime.universe.TypeTag

object BlobStorage {
  trait Timeslice[SnapshotBlob] {
    def uniqueItemQueriesFor[Item: TypeTag]: Stream[UniqueItemSpecification]
    def uniqueItemQueriesFor[Item: TypeTag](
        id: Any): Stream[UniqueItemSpecification]

    def snapshotBlobFor(
        uniqueItemSpecification: UniqueItemSpecification): Option[SnapshotBlob]
  }

  trait TimesliceContracts[SnapshotBlob] extends Timeslice[SnapshotBlob] {
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
  }
}

trait BlobStorage[SnapshotBlob] { blobStorage =>

  import BlobStorage._

  trait RevisionBuilder {
    // NOTE: the unique item specification must be exact and consistent for all of an item's snapshots. This implies that snapshots from a previous revision may have to be rewritten
    // if an item's greatest lower bound type changes.
    def recordSnapshotBlobsForEvent(
        eventIds: Set[_ <: EventId],
        when: Unbounded[Instant],
        snapshotBlobs: Map[UniqueItemSpecification, Option[SnapshotBlob]]): Unit

    def annulEvent(eventId: EventId) =
      recordSnapshotBlobsForEvent(Set(eventId), PositiveInfinity(), Map.empty)

    def build(): BlobStorage[SnapshotBlob]
  }

  def openRevision(): RevisionBuilder

  def timeSlice(when: Unbounded[Instant],
                inclusive: Boolean = true): Timeslice[SnapshotBlob]

  def retainUpTo(when: Unbounded[Instant]): BlobStorage[SnapshotBlob]
}
