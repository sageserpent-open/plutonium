package com.sageserpent.plutonium

import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification

import scala.reflect.runtime.universe.TypeTag

object BlobStorage {
  trait SnapshotRetrievalApi[SnapshotBlob] {
    def snapshotBlobFor(
        uniqueItemSpecification: UniqueItemSpecification): Option[SnapshotBlob]
  }

  trait Timeslice[SnapshotBlob] extends SnapshotRetrievalApi[SnapshotBlob] {
    def uniqueItemQueriesFor[Item: TypeTag]: Stream[UniqueItemSpecification]
    def uniqueItemQueriesFor[Item: TypeTag](
        id: Any): Stream[UniqueItemSpecification]
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

trait BlobStorage[Time, RecordingId, SnapshotBlob] { blobStorage =>

  import BlobStorage._

  implicit val timeOrdering: Ordering[Time]

  trait RevisionBuilder {
    // NOTE: the unique item specification must be exact and consistent for all of an item's snapshots. This implies that snapshots from a previous revision may have to be rewritten
    // if an item's greatest lower bound type changes.
    def record(
        key: RecordingId,
        when: Time,
        snapshotBlobs: Map[UniqueItemSpecification, Option[SnapshotBlob]]): Unit

    def annul(key: RecordingId): Unit =
      record(key, null.asInstanceOf[Time], Map.empty)

    def build(): BlobStorage[Time, RecordingId, SnapshotBlob]

  }

  def openRevision(): RevisionBuilder

  def timeSlice(when: Time, inclusive: Boolean = true): Timeslice[SnapshotBlob]

  def retainUpTo(when: Time): BlobStorage[Time, RecordingId, SnapshotBlob]

}
