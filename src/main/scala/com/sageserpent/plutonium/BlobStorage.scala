package com.sageserpent.plutonium

object BlobStorage {
  trait SnapshotRetrievalApi[SnapshotBlob] {
    def snapshotBlobFor(
        uniqueItemSpecification: UniqueItemSpecification): Option[SnapshotBlob]
  }

  trait Timeslice[SnapshotBlob] extends SnapshotRetrievalApi[SnapshotBlob] {
    def uniqueItemQueriesFor[Item](
        clazz: Class[Item]): Stream[UniqueItemSpecification]
    def uniqueItemQueriesFor[Item](
        uniqueItemSpecification: UniqueItemSpecification)
      : Stream[UniqueItemSpecification]
  }

  trait TimesliceContracts[SnapshotBlob] extends Timeslice[SnapshotBlob] {
    abstract override def snapshotBlobFor(
        uniqueItemSpecification: UniqueItemSpecification)
      : Option[SnapshotBlob] = {
      val uniqueItemSpecifications = uniqueItemQueriesFor(
        uniqueItemSpecification)
      require(
        1 >= uniqueItemSpecifications.size,
        s"The item specification: '$uniqueItemSpecification', should pick out a unique item, these match it: ${uniqueItemSpecifications.toList}."
      )
      cumulativeBlobStorageFetches += 1
      super.snapshotBlobFor(uniqueItemSpecification)
    }
  }

  var cumulativeBlobStorageFetches: Long = 0
}

trait BlobStorage[Time, SnapshotBlob] { blobStorage =>

  import BlobStorage._

  implicit val timeOrdering: Ordering[Time]

  trait RevisionBuilder {
    // NOTE: the unique item specification must be exact and consistent for all of an item's snapshots. This implies that snapshots from a previous revision may have to be rewritten
    // if an item's greatest lower bound type changes.
    def record(
        when: Time,
        snapshotBlobs: Map[UniqueItemSpecification, Option[SnapshotBlob]]): Unit

    def annul(when: Time): Unit =
      record(when, Map.empty)

    def build(): BlobStorage[Time, SnapshotBlob]
  }

  trait RevisionBuilderContracts extends RevisionBuilder {
    protected def hasBooked(when: Time): Boolean

    abstract override def record(
        when: Time,
        snapshotBlobs: Map[UniqueItemSpecification, Option[SnapshotBlob]])
      : Unit = {
      require(!hasBooked(when))

      super.record(when, snapshotBlobs)

      assert(hasBooked(when))
    }

    abstract override def annul(when: Time): Unit = {
      require(!hasBooked(when))

      super.annul(when)

      assert(hasBooked(when))
    }
  }

  def openRevision(): RevisionBuilder

  def timeSlice(when: Time, inclusive: Boolean = true): Timeslice[SnapshotBlob]

  def retainUpTo(when: Time): BlobStorage[Time, SnapshotBlob]

}
