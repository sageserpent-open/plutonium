package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded

import scala.reflect.runtime.universe.TypeTag

object BlobStorage {
  type UniqueItemSpecification[Item <: Identified] =
    (Item#Id, TypeTag[Item])

  type SnapshotBlob = Array[Byte]
}

trait BlobStorage[EventId] { blobStorage =>

  import BlobStorage._

  trait RevisionBuilder {
    // TODO - what does it mean if there are no snapshots? A no-op? Analogous to 'ItemStateStorage', we could use an optional value to encode both snapshots and annihilations...
    // NOTE: the unique item specification must be exact and consistent for all of an item's snapshots. This implies that snapshots from a previous revision may have to be rewritten
    // if an item's greatest lower bound type changes.
    def recordSnapshotBlobsForEvent(
        eventId: EventId,
        when: Unbounded[Instant],
        snapshotBlobs: Seq[(UniqueItemSpecification[_ <: Identified],
                            SnapshotBlob)]): Unit

    def annulEvent(eventId: EventId)

    // Once this has been called, the receiver will throw precondition failures on subsequent use.
    def build(): BlobStorage[EventId]
  }

  def openRevision[NewEventId >: EventId](): RevisionBuilder

  trait Timeslice {
    def uniqueItemQueriesFor[Item <: Identified: TypeTag]
      : Stream[UniqueItemSpecification[_ <: Item]]
    def uniqueItemQueriesFor[Item <: Identified: TypeTag](
        id: Item#Id): Stream[UniqueItemSpecification[_ <: Item]]

    def snapshotBlobFor[Item <: Identified](
        uniqueItemSpecification: UniqueItemSpecification[Item]): SnapshotBlob
  }

  trait TimesliceContracts extends Timeslice {
    abstract override def snapshotBlobFor[Item <: Identified](
        uniqueItemSpecification: UniqueItemSpecification[Item])
      : SnapshotBlob = {
      require(
        uniqueItemQueriesFor(uniqueItemSpecification._1)(
          uniqueItemSpecification._2).nonEmpty)
      super.snapshotBlobFor(uniqueItemSpecification)
    }
  }

  def timeSlice(when: Unbounded[Instant]): Timeslice
}
