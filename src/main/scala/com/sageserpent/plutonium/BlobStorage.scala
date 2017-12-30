package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag

object BlobStorage {
  // TODO - find a better home for this type alias.
  type UniqueItemSpecification =
    (Any, TypeTag[_ <: Any])

  type SnapshotBlob = Array[Byte]

  trait Timeslice {
    def uniqueItemQueriesFor[Item: TypeTag]: Stream[UniqueItemSpecification]
    def uniqueItemQueriesFor[Item: TypeTag](
        id: Any): Stream[UniqueItemSpecification]

    def snapshotBlobFor(
        uniqueItemSpecification: UniqueItemSpecification): Option[SnapshotBlob]
  }

  trait TimesliceContracts extends Timeslice {
    abstract override def snapshotBlobFor(
        uniqueItemSpecification: UniqueItemSpecification)
      : Option[SnapshotBlob] = {
      val uniqueItemSpecifications = uniqueItemQueriesFor(
        uniqueItemSpecification._1)(uniqueItemSpecification._2)
      require(
        1 >= uniqueItemSpecifications.size,
        s"The item specification: '$uniqueItemSpecification', should pick out a unique item, these match it: ${uniqueItemSpecifications.toList}."
      )
      super.snapshotBlobFor(uniqueItemSpecification)
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
        eventId: EventId,
        when: Unbounded[Instant],
        snapshotBlobs: Map[UniqueItemSpecification, Option[SnapshotBlob]]): Unit

    def annulEvent(eventId: EventId)

    // Once this has been called, the receiver will throw precondition failures on subsequent use.
    def build(): BlobStorage[EventId]
  }

  trait RevisionBuilderContracts extends RevisionBuilder {
    val eventIds = mutable.Set.empty[EventId]

    abstract override def recordSnapshotBlobsForEvent(
        eventId: EventId,
        when: Unbounded[Instant],
        snapshotBlobs: Map[UniqueItemSpecification, Option[SnapshotBlob]])
      : Unit = {
      require(!eventIds.contains(eventId))
      eventIds += eventId
      super.recordSnapshotBlobsForEvent(eventId, when, snapshotBlobs)
    }

    abstract override def annulEvent(eventId: EventId): Unit = {
      require(!eventIds.contains(eventId))
      eventIds += eventId
      super.annulEvent(eventId)
    }
  }

  def openRevision(): RevisionBuilder

  def timeSlice(when: Unbounded[Instant]): Timeslice
}
