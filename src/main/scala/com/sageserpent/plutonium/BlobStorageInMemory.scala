package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{Finite, Unbounded}
import com.sageserpent.plutonium.BlobStorage.{
  SnapshotBlob,
  UniqueItemSpecification
}
import com.sageserpent.plutonium.BlobStorageInMemory.Revision

import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.reflect.runtime.universe._

object BlobStorageInMemory {
  type Revision = Int

  trait Lifecycle[EventId] {
    def isValid(when: Unbounded[Instant],
                validRevisionFor: EventId => Revision): Boolean
    def snapshotBlobFor(when: Unbounded[Instant],
                        validRevisionFor: EventId => Revision): SnapshotBlob

    def addSnapshotBlob(eventId: EventId,
                        when: Unbounded[Instant],
                        snapshotBlob: SnapshotBlob,
                        revision: Revision): Lifecycle[EventId]
  }

  trait LifecycleContracts[EventId] extends Lifecycle[EventId] {
    abstract override def snapshotBlobFor(
        when: Unbounded[Instant],
        validRevisionFor: EventId => Revision): SnapshotBlob = {
      require(isValid(when, validRevisionFor))
      super.snapshotBlobFor(when, validRevisionFor)
    }
  }

  case class LifecycleImplementation[EventId](
      // TODO - this implementation is horrible, what with all the 'toSeq' and 'reverseIterator' calls.
      snapshotBlobs: SortedMap[Unbounded[Instant],
                               (SnapshotBlob, EventId, Revision)] =
        SortedMap.empty[Unbounded[Instant], (SnapshotBlob, EventId, Revision)])
      extends BlobStorageInMemory.Lifecycle[EventId] {
    override def isValid(when: Unbounded[Instant],
                         validRevisionFor: EventId => Revision): Boolean =
      snapshotBlobs
        .to(when)
        .toSeq
        .reverseIterator
        .exists(PartialFunction.cond(_) {
          case (_, (_, eventId, blobRevision)) =>
            blobRevision == validRevisionFor(eventId)
        })

    override def snapshotBlobFor(
        when: Unbounded[Instant],
        validRevisionFor: EventId => Revision): SnapshotBlob = {
      snapshotBlobs
        .to(when)
        .toSeq
        .reverseIterator
        .find(PartialFunction.cond(_) {
          case (_, (_, eventId, blobRevision)) =>
            blobRevision == validRevisionFor(eventId)
        })
        .map { case (_, (snapshot, _, _)) => snapshot }
        .get
    }

    override def addSnapshotBlob(
        eventId: EventId,
        when: Unbounded[Instant],
        snapshotBlob: SnapshotBlob,
        revision: Revision): BlobStorageInMemory.Lifecycle[EventId] = {
      require(!snapshotBlobs.contains(when))
      new LifecycleImplementation(
        snapshotBlobs = snapshotBlobs
          .updated(when, (snapshotBlob, eventId, revision)))
      with BlobStorageInMemory.LifecycleContracts[EventId]
    }
  }

  def apply[EventId]() =
    new BlobStorageInMemory[EventId](
      revision = 0,
      eventRevisions = Map.empty[EventId, Revision],
      lifecycles =
        Map.empty[UniqueItemSpecification[_ <: Identified], Lifecycle[EventId]]
    )
}

case class BlobStorageInMemory[EventId] private (
    val revision: BlobStorageInMemory.Revision,
    val eventRevisions: Map[EventId, BlobStorageInMemory.Revision],
    val lifecycles: Map[UniqueItemSpecification[_ <: Identified],
                        BlobStorageInMemory.Lifecycle[EventId]])
    extends BlobStorage[EventId] {
  thisBlobStorage =>

  override def timeSlice(when: Unbounded[Instant]): Timeslice = {
    trait TimesliceImplementation extends Timeslice {
      override def uniqueItemQueriesFor[Item <: Identified: TypeTag]
        : Stream[UniqueItemSpecification[_ <: Item]] =
        lifecycles
          .filter {
            case ((_, itemTypeTag), lifecycle) =>
              itemTypeTag.tpe <:< typeTag[Item].tpe && lifecycle
                .isValid(when, eventRevisions.apply)
          }
          .keys
          .toStream
          .asInstanceOf[Stream[UniqueItemSpecification[_ <: Item]]]

      override def uniqueItemQueriesFor[Item <: Identified: TypeTag](
          id: Item#Id): Stream[UniqueItemSpecification[_ <: Item]] =
        lifecycles
          .filter {
            case ((itemId, itemTypeTag), lifecycle) =>
              itemId == id &&
                itemTypeTag.tpe <:< typeTag[Item].tpe && lifecycle
                .isValid(when, eventRevisions.apply)
          }
          .keys
          .toStream
          .asInstanceOf[Stream[UniqueItemSpecification[_ <: Item]]]

      override def snapshotBlobFor[Item <: Identified](
          uniqueItemSpecification: UniqueItemSpecification[Item])
        : SnapshotBlob =
        lifecycles(uniqueItemSpecification)
          .snapshotBlobFor(when, eventRevisions.apply)
    }

    new TimesliceImplementation with TimesliceContracts
  }

  override def openRevision[NewEventId >: EventId](): RevisionBuilder =
    new RevisionBuilder {
      type Event =
        (EventId,
         Option[
           (Unbounded[Instant],
            Seq[(UniqueItemSpecification[_ <: Identified], SnapshotBlob)])])

      val events = mutable.Set.empty[Event] // PARDON? A set - read on ....

      override def annulEvent(eventId: EventId): Unit = {
        events += (eventId -> None)
      }

      override def recordSnapshotBlobsForEvent(
          eventId: EventId,
          when: Unbounded[Instant],
          snapshotBlobs: Seq[(UniqueItemSpecification[_ <: Identified],
                              SnapshotBlob)]): Unit = {
        events += eventId -> Some(when -> snapshotBlobs) // ....what about this, then? Suppose a client makes multiple bookings under the same event id within the same revision?
        // Yes, and for that matter, suppose multiple bookings are made at the same time?
      }

      override def build(): BlobStorage[EventId] = {
        val newRevision = 1 + thisBlobStorage.revision

        // TODO - use ++= ....
        val newEventRevisions =
          (thisBlobStorage.eventRevisions /: events) {
            case (eventRevisions, (eventId, _)) =>
              eventRevisions + (eventId -> newRevision)
          }
        val newLifecycles = (thisBlobStorage.lifecycles /: events) {
          case (lifecycles, (eventId, None)) =>
            lifecycles
          case (lifecycles, (eventId, Some((when, snapshots)))) =>
            // TODO: try to add in bulk...
            (lifecycles /: snapshots) {
              case (lifecycles, (uniqueItemSpecification, snapshot)) =>
                val lifecycle = lifecycles.getOrElse(
                  uniqueItemSpecification,
                  new BlobStorageInMemory.LifecycleImplementation[EventId]
                  with BlobStorageInMemory.LifecycleContracts[EventId])
                lifecycles.updated(uniqueItemSpecification,
                                   lifecycle.addSnapshotBlob(eventId,
                                                             when,
                                                             snapshot,
                                                             newRevision))
            }
        }

        thisBlobStorage.copy(revision = newRevision,
                             eventRevisions = newEventRevisions,
                             lifecycles = newLifecycles)
      }
    }
}
