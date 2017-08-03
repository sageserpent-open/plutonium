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
    def isValid(validRevisionFor: EventId => Revision): Boolean
    def whenCreated(validRevisionFor: EventId => Revision): Unbounded[Instant]
    def whenAnnihilated(validRevisionFor: EventId => Revision): Option[Instant] // The absence of a creation time means that lifecycle goes on forever - that *includes* 'PositiveInfinity'.
    def snapshotBlobFor(when: Unbounded[Instant],
                        validRevisionFor: EventId => Revision): SnapshotBlob

    def addSnapshotBlob(eventId: EventId,
                        when: Unbounded[Instant],
                        snapshotBlob: SnapshotBlob): Lifecycle[EventId]
  }

  trait LifecycleContracts[EventId] extends Lifecycle[EventId] {
    abstract override def whenCreated(
        validRevisionFor: EventId => Revision): Unbounded[Instant] = {
      require(isValid(validRevisionFor))
      super.whenCreated(validRevisionFor)
    }

    abstract override def whenAnnihilated(
        validRevisionFor: EventId => Revision): Option[Instant] = {
      require(isValid(validRevisionFor))
      super.whenAnnihilated(validRevisionFor)
    }

    abstract override def snapshotBlobFor(
        when: Unbounded[Instant],
        validRevisionFor: EventId => Revision): SnapshotBlob = {
      require(isValid(validRevisionFor))
      require(whenCreated(validRevisionFor) <= when)
      require(
        whenAnnihilated(validRevisionFor) map (Finite(_) < when) getOrElse true)
      super.snapshotBlobFor(when, validRevisionFor)
    }

    abstract override def addSnapshotBlob(
        eventId: EventId,
        when: Unbounded[Instant],
        snapshotBlob: SnapshotBlob): Lifecycle[EventId] = {
      // TODO - contract here?
      super.addSnapshotBlob(eventId, when, snapshotBlob)
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

// More importantly, what about filtering out annulled snapshots - what I've done here is broken.

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
                .isValid(eventRevisions.apply) && lifecycle.whenCreated(
                eventRevisions.apply) <= when && lifecycle
                .whenAnnihilated(eventRevisions.apply)
                .fold(true)(Finite(_) > when)
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
                .isValid(eventRevisions.apply) && lifecycle.whenCreated(
                eventRevisions.apply) <= when && lifecycle
                .whenAnnihilated(eventRevisions.apply)
                .fold(true)(Finite(_) > when)
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

      val events = mutable.Set.empty[Event]

      override def annulEvent(eventId: EventId): Unit = {
        events += (eventId -> None)
      }

      override def recordSnapshotBlobsForEvent(
          eventId: EventId,
          when: Unbounded[Instant],
          snapshotBlobs: Seq[(UniqueItemSpecification[_ <: Identified],
                              SnapshotBlob)]): Unit = {
        events += eventId -> Some(when -> snapshotBlobs)
      }

      override def build(): BlobStorage[EventId] = {
        val newRevision = 1 + thisBlobStorage.revision
        val newEventRevisions =
          (thisBlobStorage.eventRevisions /: events) {
            case (eventRevisions, (eventId, _)) =>
              eventRevisions + (eventId -> newRevision)
          }

        case class LifecycleImplementation(
            // TODO - this implementation is horrible, what with all the 'toSeq' calls.
            snapshotBlobs: SortedMap[Unbounded[Instant],
                                     (SnapshotBlob, EventId, Revision)] =
              SortedMap.empty)
            extends BlobStorageInMemory.Lifecycle[EventId] {
          override def isValid(validRevisionFor: EventId => Revision): Boolean =
            snapshotBlobs.iterator.filter {
              case (_, (_, eventId, blobRevision)) =>
                blobRevision == validRevisionFor(eventId)
              case _ => false
            }.hasNext

          override def whenCreated(
              validRevisionFor: EventId => Revision): Unbounded[Instant] =
            snapshotBlobs.iterator
              .collect {
                case (when, (_, eventId, blobRevision))
                    if blobRevision == validRevisionFor(eventId) =>
                  when
              }
              .next()

          override def whenAnnihilated(
              validRevisionFor: EventId => Revision): Option[Instant] =
            snapshotBlobs.toSeq.reverseIterator
              .collect {
                case (Finite(when), (_, eventId, blobRevision))
                    if blobRevision == validRevisionFor(eventId) =>
                  when
              }
              .find(_ => true)

          override def snapshotBlobFor(
              when: Unbounded[Instant],
              validRevisionFor: EventId => Revision): SnapshotBlob = {
            snapshotBlobs
              .to(when)
              .toSeq
              .reverseIterator
              .collect {
                case (_, (snapshot, eventId, blobRevision))
                    if blobRevision == validRevisionFor(eventId) =>
                  snapshot
              }
              .next()
          }

          override def addSnapshotBlob(eventId: EventId,
                                       when: Unbounded[Instant],
                                       snapshotBlob: SnapshotBlob)
            : BlobStorageInMemory.Lifecycle[EventId] = {
            copy(
              snapshotBlobs = snapshotBlobs
                .updated(when, (snapshotBlob, eventId, newRevision)))
          }
        }

        val newLifecycles = (thisBlobStorage.lifecycles /: events) {
          case (lifecycles, (eventId, None)) =>
            lifecycles
          case (lifecycles, (eventId, Some((when, snapshots)))) =>
            (lifecycles /: snapshots) {
              case (lifecycles, (uniqueItemSpecification, snapshot)) =>
                val lifecycle = lifecycles.getOrElse(
                  uniqueItemSpecification,
                  new LifecycleImplementation
                  with BlobStorageInMemory.LifecycleContracts[EventId])
                lifecycles.updated(
                  uniqueItemSpecification,
                  lifecycle.addSnapshotBlob(eventId, when, snapshot))
            }
        }

        thisBlobStorage.copy(revision = newRevision,
                             eventRevisions = newEventRevisions,
                             lifecycles = newLifecycles)
      }
    }
}
