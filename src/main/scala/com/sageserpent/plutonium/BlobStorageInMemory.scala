package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{Finite, Unbounded}
import com.sageserpent.plutonium.BlobStorage.{
  SnapshotBlob,
  UniqueItemSpecification
}

import scala.collection.mutable
import scala.reflect.runtime.universe._

object BlobStorageInMemory {
  type Revision = Int

  trait Lifecycle {
    def isValid: Boolean
    def whenCreated: Unbounded[Instant]
    def whenAnnihilated
      : Option[Instant] // The absence of a creation time means that lifecycle goes on forever - that *includes* 'PositiveInfinity'.
    def snapshotBlobFor(when: Unbounded[Instant],
                        revision: Revision): SnapshotBlob
  }

  trait LifecycleContracts extends Lifecycle {
    abstract override def whenCreated: Unbounded[Instant] = {
      require(isValid)
      super.whenCreated
    }

    abstract override def whenAnnihilated: Option[Instant] = {
      require(isValid)
      super.whenAnnihilated
    }

    abstract override def snapshotBlobFor(when: Unbounded[Instant],
                                          revision: Revision): SnapshotBlob = {
      require(isValid)
      require(whenCreated <= when)
      require(whenAnnihilated map (Finite(_) < when) getOrElse true)
      super.snapshotBlobFor(when, revision)
    }
  }

  def apply[EventId]() =
    new BlobStorageInMemory[EventId](
      revision = 0,
      eventVersions = Map.empty[EventId, BlobStorageInMemory.Revision],
      lifecycles = Map.empty[UniqueItemSpecification[_ <: Identified],
                             BlobStorageInMemory.Lifecycle]
    )
}

class BlobStorageInMemory[EventId] private (
    val revision: BlobStorageInMemory.Revision,
    val eventVersions: Map[EventId, BlobStorageInMemory.Revision],
    val lifecycles: Map[UniqueItemSpecification[_ <: Identified],
                        BlobStorageInMemory.Lifecycle])
    extends BlobStorage[EventId] {
  thisBlobStorage =>

  override def timeSlice(when: Unbounded[Instant]): Timeslice = {
    trait TimesliceImplementation extends Timeslice {
      override def uniqueItemQueriesFor[Item <: Identified: TypeTag]
        : Stream[UniqueItemSpecification[_ <: Item]] =
        lifecycles.keys
          .filter(_._2.tpe <:< typeTag[Item].tpe)
          .toStream
          .asInstanceOf[Stream[UniqueItemSpecification[_ <: Item]]]

      override def uniqueItemQueriesFor[Item <: Identified: TypeTag](
          id: Item#Id): Stream[UniqueItemSpecification[_ <: Item]] =
        lifecycles.keys
          .filter {
            case (itemId, itemTypeTag) =>
              itemId == id &&
                itemTypeTag.tpe <:< typeTag[Item].tpe
          }
          .toStream
          .asInstanceOf[Stream[UniqueItemSpecification[_ <: Item]]]

      override def snapshotBlobFor[Item <: Identified: TypeTag](
          uniqueItemSpecification: UniqueItemSpecification[Item])
        : SnapshotBlob =
        lifecycles(uniqueItemSpecification).snapshotBlobFor(when, revision)
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

      override def build(): BlobStorageInMemory.this.type = {
        thisBlobStorage
      }

      override def recordSnapshotBlobsForEvent(
          eventId: EventId,
          when: Unbounded[Instant],
          snapshotBlobs: Seq[(UniqueItemSpecification[_ <: Identified],
                              SnapshotBlob)]): Unit = {
        events += eventId -> Some(when -> snapshotBlobs)
      }
    }
}
