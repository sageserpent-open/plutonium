package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.BlobStorage.{
  SnapshotBlob,
  UniqueItemSpecification
}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.TypeTag

/**
  * Created by gerardMurphy on 21/05/2017.
  */
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
    def build(): blobStorage.type
  }

  def openRevision[NewEventId >: EventId](): RevisionBuilder

  trait Timeslice {
    def uniqueItemQueriesFor[Item <: Identified: TypeTag]
      : Stream[UniqueItemSpecification[RetrievedItem]] forSome {
        type RetrievedItem <: Item
      }
    def uniqueItemQueriesFor[Item <: Identified: TypeTag](
        id: Item#Id): Stream[UniqueItemSpecification[RetrievedItem]] forSome {
      type RetrievedItem <: Item
    }

    def snapshotBlobFor[Item <: Identified](
        uniqueItemSpecification: UniqueItemSpecification[Item]): SnapshotBlob
  }

  def timeSlice(when: Unbounded[Instant]): Timeslice
}

class BlobStorageInMemory[EventId] extends BlobStorage[EventId] { nastyHack =>

  override def timeSlice(when: Unbounded[Instant]): Timeslice = new Timeslice {
    override def uniqueItemQueriesFor[Item <: Identified: TypeTag]
      : (Stream[(RetrievedItem#Id, TypeTag[RetrievedItem])]) forSome {
        type RetrievedItem <: Item
      } = ???

    override def uniqueItemQueriesFor[Item <: Identified: TypeTag](
        id: Item#Id)
      : (Stream[(RetrievedItem#Id, TypeTag[RetrievedItem])]) forSome {
        type RetrievedItem <: Item
      } = ???

    override def snapshotBlobFor[Item <: Identified](
        uniqueItemSpecification: (Item#Id, TypeTag[Item]))
      : SnapshotBlob = Array.emptyByteArray
  }

  override def openRevision[NewEventId >: EventId](): RevisionBuilder =
    new RevisionBuilder {
      override def annulEvent(eventId: EventId): Unit = {}

      override def build(): BlobStorageInMemory.this.type = nastyHack

      override def recordSnapshotBlobsForEvent(
          eventId: EventId,
          when: Unbounded[Instant],
          snapshotBlobs: Seq[(UniqueItemSpecification[_ <: Identified],
                              SnapshotBlob)]): Unit = {}
    }
}
