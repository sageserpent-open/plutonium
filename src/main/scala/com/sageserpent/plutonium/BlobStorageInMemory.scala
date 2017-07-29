package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded
import com.sageserpent.plutonium.BlobStorage.{
  SnapshotBlob,
  UniqueItemSpecification
}

import scala.reflect.runtime.universe.TypeTag

class BlobStorageInMemory[EventId] extends BlobStorage[EventId] { nastyHack =>

  override def timeSlice(when: Unbounded[Instant]): Timeslice = new Timeslice {
    override def uniqueItemQueriesFor[Item <: Identified: TypeTag]
      : (Stream[(RetrievedItem#Id, TypeTag[RetrievedItem])]) forSome {
        type RetrievedItem <: Item
      } = ???

    override def uniqueItemQueriesFor[Item <: Identified: TypeTag](id: Item#Id)
      : (Stream[(RetrievedItem#Id, TypeTag[RetrievedItem])]) forSome {
        type RetrievedItem <: Item
      } = ???

    override def snapshotBlobFor[Item <: Identified](
        uniqueItemSpecification: (Item#Id, TypeTag[Item])): SnapshotBlob =
      Array.emptyByteArray
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
