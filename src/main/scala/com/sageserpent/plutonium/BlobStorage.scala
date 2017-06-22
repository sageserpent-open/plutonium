package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded

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
    // if an items greatest lower bound type changes.
    def recordSnapshotBlobsForEvent(
        eventId: EventId,
        when: Unbounded[Instant],
        snapshotBlobs: Seq[(UniqueItemSpecification[_], SnapshotBlob)]): Unit

    def annulEvent(eventId: EventId) = ???

    // Once this has been called, the receiver will throw precondition failures on subsequent use.
    def build(): blobStorage.type
  }

  def timeSlice(when: Unbounded[Instant]): Timeslice

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
}
