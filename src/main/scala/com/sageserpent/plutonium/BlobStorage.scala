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
}

trait BlobStorage[EventId] { blobStorage =>

  import BlobStorage._

  type SnapshotBlob = Array[Byte]

  trait RevisionBuilder {
    def recordSnapshotBlobsForEvent(
        when: Unbounded[Instant],
        snapshotBlobs: Seq[(UniqueItemSpecification[_], SnapshotBlob)]): Unit

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
