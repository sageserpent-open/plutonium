package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded

import scala.reflect.runtime.universe.TypeTag

/**
  * Created by gerardMurphy on 21/05/2017.
  */
trait BlobStorage {
  type UniqueItemSpecification[Item <: Identified] =
    (Item#Id, TypeTag[Item])

  type SnapshotBlob = Array[Byte]

  trait RevisionBuilder {
    def recordSnapshotBlob[Item <: Identified](
        uniqueItemSpecification: UniqueItemSpecification[Item],
        snapshot: SnapshotBlob,
        when: Unbounded[Instant]): Unit

    // Once this has been called, the receiver will throw precondition failures on subsequent use.
    def build(): this.type
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
