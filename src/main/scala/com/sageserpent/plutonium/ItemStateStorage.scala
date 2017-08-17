package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded

import scala.reflect.runtime.universe.TypeTag

trait ItemStateStorage[EventId] { itemStateStorage =>

  import BlobStorage._

  val blobStorage: BlobStorage[EventId]

  class RevisionBuilder(
      blobStorageRevisionBuilder: BlobStorage[EventId]#RevisionBuilder) {
    // TODO - I'm not sure if the client doing incremental event playback will know the type tag for each item
    // - if it does, let's rework the API to create a sub-builder that will record individual items with their type tags.
    // TODO - what does it mean if there are no items? A no-op?
    // TODO - annihilations - should they be handled by this method, could encode as a sequence of either values over items or unique item specifications?
    // NOTE: whatever takes over from 'PatchRecorder' should have come up with a greatest lower bound type for an item - so we assume in this API
    // that snapshots for the same item are always recorded with the same runtime type of object.
    def recordSnapshotsForEvent(eventId: EventId,
                                when: Unbounded[Instant],
                                items: Seq[_ <: Identified]): Unit = ???

    def annulEvent(eventId: EventId) =
      blobStorageRevisionBuilder.annulEvent(eventId)

    // Once this has been called, the receiver will throw precondition failures on subsequent use.
    def build(): ItemStateStorage[EventId] = ???
  }

  def openRevision(): RevisionBuilder = ???

  class ReconstitutionContext(
      blobStorageTimeslice: BlobStorage[EventId]#Timeslice)
      extends ItemCache {

    override def itemsFor[Item <: Identified: TypeTag](
        id: Item#Id): Stream[Item] =
      for {
        uniqueItemSpecification <- blobStorageTimeslice.uniqueItemQueriesFor(id)
      } yield itemFor(uniqueItemSpecification)

    override def allItems[Item <: Identified: TypeTag](): Stream[Item] =
      for {
        uniqueItemSpecification <- blobStorageTimeslice
          .uniqueItemQueriesFor[Item]
      } yield itemFor(uniqueItemSpecification)

    // This has a precondition that the type tag must pick out precisely one item - zero or multiple is not permitted.
    protected def itemFor[Item <: Identified](
        uniqueItemSpecification: UniqueItemSpecification[Item]): Item = ???
  }

  def newContext(when: Unbounded[java.time.Instant]): ReconstitutionContext =
    ???
}
