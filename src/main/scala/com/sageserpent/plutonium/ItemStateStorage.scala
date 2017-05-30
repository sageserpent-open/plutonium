package com.sageserpent.plutonium

import com.sageserpent.americium.Unbounded

import scala.reflect.runtime.universe.TypeTag

/**
  * Created by gerardMurphy on 01/05/2017.
  */
trait ItemStateStorage[EventId] {
  val blobStorage: BlobStorage

  class RevisionBuilder[EventIdForBuilding >: EventId](
      blobStorageRevisionBuilder: BlobStorage#RevisionBuilder) {}

  class ReconstitutionContext(blobStorageTimeslice: BlobStorage#Timeslice)
      extends ItemCache {

    override def itemsFor[Item <: Identified: TypeTag](
        id: Item#Id): Stream[Item] =
      for {
        uniqueItemSpecification <- blobStorageTimeslice.uniqueItemQueriesFor(
          id)
      } yield itemFor(uniqueItemSpecification)

    override def allItems[Item <: Identified: TypeTag](): Stream[Item] =
      for {
        uniqueItemSpecification <- blobStorageTimeslice
          .uniqueItemQueriesFor[Item]
      } yield itemFor(uniqueItemSpecification)

    // This has a precondition that the type tag must pick out precisely one item - zero or multiple is not permitted.
    protected def itemFor[Item <: Identified](
        uniqueItemSpecification: BlobStorage#UniqueItemSpecification[Item])
      : Item = ???
  }

  def newContext(when: Unbounded[java.time.Instant]): ReconstitutionContext =
    ???
  def openRevision[NewEventId >: EventId](): RevisionBuilder[NewEventId] = ???
}
