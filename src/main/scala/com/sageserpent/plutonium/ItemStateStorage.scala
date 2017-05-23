package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded

import scala.reflect.runtime.universe.TypeTag

/**
  * Created by gerardMurphy on 01/05/2017.
  */
trait ItemStateStorage[+EventId] {

  type RevisionBuilder[EventIdForBuilding >: EventId] <: SimpleRevisionBuilder[
    EventIdForBuilding]

  trait SimpleRevisionBuilder[EventIdForBuilding >: EventId] {

    def recordSnapshot[Item <: Identified: TypeTag](
        eventId: EventIdForBuilding,
        item: Item,
        when: Instant)
  }

  def openRevision[NewEventId >: EventId](): RevisionBuilder[NewEventId]

  type ReconstitutionContext <: ItemCache

  def newContext(when: Unbounded[Instant]): ReconstitutionContext
}

class ItemStateStorageUsingBlobs[+EventId] extends ItemStateStorage[EventId] {
  override type RevisionBuilder[EventIdForBuilding >: EventId] <: ExtendedRevisionBuilder[
    EventIdForBuilding]

  trait ExtendedRevisionBuilder[EventIdForBuilding >: EventId]
      extends SimpleRevisionBuilder[EventIdForBuilding] {
    self: BlobStorage#RevisionBuilder =>
  }

  override type ReconstitutionContext <: ExtendedReconstitutionContext

  class ExtendedReconstitutionContext extends ItemCache { self: BlobStorage =>

    override def itemsFor[Item <: Identified: TypeTag](
        id: Item#Id): Stream[Item] =
      for {
        uniqueItemSpecification <- uniqueItemQueriesFor(id)
      } yield itemFor(uniqueItemSpecification)

    override def allItems[Item <: Identified: TypeTag](): Stream[Item] =
      for {
        uniqueItemSpecification <- uniqueItemQueriesFor[Item]
      } yield itemFor(uniqueItemSpecification)

    // This has a precondition that the type tag must pick out precisely one item - zero or multiple is not permitted.
    protected def itemFor[Item <: Identified](
        uniqueItemSpecification: UniqueItemSpecification[Item]): Item = ???
  }

  def newContext(when: Unbounded[java.time.Instant]): ReconstitutionContext =
    ???
  def openRevision[NewEventId >: EventId](): RevisionBuilder[NewEventId] = ???
}

object emptyItemStateStorage extends ItemStateStorage[Nothing] {
  override def openRevision[NewEventId](): RevisionBuilder[NewEventId] = ???

  override type ReconstitutionContext = ItemCache

  override def newContext(when: Unbounded[Instant]): ReconstitutionContext =
    emptyItemCache
}
