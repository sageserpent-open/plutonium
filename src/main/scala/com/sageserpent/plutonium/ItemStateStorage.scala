package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.TypeTag

/**
  * Created by gerardMurphy on 01/05/2017.
  */
trait ItemStateStorage[+EventId] {
  type UniqueItemSpecification[Item <: Identified] =
    (Item#Id, TypeTag[Item])

  type RevisionBuilder[EventIdForBuilding >: EventId] <: SimpleRevisionBuilder[
    EventIdForBuilding]

  trait SimpleRevisionBuilder[EventIdForBuilding >: EventId] {
    // Once this has been called, the receiver will throw precondition failures on subsequent use.
    def build(): ItemStateStorage[EventIdForBuilding]

    def recordSnapshot[Item <: Identified: TypeTag](
        eventId: EventIdForBuilding,
        item: Item,
        when: Instant)
  }

  def openRevision[NewEventId >: EventId](): RevisionBuilder[NewEventId]

  type ReconstitutionContext <: ItemCache

  def newContext(when: Unbounded[Instant]): ReconstitutionContext
}

trait ItemStateStorageViaSnapshots[+EventId]
    extends ItemStateStorage[EventId] {
  type Snapshot = Array[Byte]

  override type RevisionBuilder[EventIdForBuilding >: EventId] <: ExtendedRevisionBuilder[
    EventIdForBuilding]

  trait ExtendedRevisionBuilder[EventIdForBuilding >: EventId]
      extends SimpleRevisionBuilder[EventIdForBuilding] {
    def recordSnapshot[Item <: Identified](
        uniqueItemSpecification: UniqueItemSpecification[Item],
        snapshot: Snapshot): Unit
  }

  override type ReconstitutionContext <: ExtendedReconstitutionContext

  trait ExtendedReconstitutionContext extends ItemCache {
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
        uniqueItemSpecification: UniqueItemSpecification[Item]): Item

    protected def uniqueItemQueriesFor[Item <: Identified: TypeTag]
      : Stream[UniqueItemSpecification[RetrievedItem]] forSome {
        type RetrievedItem <: Item
      }
    protected def uniqueItemQueriesFor[Item <: Identified: TypeTag](
        id: Item#Id): Stream[UniqueItemSpecification[RetrievedItem]] forSome {
      type RetrievedItem <: Item
    }

    protected def snapshotFor[Item <: Identified](
        uniqueItemSpecification: UniqueItemSpecification[Item]): Snapshot
  }
}

object emptyItemStateStorage extends ItemStateStorage[Nothing] {
  override def openRevision[NewEventId](): RevisionBuilder[NewEventId] = ???

  override type ReconstitutionContext = ItemCache

  override def newContext(when: Unbounded[Instant]): ReconstitutionContext =
    emptyItemCache
}
