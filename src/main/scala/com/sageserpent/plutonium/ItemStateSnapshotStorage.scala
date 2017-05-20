package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.TypeTag

/**
  * Created by gerardMurphy on 01/05/2017.
  */
trait ItemStateSnapshotStorage[+EventId] {
  def openRevision[NewEventId >: EventId]()
    : ItemStateSnapshotRevisionBuilder[NewEventId]

  trait ReconstitutionContext extends ItemCache {
    type UniqueItemQuery[RetrievedItem <: Identified] =
      (RetrievedItem#Id, TypeTag[RetrievedItem])

    protected def uniqueItemQueriesFor[Item <: Identified: TypeTag]
      : Stream[UniqueItemQuery[RetrievedItem]] forSome {
        type RetrievedItem <: Item
      }
    protected def uniqueItemQueriesFor[Item <: Identified: TypeTag](
        id: Item#Id): Stream[UniqueItemQuery[RetrievedItem]] forSome {
      type RetrievedItem <: Item
    }

    override def itemsFor[Item <: Identified: TypeTag](
        id: Item#Id): Stream[Item] =
      for {
        query <- uniqueItemQueriesFor(id)
        item  <- itemFor(query)
      } yield item

    // This will go fetch a snapshot from somewhere - storage or whatever and self-populate if necessary.
    // This has a precondition that the type tag must pick out precisely one item - zero or multiple is not permitted.
    protected def itemFor[Item <: Identified: TypeTag](
        query: UniqueItemQuery[Item]): Stream[Item]

    override def allItems[Item <: Identified: TypeTag](): Stream[Item] =
      for {
        query <- uniqueItemQueriesFor[Item]
        item  <- itemFor(query)
      } yield item
  }

  def newContext(when: Unbounded[Instant]): ReconstitutionContext
}

object noItemStateSnapshots extends ItemStateSnapshotStorage[Nothing] {
  override def openRevision[NewEventId]()
    : ItemStateSnapshotRevisionBuilder[NewEventId] = ???

  override def newContext(
      when: Unbounded[Instant]): noItemStateSnapshots.ReconstitutionContext =
    new ReconstitutionContext {
      override protected def uniqueItemQueriesFor[
          Item <: Identified: universe.TypeTag]: (
        Stream[(RetrievedItem#Id, universe.TypeTag[RetrievedItem])]) forSome {
        type RetrievedItem <: Item
      } = Stream.empty

      override protected def uniqueItemQueriesFor[
          Item <: Identified: universe.TypeTag](id: Item#Id): (
        Stream[(RetrievedItem#Id, universe.TypeTag[RetrievedItem])]) forSome {
        type RetrievedItem <: Item
      } = Stream.empty

      override protected def itemFor[Item <: Identified: universe.TypeTag](
          query: (Item#Id, universe.TypeTag[Item])): Stream[Item] =
        Stream.empty
    }
}
