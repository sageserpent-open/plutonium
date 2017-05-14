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

  def idsFor[Item <: Identified: TypeTag]
    : Stream[(RetrievedItem#Id, TypeTag[RetrievedItem]) forSome {
      type RetrievedItem <: Item
    }]

  trait ReconstitutionContext {
    // This will go fetch a snapshot from somewhere - storage or whatever and self-populate if necessary.
    def itemsFor[Item <: Identified: TypeTag](id: Item#Id): Stream[Item]
  }

  def newContext(when: Unbounded[Instant]): ReconstitutionContext
}

object noItemStateSnapshots extends ItemStateSnapshotStorage[Nothing] {
  override def openRevision[NewEventId]()
    : ItemStateSnapshotRevisionBuilder[NewEventId] = ???

  override def idsFor[Item <: Identified: TypeTag]
    : Stream[(RetrievedItem#Id, universe.TypeTag[RetrievedItem]) forSome {
      type RetrievedItem <: Item
    }] = Stream.empty

  override def newContext(
      when: Unbounded[Instant]): noItemStateSnapshots.ReconstitutionContext =
    new ReconstitutionContext {
      override def itemsFor[Item <: Identified: TypeTag](
          id: Item#Id): Stream[Item] = Stream.empty
    }
}
