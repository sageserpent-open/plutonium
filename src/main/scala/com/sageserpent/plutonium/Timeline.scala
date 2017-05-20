package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded

import scala.reflect.runtime.universe

/**
  * Created by gerardMurphy on 11/05/2017.
  */
trait Timeline[+EventId] {
  def revise[NewEventId >: EventId](
      events: Map[NewEventId, Option[Event]]): Timeline[NewEventId]

  def retainUpTo(when: Unbounded[Instant]): Timeline[EventId]

  def itemCacheAt(when: Unbounded[Instant]): ItemCache
}

object emptyTimeline extends Timeline[Nothing] {
  override def revise[NewEventId](
      events: Map[NewEventId, Option[Event]]): Timeline[NewEventId] = ???

  override def retainUpTo(when: Unbounded[Instant]): Timeline[Nothing] = this

  override def itemCacheAt(when: Unbounded[Instant]): ItemCache =
    new ItemCache {
      override def allItems[Item <: Identified: universe.TypeTag]()
        : Stream[Item] = Stream.empty

      override def itemsFor[Item <: Identified: universe.TypeTag](
          id: Item#Id): Stream[Item] = Stream.empty
    }
}
