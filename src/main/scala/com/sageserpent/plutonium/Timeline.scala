package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded

trait Timeline[+EventId] {
  def revise[NewEventId >: EventId](
      events: Map[NewEventId, Option[Event]]): Timeline[NewEventId]

  def retainUpTo(when: Unbounded[Instant]): Timeline[EventId]

  def itemCacheAt(when: Unbounded[Instant]): ItemCache
}

// TODO - given that we have 'emptyItemCache', I'm not sure if we need this too - let's see how it pans out...
object emptyTimeline extends Timeline[Nothing] {
  override def revise[NewEventId](
      events: Map[NewEventId, Option[Event]]): Timeline[NewEventId] = ???

  override def retainUpTo(when: Unbounded[Instant]): Timeline[Nothing] = this

  override def itemCacheAt(when: Unbounded[Instant]): ItemCache =
    emptyItemCache
}
