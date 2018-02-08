package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded

import scala.collection.immutable.Map

trait Timeline[EventId] {
  def revise(events: Map[EventId, Option[Event]]): Timeline[EventId]

  def retainUpTo(when: Unbounded[Instant]): Timeline[EventId]

  def itemCacheAt(when: Unbounded[Instant]): ItemCache
}

// TODO - given that we have 'emptyItemCache', I'm not sure if we need this too - let's see how it pans out...
object emptyTimeline {
  def apply[EventId]() =
    new TimelineImplementation[EventId]
}
