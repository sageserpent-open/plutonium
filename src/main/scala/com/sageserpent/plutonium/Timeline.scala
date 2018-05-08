package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded

import scala.collection.immutable.Map

trait Timeline {
  def revise(events: Map[_ <: EventId, Option[Event]]): Timeline

  def retainUpTo(when: Unbounded[Instant]): Timeline

  def itemCacheAt(when: Unbounded[Instant]): ItemCache
}

object emptyTimeline {
  def apply() =
    new TimelineImplementation
}
