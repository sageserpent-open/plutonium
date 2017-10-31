package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded

import scala.collection.immutable.Map

trait Timeline[EventId] {
  def revise[NewEventId >: EventId](
      events: Map[NewEventId, Option[Event]]): Timeline[NewEventId]

  def retainUpTo(when: Unbounded[Instant]): Timeline[EventId]

  def itemCacheAt(when: Unbounded[Instant]): ItemCache
}

// TODO - given that we have 'emptyItemCache', I'm not sure if we need this too - let's see how it pans out...
object emptyTimeline extends TimelineImplementation[Nothing]

class TimelineImplementation[EventId](events: Map[EventId, Event] = Map.empty)
    extends Timeline[EventId] {
  override def revise[NewEventId >: EventId](
      events: Map[NewEventId, Option[Event]]) = {

    new TimelineImplementation(
      events = (this.events
        .asInstanceOf[Map[NewEventId, Event]] /: events) {
        case (events, (eventId, Some(event))) => events + (eventId -> event)
        case (events, (eventId, None))        => events - eventId
      }
    )
  }

  override def retainUpTo(when: Unbounded[Instant]) =
    ??? // TODO - support experimental worlds.

  override def itemCacheAt(when: Unbounded[Instant]) = ???
}
