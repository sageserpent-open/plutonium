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

class TimelineImplementation[EventId](
    events: Map[EventId, Event] = Map.empty,
    itemStateStorage: ItemStateStorage[EventId] =
      new ItemStateStorage[EventId] {
        override val blobStorage: BlobStorage[EventId] =
          BlobStorageInMemory.apply[EventId]
      })
    extends Timeline[EventId] {
  override def revise[NewEventId >: EventId](
      events: Map[NewEventId, Option[Event]]) = {
    // TODO: use a modified patch recorder to build this up...
    val itemStateStorage =
      this.itemStateStorage.asInstanceOf[ItemStateStorage[NewEventId]]

    new TimelineImplementation(
      events = (this.events
        .asInstanceOf[Map[NewEventId, Event]] /: events) {
        case (events, (eventId, Some(event))) => events + (eventId -> event)
        case (events, (eventId, None))        => events - eventId
      },
      itemStateStorage
    )
  }

  override def retainUpTo(when: Unbounded[Instant]) =
    ??? // TODO - support experimental worlds.

  override def itemCacheAt(when: Unbounded[Instant]) = ???
}
