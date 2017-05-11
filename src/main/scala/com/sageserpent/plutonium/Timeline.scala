package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded

/**
  * Created by gerardMurphy on 11/05/2017.
  */
trait Timeline[+EventId] {
  // These can be in any order, as they are just fed to a builder.
  trait ItemStateSnapshotBookings
      extends collection.immutable.Seq[
        ItemStateSnapshotBooking[EventId, _ <: Identified]]

  def revise[NewEventId >: EventId](events: Map[NewEventId, Option[Event]])
    : (Timeline[NewEventId], ItemStateSnapshotBookings)

  def retainUpTo(when: Unbounded[Instant]): Timeline[EventId]
}

object emptyTimeline extends Timeline[Nothing] {
  override def revise[NewEventId](events: Map[NewEventId, Option[Event]])
    : (Timeline[NewEventId], ItemStateSnapshotBookings) = ???

  override def retainUpTo(when: Unbounded[Instant]): Timeline[Nothing] = this
}

case class ItemStateSnapshotBooking[+EventId, Item <: Identified](
    eventId: EventId,
    id: Item#Id,
    when: Instant,
    snapshot: ItemStateSnapshot)
