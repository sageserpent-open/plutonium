package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded

/**
  * Created by gerardMurphy on 11/05/2017.
  */
trait Timeline[+EventId] {
  // TODO - somehow the notion of an 'ItemStateStorage' is going to be added to this class, probably as a field.
  // How should this be reflected in the public API? What does a client want to do? Make a scope, perhaps? Is this
  // going to turn into a purely functional version of the 'World' API?
  def revise[NewEventId >: EventId](
      events: Map[NewEventId, Option[Event]]): Timeline[NewEventId]

  def retainUpTo(when: Unbounded[Instant]): Timeline[EventId]
}

object emptyTimeline extends Timeline[Nothing] {
  override def revise[NewEventId](
      events: Map[NewEventId, Option[Event]]): Timeline[NewEventId] = ???

  override def retainUpTo(when: Unbounded[Instant]): Timeline[Nothing] = this
}
