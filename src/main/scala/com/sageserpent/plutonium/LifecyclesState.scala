package com.sageserpent.plutonium

import scala.collection.immutable.Map

trait LifecyclesState[EventId] {

  def revise(events: Map[EventId, Option[Event]])
    : (LifecyclesState[EventId], UpdatePlan[EventId])
}

object noLifecyclesState {
  def apply[EventId]: LifecyclesState[EventId] = ???
}
