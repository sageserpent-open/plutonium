package com.sageserpent.plutonium

import com.sageserpent.plutonium.LifecyclesState.Dependencies

import scala.collection.immutable.Map

object LifecyclesState {
  type Dependencies
}

trait LifecyclesState[EventId] {

  def revise[Input, Output](events: Map[EventId, Option[Event]],
                            updatePlanConsumer: (
                                Input,
                                UpdatePlan[EventId]) => (Dependencies, Output))
    : (LifecyclesState[EventId], Output)
}

object noLifecyclesState {
  def apply[EventId]: LifecyclesState[EventId] = ???
}
