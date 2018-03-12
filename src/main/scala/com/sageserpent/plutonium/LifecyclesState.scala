package com.sageserpent.plutonium

import com.sageserpent.plutonium.LifecyclesState.Dependencies

import scala.collection.immutable.Map

object LifecyclesState {
  trait Dependencies

  object noDependencies extends Dependencies
}

trait LifecyclesState[EventId] {

  def revise[Output](
      events: Map[EventId, Option[Event]],
      updatePlanConsumer: (UpdatePlan[EventId]) => (Dependencies, Output))
    : (LifecyclesState[EventId], Output)
}

object noLifecyclesState {
  def apply[EventId](): LifecyclesState[EventId] = ???
}
