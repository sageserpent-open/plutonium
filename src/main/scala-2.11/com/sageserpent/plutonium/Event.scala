package com.sageserpent.plutonium

/**
 * Created by Gerard on 09/07/2015.
 */
sealed trait Event{

}


// The idea is that 'update' will set public properties and call public methods on bitemporals fetched from the scope.
case class Change(update: (Scope => Unit)) extends Event {
}

object Change{
  def apply[Raw <: Identified](id: Raw#Id, update: (Raw => Unit)): Change = {
    Change (scope => {
      val bitemporal = scope.render(Bitemporal.withId(id)).head
      update(bitemporal)
    })
  }
  // etc for multiple bitemporals....
}


// The idea is that 'recording' will set public properties and call public methods on bitemporals fetched from the scope.
case class Observation(recording: (Scope => Unit)) extends Event{
  // etc for multiple bitemporals....
}
object Observation{
  def apply[Raw <: Identified](id: Raw#Id, recording: (Raw => Unit)): Observation = {
    Observation (scope => {
      val bitemporal = scope.render(Bitemporal.withId(id)).head
      recording(bitemporal)
    })
  }
}


// NOTE: creation is implied by the first change or observation, so we don't bother with an explicit case class for that.
case class Annihilation[Raw <: Identified](id: Raw#Id) extends Event{
}
