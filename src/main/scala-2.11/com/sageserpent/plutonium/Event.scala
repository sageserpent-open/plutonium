package com.sageserpent.plutonium

/**
 * Created by Gerard on 09/07/2015.
 */
sealed trait Event{

}


// The idea is that 'update' will set public properties and call public methods on bitemporals fetched from the scope.
// NOTE: the scope is 'writeable' - raw values that it renders from bitemporals can be mutated. One can call any public
// property or method, be it getter or setter, unit- or value-returning. The crux is that only top-level calls to
// public property setters and public unit returning methods are patched.
//
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
// NOTE: the scope is 'writeable' - raw values that it renders from bitemporals can be mutated. In contrast to the situation
// with 'Change', in an 'Observation' the only interaction with the raw value is via setting public properties or calling
// public unit-returning methods from client code - and that only the top-level calls are recorded as patches, any nested calls made within
// the execution of a top-level invocation are not recorded. Any attempt to call public property getters, or public value-returning
// methods will result in an exception being thrown.
case class Observation(recording: (Scope => Unit)) extends Event{
}
object Observation{
  def apply[Raw <: Identified](id: Raw#Id, recording: (Raw => Unit)): Observation = {
    Observation (scope => {
      val bitemporal = scope.render(Bitemporal.withId(id)).head
      recording(bitemporal)
    })
  }

  // etc for multiple bitemporals....
}


// NOTE: creation is implied by the first change or observation, so we don't bother with an explicit case class for that.
case class Annihilation[Raw <: Identified](id: Raw#Id) extends Event{
}
