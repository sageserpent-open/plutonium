package com.sageserpent.plutonium

import scala.spores._

/**
 * Created by Gerard on 09/07/2015.
 */
sealed trait Event {

}


// The idea is that 'update' will set public properties and call public methods on bitemporals fetched from the scope.
// NOTE: the scope is 'writeable' - raw values that it renders from bitemporals can be mutated. One can call any public
// property or method, be it getter or setter, unit- or value-returning. The crux is that only top-level calls to
// public property setters and public unit returning methods are patched.
// NOTE: the scope initially represents the state of the world when the event is to be applied, but *without* the event having been
// applied yet - so all previous history will have taken place.
case class Change(update: Spore[Scope, Unit]) extends Event {

}

object Change {
  def apply[Raw <: Identified](id: Raw#Id, update: Spore[Raw, Unit]): Change = {
    Change(spore {
      val bitemporal = Bitemporal.withId(id)
      (scope: Scope) => {
        val raws = scope.render(bitemporal)
        capture(update)(raws.head)
      }
    })
  }

  // etc for multiple bitemporals....
}


// The idea is that 'recording' will set public properties and call public methods on bitemporals fetched from the scope.
// NOTE: the scope is 'writeable' - raw values that it renders from bitemporals can be mutated. In contrast to the situation
// with 'Change', in an 'Observation' the only interaction with the raw value is via setting public properties or calling
// public unit-returning methods from client code - and that only the top-level calls are recorded as patches, any nested calls made within
// the execution of a top-level invocation are not recorded (actually the code isn't executed at all). Any attempt to call public property
// getters, or public value-returning methods will result in an exception being thrown.
// NOTE: the scope is synthetic one that has no prior history applied it to whatsoever - it is there purely to capture the effects
// of the recording.
case class Observation(recording: Spore[Scope, Unit]) extends Event {
}

object Observation {
  def apply[Raw <: Identified](id: Raw#Id, recording: Spore[Raw, Unit]): Observation = {
    Observation(spore {
      val bitemporal = Bitemporal.withId(capture(id))
      (scope: Scope) => {
        val raws = scope.render(bitemporal)
        capture(recording)(raws.head)
      }
    })
  }

  // etc for multiple bitemporals....
}


// NOTE: creation is implied by the first change or observation, so we don't bother with an explicit case class for that.
case class Annihilation[Raw <: Identified](id: Raw#Id) extends Event {
}
