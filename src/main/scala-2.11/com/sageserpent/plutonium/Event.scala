package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.infrastructure.{NegativeInfinity, Finite, PositiveInfinity, Unbounded}

import scala.spores._

import scala.reflect.runtime.universe._

/**
 * Created by Gerard on 09/07/2015.
 */

// NOTE: if 'when' is 'NegativeInfinity', the event is taken to be 'at the beginning of time' - this is a way of introducing
// timeless events, although it permits following events to modify the outcome, which may be quite handy. For now, there is
// no such corresponding use for 'PositiveInfinity' - that results in a precondition failure.
sealed abstract class Event {
  val when: Unbounded[Instant]
  require(when < PositiveInfinity())
}


// The idea is that 'update' will set public properties and call public methods on bitemporals fetched from the scope.
// NOTE: the scope is 'writeable' - raw values that it renders from bitemporals can be mutated. One can call any public
// property or method, be it getter or setter, unit- or value-returning. The crux is that only top-level calls to
// public property setters and public unit returning methods are patched.
// NOTE: the scope initially represents the state of the world when the event is to be applied, but *without* the event having been
// applied yet - so all previous history will have taken place.
case class Change(val when: Unbounded[Instant], update: Spore[com.sageserpent.plutonium.Scope, Unit]) extends Event {

}

object Change {
  def apply[Raw <: Identified: TypeTag](when: Unbounded[Instant])(id: Raw#Id, update: Spore[Raw, Unit]): Change = {
    Change(when, spore {
      val bitemporal = Bitemporal.singleOneOf(id)
      (scope: com.sageserpent.plutonium.Scope) => {
        val raws = scope.render(bitemporal)
        capture(update)(raws.head)
      }
    })
  }

  def apply[Raw <: Identified: TypeTag](when: Instant)(id: Raw#Id, update: Spore[Raw, Unit]): Change = apply(Finite(when))(id, update)

  def apply[Raw <: Identified: TypeTag](id: Raw#Id, update: Spore[Raw, Unit]): Change = apply(NegativeInfinity[Instant]())(id, update)

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
case class Observation(definiteWhen: Instant, recording: Spore[com.sageserpent.plutonium.Scope, Unit]) extends Event {
  val when = Finite(definiteWhen)
}


object Observation {
  def apply[Raw <: Identified: TypeTag](definiteWhen: Instant)(id: Raw#Id, recording: Spore[Raw, Unit]): Observation = {
    Observation(definiteWhen, spore {
      val bitemporal = Bitemporal.singleOneOf(capture(id))
      (scope: com.sageserpent.plutonium.Scope) => {
        val raws = scope.render(bitemporal)
        capture(recording)(raws.head)
      }
    })
  }

  // etc for multiple bitemporals....
}


// NOTE: creation is implied by the first change or observation, so we don't bother with an explicit case class for that.
// NOTE: annihilation has to happen at some definite time.
case class Annihilation[Raw <: Identified](definiteWhen: Instant, id: Raw#Id) extends Event {
  val when = Finite(definiteWhen)
}
