package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium
import com.sageserpent.americium.{Finite, PositiveInfinity, Unbounded}

import scala.reflect.runtime.universe._
import scala.spores._

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
case class Change(val when: Unbounded[Instant], update: Spore[RecorderFactory, Unit]) extends Event {
}

object Change {
  def apply[Raw <: Identified : TypeTag](when: Unbounded[Instant])(id: Raw#Id, update: Spore[Raw, Unit]): Change = {
    Change(when, spore {
      (recorderFactory: RecorderFactory) => {
        val recorder = recorderFactory(capture(id))
        capture(update)(recorder)
      }
    })
  }

  def apply[Raw <: Identified : TypeTag](when: Instant)(id: Raw#Id, update: Spore[Raw, Unit]): Change = apply(Finite(when))(id, update)

  def apply[Raw <: Identified : TypeTag](id: Raw#Id, update: Spore[Raw, Unit]): Change = apply(americium.NegativeInfinity[Instant]())(id, update)

  // etc for multiple bitemporals....
}


// The idea is that 'reading' will set public properties and call public methods on bitemporals fetched from the scope.
// NOTE: the scope is 'writeable' - raw values that it renders from bitemporals can be mutated. In contrast to the situation
// with 'Change', in an 'Measurement' the only interaction with the raw value is via setting public properties or calling
// public unit-returning methods from client code - and that only the top-level calls are recorded as patches, any nested calls made within
// the execution of a top-level invocation are not recorded (actually the code isn't executed at all). Any attempt to call public property
// getters, or public value-returning methods will result in an exception being thrown.
// NOTE: the scope is a synthetic one that has no prior history applied it to whatsoever - it is there purely to capture the effects
// of the recording.
case class Measurement(val when: Unbounded[Instant], reading: Spore[RecorderFactory, Unit]) extends Event {
}


object Measurement {
  def apply[Raw <: Identified : TypeTag](when: Unbounded[Instant])(id: Raw#Id, measurement: Spore[Raw, Unit]): Measurement = {
    Measurement(when, spore {
      (recorderFactory: RecorderFactory) => {
        val recorder = recorderFactory(capture(id))
        capture(measurement)(recorder)
      }
    })
  }

  def apply[Raw <: Identified : TypeTag](when: Instant)(id: Raw#Id, update: Spore[Raw, Unit]): Measurement = apply(Finite(when))(id, update)

  def apply[Raw <: Identified : TypeTag](id: Raw#Id, update: Spore[Raw, Unit]): Measurement = apply(americium.NegativeInfinity[Instant]())(id, update)

  // etc for multiple bitemporals....
}


// NOTE: creation is implied by the first change or measurement, so we don't bother with an explicit case class for that.
// NOTE: annihilation has to happen at some definite time.
// NOTE: an annihilation can only be booked in as part of a revision if the id is refers has already been defined by some
// earlier event and is not already annihilated - this is checked as a precondition on 'World.revise'.
// NOTE: it is OK to have annihilations and other events occurring at the same time: the documentation of 'World.revise'
// covers how coincident events are resolved. So an item referred to by an id may be changed, then annihilated, then
// recreated and so on all at the same time.
// TODO: currently it is possible to annihilate multiple items sharing the same id - simply use a common upper bound type.
// Should this be allowed? There aren't any tests to say one way or another, are there?
case class Annihilation[Raw <: Identified: TypeTag](definiteWhen: Instant, id: Raw#Id) extends Event {
  val when = Finite(definiteWhen)
  val typeTag = implicitly[TypeTag[Raw]]
}

