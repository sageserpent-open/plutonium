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


case class Change(val when: Unbounded[Instant], update: Spore[RecorderFactory, Unit]) extends Event {
}

object Change {
  def forOneItem[Raw <: Identified : TypeTag](when: Unbounded[Instant])(id: Raw#Id, update: Spore[Raw, Unit]): Change = {
    val typeTag = implicitly[TypeTag[Raw]]
    Change(when, spore {
      (recorderFactory: RecorderFactory) => {
        val recorder = recorderFactory(capture(id))(capture(typeTag))
        capture(update)(recorder)
      }
    })
  }

  def forOneItem[Raw <: Identified : TypeTag](when: Instant)(id: Raw#Id, update: Spore[Raw, Unit]): Change = forOneItem(Finite(when))(id, update)

  def forOneItem[Raw <: Identified : TypeTag](id: Raw#Id, update: Spore[Raw, Unit]): Change = forOneItem(americium.NegativeInfinity[Instant]())(id, update)

  def forTwoItems[Raw1 <: Identified : TypeTag, Raw2 <: Identified : TypeTag](when: Unbounded[Instant])(id1: Raw1#Id, id2: Raw2#Id, update: Spore2[Raw1, Raw2, Unit]): Change = {
    ???
  }

  def forOneItem[Raw1 <: Identified : TypeTag, Raw2 <: Identified : TypeTag](when: Instant)(id1: Raw1#Id, id2: Raw2#Id, update: Spore2[Raw1, Raw2, Unit]): Change = forTwoItems(Finite(when))(id1, id2, update)

  def forOneItem[Raw1 <: Identified : TypeTag, Raw2 <: Identified : TypeTag](id1: Raw1#Id, id2: Raw2#Id, update: Spore2[Raw1, Raw2, Unit]): Change = forTwoItems(americium.NegativeInfinity[Instant]())(id1, id2, update)
}

case class Measurement(val when: Unbounded[Instant], reading: Spore[RecorderFactory, Unit]) extends Event {
}


object Measurement {
  def forOneItem[Raw <: Identified : TypeTag](when: Unbounded[Instant])(id: Raw#Id, measurement: Spore[Raw, Unit]): Measurement = {
    val typeTag = implicitly[TypeTag[Raw]]
    Measurement(when, spore {
      (recorderFactory: RecorderFactory) => {
        val recorder = recorderFactory(capture(id))(capture(typeTag))
        capture(measurement)(recorder)
      }
    })
  }

  def forOneItem[Raw <: Identified : TypeTag](when: Instant)(id: Raw#Id, update: Spore[Raw, Unit]): Measurement = forOneItem(Finite(when))(id, update)

  def forOneItem[Raw <: Identified : TypeTag](id: Raw#Id, update: Spore[Raw, Unit]): Measurement = forOneItem(americium.NegativeInfinity[Instant]())(id, update)

  def forTwoItems[Raw1 <: Identified : TypeTag, Raw2 <: Identified : TypeTag](when: Unbounded[Instant])(id1: Raw1#Id, id2: Raw2#Id, update: Spore2[Raw1, Raw2, Unit]): Measurement = {
    ???
  }

  def forOneItem[Raw1 <: Identified : TypeTag, Raw2 <: Identified : TypeTag](when: Instant)(id1: Raw1#Id, id2: Raw2#Id, update: Spore2[Raw1, Raw2, Unit]): Measurement = forTwoItems(Finite(when))(id1, id2, update)

  def forOneItem[Raw1 <: Identified : TypeTag, Raw2 <: Identified : TypeTag](id1: Raw1#Id, id2: Raw2#Id, update: Spore2[Raw1, Raw2, Unit]): Measurement = forTwoItems(americium.NegativeInfinity[Instant]())(id1, id2, update)
}


// NOTE: creation is implied by the first change or measurement, so we don't bother with an explicit case class for that.
// NOTE: annihilation has to happen at some definite time.
// NOTE: an annihilation can only be booked in as part of a revision if the id is refers has already been defined by some
// earlier event and is not already annihilated - this is checked as a precondition on 'World.revise'.
// NOTE: it is OK to have annihilations and other events occurring at the same time: the documentation of 'World.revise'
// covers how coincident events are resolved. So an item referred to by an id may be changed, then annihilated, then
// recreated and so on all at the same time.
case class Annihilation[Raw <: Identified: TypeTag](definiteWhen: Instant, id: Raw#Id) extends Event {
  val when = Finite(definiteWhen)
  val capturedTypeTag = typeTag[Raw]
}

