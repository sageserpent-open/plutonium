package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.{PositiveInfinity, Unbounded}
import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification
import com.sageserpent.plutonium.Lifecycle.FusionResult

import scala.reflect.runtime.universe.TypeTag

object Lifecycle {
  val orderingByStartTime: Ordering[Lifecycle[_]] =
    Ordering.by[Lifecycle[_], Unbounded[Instant]](_.startTime)

  val orderingByEndTime: Ordering[Lifecycle[_]] =
    Ordering.by[Lifecycle[_], Unbounded[Instant]](
      _.endTime.getOrElse(PositiveInfinity()))

  trait PatchKind

  case object Change extends PatchKind

  case object Measurement extends PatchKind

  def apply[EventId](eventId: EventId,
                     when: Unbounded[Instant],
                     patch: AbstractPatch,
                     kind: PatchKind): Seq[Lifecycle[EventId]] = ???

  trait FusionResult[EventId]

  case class Split[EventId](first: Lifecycle[EventId],
                            second: Lifecycle[EventId])
      extends FusionResult[EventId] {
    require(first.endTime.fold(false)(_ < second.startTime))
  }

  case class Merge[EventId](merged: Lifecycle[EventId])
      extends FusionResult[EventId]
}

trait Lifecycle[EventId] {
  import Lifecycle.FusionResult

  val uniqueItemSpecification: UniqueItemSpecification

  val lowerBoundTypeTag = uniqueItemSpecification.typeTag

  val upperBoundTypeTag: TypeTag[_]

  require(lowerBoundTypeTag.tpe <:< upperBoundTypeTag.tpe)

  def isInconsistentWith(another: Lifecycle[EventId]): Boolean =
    (another.upperBoundTypeTag.tpe <:< this.upperBoundTypeTag.tpe || this.upperBoundTypeTag.tpe <:< another.upperBoundTypeTag.tpe) && !this
      .isFusibleWith(another)

  def isFusibleWith(another: Lifecycle[EventId]): Boolean =
    this.lowerBoundTypeTag.tpe <:< another.lowerBoundTypeTag.tpe || another.lowerBoundTypeTag.tpe <:< this.lowerBoundTypeTag.tpe

  def overlapsWith(another: Lifecycle[EventId]): Boolean =
    !(this.endTime.fold(false)(_ < another.startTime) || another.endTime.fold(
      false)(_ < this.startTime))

  val startTime: Unbounded[Instant]

  val endTime: Option[Unbounded[Instant]]

  def fuseWith(another: Lifecycle[EventId]): FusionResult[EventId]

  def annul(eventId: EventId): Lifecycle[EventId]
}

trait LifecycleContracts[EventId] extends Lifecycle[EventId] {
  require(endTime.fold(true)(startTime < _))
  require(lowerBoundTypeTag.tpe <:< upperBoundTypeTag.tpe)

  abstract override def fuseWith(
      another: Lifecycle[EventId]): FusionResult[EventId] = {
    require(
      this.uniqueItemSpecification.id == another.uniqueItemSpecification.id)
    require(isFusibleWith(another))
    require(!isInconsistentWith(another))
    require(overlapsWith(another))
    super.fuseWith(another)
  }
}

trait Lifecycles[EventId] {
  // TODO - how do we get root patches out to prime the update plan? Use a state transformer monad?
  // Where does the patch dag come into this, if at all? How do *new* root patches make their way into the dag?
  val uniqueItemSpecification: UniqueItemSpecification

  def annul(eventId: EventId): Lifecycles[EventId]

  def recordPatchFromChange(eventId: EventId,
                            when: Unbounded[Instant],
                            patch: AbstractPatch): Lifecycles[EventId]

  def recordPatchFromMeasurement(eventId: EventId,
                                 when: Unbounded[Instant],
                                 patch: AbstractPatch): Lifecycles[EventId]

  def recordAnnihilation(eventId: EventId,
                         annihilation: Annihilation): Lifecycles[EventId]
}

class LifecyclesImplementation[EventId] extends Lifecycles[EventId] {
  // TODO: need to store each lifecycle somewhere - how about in a map keyed by id, where each maplet refers to an ordered sequence of lifecycles for that id?
  // This implies an invariant - the lifecycles in a maplet must not conflict - so no overlaps are permitted, they should all be fused already.

  // TODO: yes, we can annul an event id from an individual lifecycle - but how do we get to that lifecycle? Do we need a second map from event id to lifecycle?
  // Is it a map or a multimap? Well, for one thing an event may generate more than one patch at the same physical time that refers to the same item - and not just
  // by alternating between target and argument references, we can repeat the same kind of reference. The other thing is that an annihilation can be sandwiched into
  // the middle of an event, so patches in the same event (and thus sharing the same event id) may refer to separate lifecycles. This also means that lifecycles may
  // indeed lap up to each other in physical time. Hmmm.

  override val uniqueItemSpecification: UniqueItemSpecification = ???

  override def annul(eventId: EventId): Lifecycles[EventId] = ???

  override def recordPatchFromChange(
      eventId: EventId,
      when: Unbounded[Instant],
      patch: AbstractPatch): Lifecycles[EventId] = ???

  override def recordPatchFromMeasurement(
      eventId: EventId,
      when: Unbounded[Instant],
      patch: AbstractPatch): Lifecycles[EventId] = ???

  override def recordAnnihilation(
      eventId: EventId,
      annihilation: Annihilation): Lifecycles[EventId] = ???
}
