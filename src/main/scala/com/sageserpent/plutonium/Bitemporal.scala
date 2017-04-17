package com.sageserpent.plutonium

/**
  * Created by Gerard on 09/07/2015.
  */
import scala.reflect.runtime.universe._
import scalaz.ApplicativePlus

sealed trait Bitemporal[Raw] {
  def map[Raw2] =
    implicitly[ApplicativePlus[Bitemporal]].map[Raw, Raw2](this) _

  def ap[Raw2](stage: Bitemporal[Raw => Raw2]): Bitemporal[Raw2] =
    ApBitemporalResult(preceedingContext = this, stage = stage)

  def plus(another: Bitemporal[Raw]): Bitemporal[Raw] =
    PlusBitemporalResult(lhs = this, rhs = another)
}

case class ApBitemporalResult[ContextRaw, Raw](
    preceedingContext: Bitemporal[ContextRaw],
    stage: Bitemporal[ContextRaw => Raw])
    extends Bitemporal[Raw]

case class PlusBitemporalResult[Raw](lhs: Bitemporal[Raw],
                                     rhs: Bitemporal[Raw])
    extends Bitemporal[Raw]

case class PointBitemporalResult[Raw](raw: Raw) extends Bitemporal[Raw]

case class NoneBitemporalResult[Raw]() extends Bitemporal[Raw]

case class IdentifiedItemsBitemporalResult[Raw <: Identified: TypeTag](
    id: Raw#Id)
    extends Bitemporal[Raw] {
  val capturedTypeTag = typeTag[Raw]
}

case class ZeroOrOneIdentifiedItemBitemporalResult[Raw <: Identified: TypeTag](
    id: Raw#Id)
    extends Bitemporal[Raw] {
  val capturedTypeTag = typeTag[Raw]
}

case class SingleIdentifiedItemBitemporalResult[Raw <: Identified: TypeTag](
    id: Raw#Id)
    extends Bitemporal[Raw] {
  val capturedTypeTag = typeTag[Raw]
}

case class WildcardBitemporalResult[Raw <: Identified: TypeTag]()
    extends Bitemporal[Raw] {
  val capturedTypeTag = typeTag[Raw]
}

// This companion object can produce a bitemporal instance that refers to zero, one or many raw instances depending
// how many of those raw instances match the id or wildcard.
object Bitemporal {
  def apply[Raw](raw: Raw) = PointBitemporalResult(raw)

  def withId[Raw <: Identified: TypeTag](id: Raw#Id): Bitemporal[Raw] =
    IdentifiedItemsBitemporalResult(id)

  def zeroOrOneOf[Raw <: Identified: TypeTag](id: Raw#Id): Bitemporal[Raw] =
    ZeroOrOneIdentifiedItemBitemporalResult(id)

  def singleOneOf[Raw <: Identified: TypeTag](id: Raw#Id): Bitemporal[Raw] =
    SingleIdentifiedItemBitemporalResult(id)

  def wildcard[Raw <: Identified: TypeTag](): Bitemporal[Raw] =
    WildcardBitemporalResult[Raw]

  def none[Raw]: Bitemporal[Raw] = NoneBitemporalResult[Raw]

  // TODO - a Bitemporal[Instant] that yields the query scope's 'when' from within the monad.

  // TODO - something that yields a bitemporal for the asOf and for the revision?

  // TODO - something that takes a bitemporal and then makes a time-shifted bitemporal from within the monad - for PnL.
}
