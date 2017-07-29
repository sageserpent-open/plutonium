package com.sageserpent.plutonium

import scala.reflect.runtime.universe._
import scalaz.ApplicativePlus

sealed trait Bitemporal[Item] {
  def map[Item2] =
    implicitly[ApplicativePlus[Bitemporal]].map[Item, Item2](this) _

  def ap[Item2](stage: Bitemporal[Item => Item2]): Bitemporal[Item2] =
    ApBitemporalResult(preceedingContext = this, stage = stage)

  def plus(another: Bitemporal[Item]): Bitemporal[Item] =
    PlusBitemporalResult(lhs = this, rhs = another)
}

case class ApBitemporalResult[ContextRaw, Item](
    preceedingContext: Bitemporal[ContextRaw],
    stage: Bitemporal[ContextRaw => Item])
    extends Bitemporal[Item]

case class PlusBitemporalResult[Item](lhs: Bitemporal[Item],
                                      rhs: Bitemporal[Item])
    extends Bitemporal[Item]

case class PointBitemporalResult[Item](raw: Item) extends Bitemporal[Item]

case class NoneBitemporalResult[Item]() extends Bitemporal[Item]

case class IdentifiedItemsBitemporalResult[Item <: Identified: TypeTag](
    id: Item#Id)
    extends Bitemporal[Item] {
  val capturedTypeTag = typeTag[Item]
}

case class ZeroOrOneIdentifiedItemBitemporalResult[Item <: Identified: TypeTag](
    id: Item#Id)
    extends Bitemporal[Item] {
  val capturedTypeTag = typeTag[Item]
}

case class SingleIdentifiedItemBitemporalResult[Item <: Identified: TypeTag](
    id: Item#Id)
    extends Bitemporal[Item] {
  val capturedTypeTag = typeTag[Item]
}

case class WildcardBitemporalResult[Item <: Identified: TypeTag]()
    extends Bitemporal[Item] {
  val capturedTypeTag = typeTag[Item]
}

// This companion object can produce a bitemporal instance that refers to zero, one or many raw instances depending
// how many of those raw instances match the id or wildcard.
object Bitemporal {
  def apply[Item](raw: Item) = PointBitemporalResult(raw)

  def withId[Item <: Identified: TypeTag](id: Item#Id): Bitemporal[Item] =
    IdentifiedItemsBitemporalResult(id)

  def zeroOrOneOf[Item <: Identified: TypeTag](id: Item#Id): Bitemporal[Item] =
    ZeroOrOneIdentifiedItemBitemporalResult(id)

  def singleOneOf[Item <: Identified: TypeTag](id: Item#Id): Bitemporal[Item] =
    SingleIdentifiedItemBitemporalResult(id)

  def wildcard[Item <: Identified: TypeTag](): Bitemporal[Item] =
    WildcardBitemporalResult[Item]

  def none[Item]: Bitemporal[Item] = NoneBitemporalResult[Item]

  // TODO - a Bitemporal[Instant] that yields the query scope's 'when' from within the monad.

  // TODO - something that yields a bitemporal for the asOf and for the revision?

  // TODO - something that takes a bitemporal and then makes a time-shifted bitemporal from within the monad - for PnL.
}
