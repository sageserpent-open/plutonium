package com.sageserpent.plutonium

import cats.Applicative

import scala.reflect.runtime.universe._

sealed trait Bitemporal[Item] {
  def map[Item2] =
    implicitly[Applicative[Bitemporal]].map[Item, Item2](this) _

  def ap[Item2](stage: Bitemporal[Item => Item2]): Bitemporal[Item2] =
    ApBitemporalResult(preceedingContext = this, stage = stage)

  def plus(another: Bitemporal[Item]): Bitemporal[Item] =
    PlusBitemporalResult(lhs = this, rhs = another)
}

case class ApBitemporalResult[ContextItem, Item](
    preceedingContext: Bitemporal[ContextItem],
    stage: Bitemporal[ContextItem => Item])
    extends Bitemporal[Item]

case class PlusBitemporalResult[Item](lhs: Bitemporal[Item],
                                      rhs: Bitemporal[Item])
    extends Bitemporal[Item]

case class PointBitemporalResult[Item](item: Item) extends Bitemporal[Item]

case class NoneBitemporalResult[Item]() extends Bitemporal[Item]

case class IdentifiedItemsBitemporalResult[Item](
    uniqueItemSpecification: UniqueItemSpecification)
    extends Bitemporal[Item]

case class WildcardBitemporalResult[Item](clazz: Class[Item])
    extends Bitemporal[Item]

// This companion object can produce a bitemporal instance that refers to zero, one or many items depending
// how many of those items match the id or wildcard.
object Bitemporal {
  def apply[Item](item: Item) = PointBitemporalResult(item)

  def withId[Item: TypeTag](id: Any): Bitemporal[Item] =
    IdentifiedItemsBitemporalResult(UniqueItemSpecification(id, typeOf[Item]))

  def wildcard[Item: TypeTag](): Bitemporal[Item] =
    WildcardBitemporalResult[Item](classFromType(typeOf[Item]))

  def none[Item]: Bitemporal[Item] = NoneBitemporalResult[Item]

  // TODO - a Bitemporal[Instant] that yields the query scope's 'when' from within the monad.

  // TODO - something that yields a bitemporal for the asOf and for the revision?

  // TODO - something that takes a bitemporal and then makes a time-shifted bitemporal from within the monad - for PnL.
}
