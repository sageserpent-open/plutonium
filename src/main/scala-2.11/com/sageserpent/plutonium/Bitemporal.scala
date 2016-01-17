package com.sageserpent.plutonium

/**
 * Created by Gerard on 09/07/2015.
 */

import scala.reflect.runtime.universe._
import scalaz.MonadPlus

trait Bitemporal[Raw] {
  def filter = implicitly[MonadPlus[Bitemporal]].filter[Raw](this) _

  def map[Raw2] = implicitly[MonadPlus[Bitemporal]].map[Raw, Raw2](this) _

  def flatMap[Raw2](stage: Raw => Bitemporal[Raw2]): Bitemporal[Raw2]

  def interpret(scope: Bitemporal.IdentifiedItemsScope): Stream[Raw]

  def join[Raw2 <: Raw](another: Bitemporal[Raw2]): Bitemporal[Raw]
}

// This companion object can produce a bitemporal instance that refers to zero, one or many raw instances depending
// how many of those raw instances match the id or wildcard.
object Bitemporal {

  trait IdentifiedItemsScope {
    def itemsFor[Raw <: Identified: TypeTag](id: Raw#Id): Stream[Raw]

    def allItems[Raw <: Identified: TypeTag](): Stream[Raw]
  }

  def apply[Raw](raw: Raw) = new DefaultBitemporalReferenceImplementation[Raw](raw)

  def withId[Raw <: Identified : TypeTag](id: Raw#Id): Bitemporal[Raw] = new IdentifiedItemsBitemporalReferenceImplementation(id)

  def zeroOrOneOf[Raw <: Identified : TypeTag](id: Raw#Id): Bitemporal[Raw] = new ZeroOrOneIdentifiedItemBitemporalReferenceImplementation(id)

  def singleOneOf[Raw <: Identified : TypeTag](id: Raw#Id): Bitemporal[Raw] = new SingleIdentifiedItemBitemporalReferenceImplementation(id)

  def wildcard[Raw <: Identified : TypeTag](): Bitemporal[Raw] = new WildcardBitemporalReferenceImplementation[Raw]

  def none[Raw]: Bitemporal[Raw] = new DefaultBitemporalReferenceImplementation[Raw]

  def numberOf[Raw <: Identified : TypeTag](id: Raw#Id): Bitemporal[Int] = ???  // TODO - this counts the items.

  // TODO - something that makes a Bitemporal[Instant] to provide a way of snooping into the scope's 'when' from within the monad.

  // TODO - something that yields a bitemporal for the asOf and for the revision?

  // TODO - something that takes a bitemporal and then makes a time-shifted bitemporal from within the monad - for PnL.
}








