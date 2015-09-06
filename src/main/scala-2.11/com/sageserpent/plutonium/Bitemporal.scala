package com.sageserpent.plutonium

/**
 * Created by Gerard on 09/07/2015.
 */

import scala.reflect.runtime.universe._

trait Bitemporal[Raw] {
  def filter(predicate: Raw => Boolean): Bitemporal[Raw]

  def map[Raw2](transform: Raw => Raw2): Bitemporal[Raw2]

  def flatMap[Raw2](stage: Raw => Bitemporal[Raw2]): Bitemporal[Raw2]

  def interpret(scope: Bitemporal.Scope): Stream[Raw]
}

// This companion object can produce a bitemporal instance that refers to zero, one or many raw instances depending
// how many of those raw instances match the id or wildcard.
object Bitemporal {

  trait Scope {
    def itemsFor[Raw <: Identified](id: Raw#Id): Stream[Raw]

    def allItems[Raw <: Identified](): Stream[Raw]
  }

  def apply[Raw](raw: Raw) = new DefaultBitemporalReferenceImplementation[Raw](raw)

  def withId[Raw <: Identified : TypeTag](id: Raw#Id): Bitemporal[Raw] = new IdentifiedItemsBitemporalReferenceImplementation(id)

  def zeroOrOneOf[Raw <: Identified : TypeTag](id: Raw#Id): Bitemporal[Raw] = new ZeroOrOneIdentifiedItemBitemporalReferenceImplementation(id)

  def singleOneOf[Raw <: Identified : TypeTag](id: Raw#Id): Bitemporal[Raw] = new SingleIdentifiedItemBitemporalReferenceImplementation(id)

  def wildcard[Raw <: Identified](): Bitemporal[Raw] = new WildcardBitemporalReferenceImplementation[Raw]

  def none[Raw]: Bitemporal[Raw] = new DefaultBitemporalReferenceImplementation[Raw]

  // TODO - something that makes Bitemporal[Instant] to provide a way of snooping into the scope's 'when' from within the monad.

  // TODO - something that takes a bitemporal and then makes a time-shifted bitemporal from within the monad - for PnL.
}








