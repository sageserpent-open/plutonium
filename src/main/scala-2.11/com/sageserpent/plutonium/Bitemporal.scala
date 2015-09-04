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

  def interpret(scope: Bitemporal.Scope): Stream[Raw]

  def join[Raw2 <: Raw](another: Bitemporal[Raw2]): Bitemporal[Raw]
}

// This companion object can produce a bitemporal instance that refers to zero, one or many raw instances depending
// how many of those raw instances match the id or wildcard.
object Bitemporal {

  trait Scope {
    def itemsFor[Raw <: Identified](id: Raw#Id): Stream[Raw]

    def allItems[Raw <: Identified](): Stream[Raw]
  }

  def apply[Raw](raw: Raw) = new DefaultBitemporalReferenceImplementation[Raw](raw)

  def withId[Raw <: Identified : TypeTag](id: Raw#Id): Bitemporal[Raw] = new ZeroOrOneItemBitemporalReferenceImplementation(id)

  // NOTE: if there is either no or several instances matching the id, a precondition exception is thrown when it is rendered.
  def singleOneOf[Raw <: Identified : TypeTag](id: Raw#Id): Bitemporal[Raw] = new SingleItemBitemporalReferenceImplementation(id)

  def wildcard[Raw <: Identified](): Bitemporal[Raw] = new WildcardBitemporalReferenceImplementation[Raw]

  def none[Raw]: Bitemporal[Raw] = new DefaultBitemporalReferenceImplementation[Raw]

  // TODO - something that makes Bitemporal[Instant] to provide a way of snooping into the scope's 'when' from within the monad.

  // TODO - something that takes a bitemporal and then makes a time-shifted bitemporal from within the monad - for PnL.
}








