package com.sageserpent.plutonium

/**
 * Created by Gerard on 09/07/2015.
 */
trait Bitemporal[Raw] {
  def filter(predicate: Raw => Boolean): Bitemporal[Raw]

  def map[Raw2](transform: Raw => Raw2): Bitemporal[Raw2]

  def flatMap[Raw2](stage: Raw => Bitemporal[Raw2]): Bitemporal[Raw2]
}

// This companion object can produce a bitemporal instance that refers to zero, one or many raw instances depending
// how many of those raw instances match the id or wildcard.
object Bitemporal{
  def apply[Raw](raw: Raw) = new BitemporalReferenceImplementation[Raw](raw)

  def withId[Raw <: Identified](id: Raw#Id): Bitemporal[Raw] = ??? // TODO - don't we need a manifest type for 'Raw'?

  def wildcard[Raw <: Identified](): Bitemporal[Raw] = new BitemporalReferenceImplementation[Raw]

  def none[Raw]: Bitemporal[Raw] = new BitemporalReferenceImplementation[Raw]

  // TODO - something that makes Bitemporal[Instant] to provide a way of snooping into the scope's 'when' from within the monad.

  // TODO - something that takes a bitemporal and then makes a time-shifted bitemporal from within the monad - for PnL.
}








