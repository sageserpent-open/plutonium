package com.sageserpent.plutonium

/**
 * Created by Gerard on 03/09/2015.
 */
class WildcardBitemporalReferenceImplementation[Raw <: Identified]() extends AbstractBitemporalReferenceImplementation[Raw] {
  override def interpret(scope: Bitemporal.Scope): Stream[Raw] = scope.allItems[Raw]()
}
