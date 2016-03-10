package com.sageserpent.plutonium

import scalaz.syntax.monad._

trait Identified {
  type Id
  val id: Id // This can't be mutated! Begs the question - should other attributes be mutable or not?
  // Answer - provide support for in-place mutation of bitemporal objects. It just seems a whole lot more natural. OTOH, this will
  // burn me when it comes to bitemporals that want to be case classes and for PnL style calculations where we get bitemporals
  // for the same id via different scopes.

  // This yields a bitemporal unit value whose execution should check the invariant. When overriding this method,
  // it is vital that the subclass uses a for-comprehension to make the conjunction of the subclass part of the
  // invariant with that of the superclass, as the invariant has to be executed *later* when rendered against a
  // scope.
  def checkInvariant: Bitemporal[() => Unit] = (() => ()).point
}


