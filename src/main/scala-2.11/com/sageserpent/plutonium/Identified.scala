package com.sageserpent.plutonium

import scalaz.syntax.monad._

trait Identified {
  type Id
  val id: Id // This can't be mutated! Begs the question - should other attributes be mutable or not?
  // Answer - provide support for in-place mutation of bitemporal objects. It just seems a whole lot more natural. OTOH, this will
  // burn me when it comes to bitemporals that want to be case classes and for PnL style calculations where we get bitemporals
  // for the same id via different scopes.

  // This checks the invariant across the state of the receiver and any bitemporal
  // objects it depends on, taking the state of the world wrt some specific scope.
  // If the invariant fails, a runtime exception should be thrown. Violating the bitemporal
  // invariant is considered an admissible failure, unlike for a conventional invariant,
  // where it would be a fatal assertion failure. This is to allow a world to check the
  // effects of a revision, which may cause an inconsistency.
  def checkBitemporalInvariant: Bitemporal[Unit] = ().point[Bitemporal]
}


