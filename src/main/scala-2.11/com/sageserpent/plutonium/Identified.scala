package com.sageserpent.plutonium

trait Identified {
  type Id
  var id: Id // This can't be mutated! Begs the question - should other attributes be mutable or not?
  // Answer - provide support for in-place mutation of bitemporal objects. It just seems a whole lot more natural. OTOH, this will
  // burn me when it comes to bitemporals that want to be case classes and for PnL style calculations where we get bitemporals
  // for the same id via different scopes.
}


