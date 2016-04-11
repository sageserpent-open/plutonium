package com.sageserpent.plutonium

trait Identified {
  type Id
  val id: Id

  // If an item has been annihilated, it will not be accessible from a query on a scope - but
  // if there was an event that made another item refer to the annihilated one earlier in time,
  // then it is possible for that other item to have a 'dangling reference' to the annihilated
  // item. In this case, the annihilated item is considered to be a 'ghost', and the only calls
  // that can be made on it are non-mutative ones on 'isGhost', 'id' plus inherited non-mutative
  // properties / methods from 'Any'.
  def isGhost: Boolean = false

  def checkInvariant(): Unit = {
  }
}


