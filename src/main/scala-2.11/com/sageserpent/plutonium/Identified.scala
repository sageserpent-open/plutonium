package com.sageserpent.plutonium

trait Identified {
  type Id
  val id: Id

  private var _isGhost = false

  protected [plutonium] def recordAnnihilation(): Unit = {
    require(!_isGhost)
    _isGhost = true
  }

  // If an item has been annihilated, it will not be accessible from a query on a scope - but
  // if there was an event that made another item refer to the annihilated one earlier in time,
  // then it is possible for that other item to have a reference to the annihilated item. In
  // this case, the annihilated item is considered to be a 'ghost'.
  def isGhost: Boolean = _isGhost

  def checkInvariant(): Unit = {
  }
}


