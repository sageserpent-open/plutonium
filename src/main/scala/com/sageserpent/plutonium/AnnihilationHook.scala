package com.sageserpent.plutonium

/**
  * Created by Gerard on 14/04/2016.
  */
trait AnnihilationHook {
  protected var _isGhost = false

  def recordAnnihilation(): Unit = {
    require(!_isGhost)
    _isGhost = true
  }

  def isGhost: Boolean = _isGhost
}
