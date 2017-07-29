package com.sageserpent.plutonium

trait AnnihilationHook {
  protected var _isGhost = false

  def recordAnnihilation(): Unit = {
    require(!_isGhost)
    _isGhost = true
  }

  def isGhost: Boolean = _isGhost
}
