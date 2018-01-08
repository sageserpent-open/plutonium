package com.sageserpent.plutonium

trait AnnihilationHook {
  def recordAnnihilation(): Unit

  def isGhost: Boolean
}
