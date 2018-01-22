package com.sageserpent.plutonium

import java.util.UUID

trait AnnihilationHook {
  def recordAnnihilation(): Unit

  def isGhost: Boolean

  def lifecycleUUID: UUID

  def setLifecycleUUID(uuid: UUID): Unit
}
