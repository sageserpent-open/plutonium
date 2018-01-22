package com.sageserpent.plutonium

import java.util.UUID

trait AnnihilationHook {
  def recordAnnihilation(): Unit

  def setLifecycleUUID(uuid: UUID): Unit
}
