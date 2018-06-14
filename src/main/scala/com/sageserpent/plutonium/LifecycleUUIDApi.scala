package com.sageserpent.plutonium

import java.util.UUID

trait LifecycleUUIDApi {
  def setLifecycleUUID(uuid: UUID): Unit

  def lifecycleUUID: UUID
}
