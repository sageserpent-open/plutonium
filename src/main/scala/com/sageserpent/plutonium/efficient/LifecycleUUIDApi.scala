package com.sageserpent.plutonium.efficient

import java.util.UUID

trait LifecycleUUIDApi {
  def setLifecycleUUID(uuid: UUID): Unit

  def lifecycleUUID: UUID
}
