package com.sageserpent.plutonium

trait ItemStateUpdateKeyTrackingApi[EventId] {
  def setItemStateUpdateKey(itemStateUpdateKey: Any): Unit

  def itemStateUpdateKey: Option[ItemStateUpdate.Key[EventId]]
}
