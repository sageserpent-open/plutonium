package com.sageserpent.plutonium

trait ItemStateUpdateKeyTrackingApi[EventId] {
  def setItemStateUpdateKey(
      itemStateUpdateKey: Option[ItemStateUpdate.Key[EventId]]): Unit

  def itemStateUpdateKey: Option[ItemStateUpdate.Key[EventId]]
}
