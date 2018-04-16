package com.sageserpent.plutonium

trait ItemStateUpdateKeyTrackingApi[EventId] {
  def setItemStateUpdateKey(
      itemStateUpdateKey: ItemStateUpdate.Key[EventId]): Unit

  def itemStateUpdateKey: ItemStateUpdate.Key[EventId]
}
