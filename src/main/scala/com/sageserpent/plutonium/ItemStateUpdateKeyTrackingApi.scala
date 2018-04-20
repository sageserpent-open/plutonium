package com.sageserpent.plutonium

trait ItemStateUpdateKeyTrackingApi {
  def setItemStateUpdateKey(
      itemStateUpdateKey: Option[ItemStateUpdate.Key]): Unit

  def itemStateUpdateKey: Option[ItemStateUpdate.Key]
}
