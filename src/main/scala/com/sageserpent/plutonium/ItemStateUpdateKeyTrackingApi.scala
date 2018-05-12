package com.sageserpent.plutonium

trait ItemStateUpdateKeyTrackingApi {
  def setItemStateUpdateKey(
      itemStateUpdateKey: Option[ItemStateUpdateKey]): Unit

  def itemStateUpdateKey: Option[ItemStateUpdateKey]
}
