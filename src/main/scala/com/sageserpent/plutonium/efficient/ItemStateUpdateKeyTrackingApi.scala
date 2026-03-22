package com.sageserpent.plutonium.efficient

trait ItemStateUpdateKeyTrackingApi {
  def setItemStateUpdateKey(
      itemStateUpdateKey: Option[ItemStateUpdateKey]): Unit

  def itemStateUpdateKey: Option[ItemStateUpdateKey]
}
