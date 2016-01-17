package com.sageserpent.plutonium

/**
  * Created by Gerard on 17/01/2016.
  */

trait RecorderFactory {
  def apply[Raw <: Identified](id: Raw#Id): Raw
}
