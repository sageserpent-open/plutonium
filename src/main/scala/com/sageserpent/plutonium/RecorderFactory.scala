package com.sageserpent.plutonium

import scala.reflect.runtime.universe._

// TODO - document the mysterious behaviour of the items returned.
trait RecorderFactory {
  def apply[Item <: Identified: TypeTag](id: Item#Id): Item
}
