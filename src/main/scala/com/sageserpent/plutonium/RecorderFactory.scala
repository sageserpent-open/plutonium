package com.sageserpent.plutonium

// TODO - document the mysterious behaviour of the items returned.
trait RecorderFactory {
  def apply[Item](uniqueItemSpecification: UniqueItemSpecification): Item
}
