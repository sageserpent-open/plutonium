package com.sageserpent.plutonium

trait IdentifiedItemAccess {
  def reconstitute(uniqueItemSpecification: UniqueItemSpecification): Any

  def noteAnnihilation(uniqueItemSpecification: UniqueItemSpecification): Unit
}
