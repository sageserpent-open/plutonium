package com.sageserpent.plutonium

import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification

trait IdentifiedItemAccess {
  def reconstitute(uniqueItemSpecification: UniqueItemSpecification): Any

  def forget(uniqueItemSpecification: UniqueItemSpecification): Unit
}
