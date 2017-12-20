package com.sageserpent.plutonium

import com.sageserpent.plutonium.BlobStorage.UniqueItemSpecification

trait IdentifiedItemAccess {
  def reconstitute(itemReconstitutionData: UniqueItemSpecification): Any
}
