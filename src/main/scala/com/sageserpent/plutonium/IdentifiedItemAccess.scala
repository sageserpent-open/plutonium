package com.sageserpent.plutonium

trait IdentifiedItemAccess {
  def reconstitute[Item](
      itemReconstitutionData: Recorder#ItemReconstitutionData[Item]): Item
}
