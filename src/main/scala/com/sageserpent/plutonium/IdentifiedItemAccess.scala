package com.sageserpent.plutonium

/**
  * Created by Gerard on 17/01/2016.
  */
trait IdentifiedItemAccess {
  def reconstitute[Item](
      itemReconstitutionData: Recorder#ItemReconstitutionData[Item]): Item
}
