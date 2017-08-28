package com.sageserpent.plutonium

/**
  * Created by Gerard on 17/01/2016.
  */
trait IdentifiedItemAccess {
  def reconstitute[Item <: Identified](
      itemReconstitutionData: Recorder#ItemReconstitutionData[Item]): Item
}

trait IdentifiedItemAccessContracts extends IdentifiedItemAccess {
  abstract override def reconstitute[Item <: Identified](
      itemReconstitutionData: Recorder#ItemReconstitutionData[Item]): Item = {
    val result = super.reconstitute(itemReconstitutionData)
    require(itemReconstitutionData._1 == result.id)
    result
  }
}
