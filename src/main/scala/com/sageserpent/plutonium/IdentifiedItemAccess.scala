package com.sageserpent.plutonium

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
