package com.sageserpent.plutonium

/**
  * Created by Gerard on 17/01/2016.
  */
trait IdentifiedItemAccess {
  def reconstitute[Raw <: Identified](
      itemReconstitutionData: Recorder#ItemReconstitutionData[Raw]): Raw
}

trait IdentifiedItemAccessContracts extends IdentifiedItemAccess {
  abstract override def reconstitute[Raw <: Identified](
      itemReconstitutionData: Recorder#ItemReconstitutionData[Raw]): Raw = {
    val result = super.reconstitute(itemReconstitutionData)
    require(itemReconstitutionData._1 == result.id)
    result
  }
}
