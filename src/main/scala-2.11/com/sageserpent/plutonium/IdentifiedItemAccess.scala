package com.sageserpent.plutonium

/**
  * Created by Gerard on 17/01/2016.
  */

trait IdentifiedItemAccess {
  def itemFor[Raw <: Identified](itemReconstitutionData: Recorder#ItemReconstitutionData[Raw]): Raw
}


trait IdentifiedItemAccessContracts extends IdentifiedItemAccess {
  abstract override def itemFor[Raw <: Identified](itemReconstitutionData: Recorder#ItemReconstitutionData[Raw]): Raw = {
    val result = super.itemFor(itemReconstitutionData)
    require(itemReconstitutionData._1 == result.id)
    result
  }
}


