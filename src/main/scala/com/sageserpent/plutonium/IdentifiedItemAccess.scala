package com.sageserpent.plutonium

/**
  * Created by Gerard on 17/01/2016.
  */
trait IdentifiedItemAccess {
  def reconstitute[Item](
      itemReconstitutionData: Recorder#ItemReconstitutionData[Item]): Item
}

trait IdentifiedItemAccessContracts extends IdentifiedItemAccess {
  abstract override def reconstitute[Item](
      itemReconstitutionData: Recorder#ItemReconstitutionData[Item]): Item = {
    val result = super.reconstitute(itemReconstitutionData)
    // TODO: given that we are using what has become a hidden trait to access the id, is this still relevant?
    require(itemReconstitutionData._1 == result.asInstanceOf[Identified].id)
    result
  }
}
