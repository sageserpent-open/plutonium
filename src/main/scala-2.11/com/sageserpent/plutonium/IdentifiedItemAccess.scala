package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded

/**
  * Created by Gerard on 17/01/2016.
  */

trait IdentifiedItemAccess {
  def reconstitute[Raw <: Identified](itemReconstitutionData: Recorder#ItemReconstitutionData[Raw], when: Unbounded[Instant]): Raw
}


trait IdentifiedItemAccessContracts extends IdentifiedItemAccess {
  abstract override def reconstitute[Raw <: Identified](itemReconstitutionData: Recorder#ItemReconstitutionData[Raw], when: Unbounded[Instant]): Raw = {
    val result = super.reconstitute(itemReconstitutionData, when)
    require(itemReconstitutionData._1 == result.id)
    result
  }
}


