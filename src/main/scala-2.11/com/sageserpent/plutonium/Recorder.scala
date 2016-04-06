package com.sageserpent.plutonium
import scala.reflect.runtime.universe._

/**
  * Created by Gerard on 03/04/2016.
  */
protected [plutonium] trait Recorder {
  type ItemReconstitutionData[Raw2 <: Identified] = (Raw2#Id, TypeTag[Raw2])

  def itemReconstitutionData: ItemReconstitutionData[_ <: Identified] = ???
}
