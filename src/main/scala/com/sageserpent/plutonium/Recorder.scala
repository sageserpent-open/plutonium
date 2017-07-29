package com.sageserpent.plutonium
import scala.reflect.runtime.universe._

protected[plutonium] trait Recorder {
  type ItemReconstitutionData[Item2 <: Identified] = (Item2#Id, TypeTag[Item2])

  def itemReconstitutionData: ItemReconstitutionData[_ <: Identified]
}
