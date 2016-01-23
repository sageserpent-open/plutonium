package com.sageserpent.plutonium

import scala.reflect.runtime.universe._

/**
  * Created by Gerard on 10/01/2016.
  */
abstract class AbstractPatch[+Raw <: Identified: TypeTag](val id: Raw#Id){
  def apply(identifiedItemFactory: IdentifiedItemFactory): Unit
}


