package com.sageserpent.plutonium

import scala.reflect.runtime.universe._

/**
  * Created by Gerard on 10/01/2016.
  */

object AbstractPatch {
  def bothPatchesReferToTheSameItem(lhs: AbstractPatch[_], rhs: AbstractPatch[_]): Boolean = {
    lhs.id == rhs.id && (lhs.itemType <:< rhs.itemType || rhs.itemType <:< lhs.itemType)
  }
}

abstract class AbstractPatch[Raw <: Identified: TypeTag](val id: Raw#Id){
  val itemType = typeOf[Raw]
  def apply(identifiedItemFactory: IdentifiedItemFactory): Unit
}


