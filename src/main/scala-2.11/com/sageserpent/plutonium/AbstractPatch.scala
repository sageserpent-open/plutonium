package com.sageserpent.plutonium

import scala.reflect.runtime.universe._

/**
  * Created by Gerard on 10/01/2016.
  */

object AbstractPatch {
  def bothPatchesReferToTheSameItem(lhs: AbstractPatch[_], rhs: AbstractPatch[_]): Boolean = {
    lhs.id == rhs.id && (lhs.capturedTypeTag.tpe <:< rhs.capturedTypeTag.tpe || rhs.capturedTypeTag.tpe <:< lhs.capturedTypeTag.tpe)
  }
}

abstract class AbstractPatch[Raw <: Identified: TypeTag](val id: Raw#Id){
  val capturedTypeTag = typeTag[Raw]
  def apply(identifiedItemFactory: IdentifiedItemAccess): Unit
  def checkInvariant(scope: Scope): Unit
}


