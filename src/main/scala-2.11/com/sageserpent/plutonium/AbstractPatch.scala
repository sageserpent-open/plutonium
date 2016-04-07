package com.sageserpent.plutonium

import java.lang.reflect.Method

/**
  * Created by Gerard on 10/01/2016.
  */

object AbstractPatch {
  def patchesAreRelated(lhs: AbstractPatch, rhs: AbstractPatch): Boolean = {
    val bothReferToTheSameItem = lhs.id == rhs.id && (lhs.typeTag.tpe <:< rhs.typeTag.tpe || rhs.typeTag.tpe <:< lhs.typeTag.tpe)
    val bothReferToTheSameMethod = WorldReferenceImplementation.firstMethodIsOverrideCompatibleWithSecond(lhs.method, rhs.method) ||
      WorldReferenceImplementation.firstMethodIsOverrideCompatibleWithSecond(rhs.method, lhs.method)
    bothReferToTheSameItem && bothReferToTheSameMethod
  }
}

abstract class AbstractPatch(val method: Method){
  val targetReconstitutionData: Recorder#ItemReconstitutionData[_ <: Identified]
  val id: Identified#Id
  val typeTag: scala.reflect.runtime.universe.TypeTag[_ <: Identified]
  def apply(identifiedItemAccess: IdentifiedItemAccess): Unit
  def checkInvariant(identifiedItemAccess: IdentifiedItemAccess): Unit
}


