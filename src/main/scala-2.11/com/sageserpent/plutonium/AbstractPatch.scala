package com.sageserpent.plutonium

import java.lang.reflect.Method

import scala.reflect.runtime.universe._

/**
  * Created by Gerard on 10/01/2016.
  */

object AbstractPatch {
  def patchesAreRelated(lhs: AbstractPatch[_], rhs: AbstractPatch[_]): Boolean = {
    val bothReferToTheSameItem = lhs.id == rhs.id && (lhs.capturedTypeTag.tpe <:< rhs.capturedTypeTag.tpe || rhs.capturedTypeTag.tpe <:< lhs.capturedTypeTag.tpe)
    val bothReferToTheSameMethod = WorldReferenceImplementation.firstMethodIsOverrideCompatibleWithSecond(lhs.method, rhs.method) ||
      WorldReferenceImplementation.firstMethodIsOverrideCompatibleWithSecond(rhs.method, lhs.method)
    bothReferToTheSameItem && bothReferToTheSameMethod
  }
}

abstract class AbstractPatch[Raw <: Identified: TypeTag](val id: Raw#Id, val method: Method){
  val capturedTypeTag = typeTag[Raw]
  def apply(identifiedItemAccess: IdentifiedItemAccess): Unit
  def checkInvariant(identifiedItemAccess: IdentifiedItemAccess): Unit
}


