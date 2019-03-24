package com.sageserpent.plutonium

import java.lang.reflect.Method

object AbstractPatch {
  def patchesAreRelated(lhs: AbstractPatch, rhs: AbstractPatch): Boolean = {
    val bothReferToTheSameItem =
      lhs.targetItemSpecification.id == rhs.targetItemSpecification.id && (rhs.targetItemSpecification.clazz
        .isAssignableFrom(lhs.targetItemSpecification.clazz)
        || lhs.targetItemSpecification.clazz
          .isAssignableFrom(rhs.targetItemSpecification.clazz))
    val bothReferToTheSameMethod = WorldImplementationCodeFactoring
      .firstMethodIsOverrideCompatibleWithSecond(lhs.method, rhs.method) ||
      WorldImplementationCodeFactoring
        .firstMethodIsOverrideCompatibleWithSecond(rhs.method, lhs.method)
    bothReferToTheSameItem && bothReferToTheSameMethod
  }

  type ClazzRefinement = UniqueItemSpecification => Class[_]
}

abstract class AbstractPatch {
  def rewriteItemClazzes(
      typeRefinement: AbstractPatch.ClazzRefinement): AbstractPatch

  val method: Method
  val targetItemSpecification: UniqueItemSpecification
  val argumentItemSpecifications: Seq[UniqueItemSpecification]
  def apply(identifiedItemAccess: IdentifiedItemAccess): Unit
  def checkInvariants(identifiedItemAccess: IdentifiedItemAccess): Unit
}
