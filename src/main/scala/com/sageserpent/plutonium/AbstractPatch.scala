package com.sageserpent.plutonium

import java.lang.reflect.Method

import scala.reflect.runtime.universe._

object AbstractPatch {
  def patchesAreRelated(lhs: AbstractPatch, rhs: AbstractPatch): Boolean = {
    val bothReferToTheSameItem =
      lhs.targetItemSpecification.id == rhs.targetItemSpecification.id && (lhs.targetItemSpecification.typeTag.tpe <:< rhs.targetItemSpecification.typeTag.tpe
        || rhs.targetItemSpecification.typeTag.tpe <:< lhs.targetItemSpecification.typeTag.tpe)
    val bothReferToTheSameMethod = WorldImplementationCodeFactoring
      .firstMethodIsOverrideCompatibleWithSecond(lhs.method, rhs.method) ||
      WorldImplementationCodeFactoring
        .firstMethodIsOverrideCompatibleWithSecond(rhs.method, lhs.method)
    bothReferToTheSameItem && bothReferToTheSameMethod
  }

  type TypeRefinement = UniqueItemSpecification => TypeTag[_]
}

// TODO: will need to be able to lower the typetags for the target and arguments somehow if we are going to build an update plan with these.
abstract class AbstractPatch {
  def rewriteItemTypeTags(
      typeRefinement: AbstractPatch.TypeRefinement): AbstractPatch

  val method: Method
  val targetItemSpecification: UniqueItemSpecification
  val argumentItemSpecifications: Seq[UniqueItemSpecification]
  def apply(identifiedItemAccess: IdentifiedItemAccess): Unit
  def checkInvariants(identifiedItemAccess: IdentifiedItemAccess): Unit
}
