package com.sageserpent.plutonium

import java.lang.reflect.Method
import scala.reflect.runtime.universe._

import com.sageserpent.plutonium.ItemExtensionApi.UniqueItemSpecification

object AbstractPatch {
  def patchesAreRelated(lhs: AbstractPatch, rhs: AbstractPatch): Boolean = {
    val bothReferToTheSameItem = lhs.targetId == rhs.targetId && (lhs.targetTypeTag.tpe <:< rhs.targetTypeTag.tpe || rhs.targetTypeTag.tpe <:< lhs.targetTypeTag.tpe)
    val bothReferToTheSameMethod = WorldImplementationCodeFactoring
      .firstMethodIsOverrideCompatibleWithSecond(lhs.method, rhs.method) ||
      WorldImplementationCodeFactoring
        .firstMethodIsOverrideCompatibleWithSecond(rhs.method, lhs.method)
    bothReferToTheSameItem && bothReferToTheSameMethod
  }
}

// TODO: will need to be able to lower the typetags for the target and arguments somehow if we are going to build an update plan with these.
abstract class AbstractPatch {
  def rewriteItemTypeTags(
      uniqueItemSpecificationToTypeTagMap: collection.Map[
        UniqueItemSpecification,
        TypeTag[_]]): AbstractPatch

  val method: Method
  val targetItemSpecification: UniqueItemSpecification
  val argumentItemSpecifications: Seq[UniqueItemSpecification]
  lazy val (targetId, targetTypeTag) = targetItemSpecification
  def apply(identifiedItemAccess: IdentifiedItemAccess): Unit
  def checkInvariants(identifiedItemAccess: IdentifiedItemAccess): Unit
}
