package com.sageserpent.plutonium

import java.lang.reflect.Method

import com.sageserpent.plutonium.BlobStorage.UniqueItemSpecification

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

abstract class AbstractPatch {
  val method: Method
  val targetItemSpecification: UniqueItemSpecification
  val argumentItemSpecifications: Seq[UniqueItemSpecification]
  lazy val (targetId, targetTypeTag) = targetItemSpecification
  def apply(identifiedItemAccess: IdentifiedItemAccess): Unit
  def checkInvariants(identifiedItemAccess: IdentifiedItemAccess): Unit
}
