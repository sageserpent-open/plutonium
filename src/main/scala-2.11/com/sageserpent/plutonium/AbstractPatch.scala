package com.sageserpent.plutonium

/**
  * Created by Gerard on 10/01/2016.
  */

object AbstractPatch {
  def patchesAreRelated[FBoundedOperationClassifier <: OperationClassifier[FBoundedOperationClassifier]](lhs: AbstractPatch[FBoundedOperationClassifier], rhs: AbstractPatch[FBoundedOperationClassifier]): Boolean = {
    val bothReferToTheSameItem = lhs.targetId == rhs.targetId && (lhs.targetTypeTag.tpe <:< rhs.targetTypeTag.tpe || rhs.targetTypeTag.tpe <:< lhs.targetTypeTag.tpe)
    val bothReferToTheSameMethod = lhs.operationClassifier.isCompatibleWith(rhs.operationClassifier) || rhs.operationClassifier.isCompatibleWith(lhs.operationClassifier)
    bothReferToTheSameItem && bothReferToTheSameMethod
  }
}

trait OperationClassifier[FBoundedOperationClassifier <: OperationClassifier[FBoundedOperationClassifier]] {
  self: FBoundedOperationClassifier =>
    def isCompatibleWith(another: FBoundedOperationClassifier): Boolean
}

abstract trait AbstractPatch[FBoundedOperationClassifier <: OperationClassifier[FBoundedOperationClassifier]] {
  val operationClassifier: FBoundedOperationClassifier

  val targetReconstitutionData: Recorder#ItemReconstitutionData[_ <: Identified]
  val argumentReconstitutionDatums: Seq[Recorder#ItemReconstitutionData[_ <: Identified]]
  val targetId: Identified#Id
  val targetTypeTag: scala.reflect.runtime.universe.TypeTag[_ <: Identified]
  def apply(identifiedItemAccess: IdentifiedItemAccess): Unit
  def checkInvariant(identifiedItemAccess: IdentifiedItemAccess): Unit
}


