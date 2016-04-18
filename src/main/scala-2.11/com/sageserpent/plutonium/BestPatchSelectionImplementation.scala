package com.sageserpent.plutonium

/**
  * Created by Gerard on 10/01/2016.
  */
trait BestPatchSelectionImplementation[FBoundedOperationClassifier <: OperationClassifier[FBoundedOperationClassifier]] extends BestPatchSelection[FBoundedOperationClassifier] {
  override def apply(relatedPatches: Seq[AbstractPatch[FBoundedOperationClassifier]]): AbstractPatch[FBoundedOperationClassifier] = relatedPatches.last
}
