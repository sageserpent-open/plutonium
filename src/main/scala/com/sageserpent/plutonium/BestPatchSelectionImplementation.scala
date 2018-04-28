package com.sageserpent.plutonium

trait BestPatchSelectionImplementation extends BestPatchSelection {
  override def apply[AssociatedData](
      relatedPatches: Seq[(AbstractPatch, AssociatedData)]) =
    relatedPatches.last
}
