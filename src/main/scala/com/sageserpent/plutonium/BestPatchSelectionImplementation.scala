package com.sageserpent.plutonium

trait BestPatchSelectionImplementation extends BestPatchSelection {
  override def apply(relatedPatches: Seq[AbstractPatch]): AbstractPatch =
    relatedPatches.last
}
