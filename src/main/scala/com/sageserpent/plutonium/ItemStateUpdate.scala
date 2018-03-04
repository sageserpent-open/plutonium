package com.sageserpent.plutonium

sealed trait ItemStateUpdate

case class ItemStatePatch(patch: AbstractPatch) extends ItemStateUpdate

case class ItemStateAnnihilation(annihilation: Annihilation)
    extends ItemStateUpdate
