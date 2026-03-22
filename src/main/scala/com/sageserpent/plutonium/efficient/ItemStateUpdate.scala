package com.sageserpent.plutonium.efficient
import com.sageserpent.plutonium.{AbstractPatch, Annihilation}

sealed trait ItemStateUpdate

case class ItemStatePatch(patch: AbstractPatch) extends ItemStateUpdate

case class ItemStateAnnihilation(annihilation: Annihilation)
    extends ItemStateUpdate
