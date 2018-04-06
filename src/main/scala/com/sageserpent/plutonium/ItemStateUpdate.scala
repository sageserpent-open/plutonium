package com.sageserpent.plutonium

import java.time.Instant

import com.sageserpent.americium.Unbounded

object ItemStateUpdate {
  type IntraEventIndex = Int
  type Key             = (Unbounded[Instant], IntraEventIndex)
}

sealed trait ItemStateUpdate

case class ItemStatePatch(patch: AbstractPatch) extends ItemStateUpdate

case class ItemStateAnnihilation(annihilation: Annihilation)
    extends ItemStateUpdate
