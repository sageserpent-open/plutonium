package com.sageserpent.plutonium

object ItemStateUpdate {
  type IntraEventIndex = Int
  case class Key[EventId](eventId: EventId, intraEventIndex: IntraEventIndex)
}

sealed trait ItemStateUpdate

case class ItemStatePatch(patch: AbstractPatch) extends ItemStateUpdate

case class ItemStateAnnihilation(annihilation: Annihilation)
    extends ItemStateUpdate
