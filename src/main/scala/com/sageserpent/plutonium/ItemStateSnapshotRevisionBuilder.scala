package com.sageserpent.plutonium

import java.time.Instant

import scala.reflect.runtime.universe.TypeTag

/**
  * Created by gerardMurphy on 01/05/2017.
  */
trait ItemStateSnapshotRevisionBuilder[EventId] {
  // Once this has been called, the receiver will throw precondition failures on subsequent use.
  def build(): ItemStateSnapshotStorage[EventId]

  def recordSnapshot[Item <: Identified: TypeTag](eventId: EventId,
                                                  id: Item#Id,
                                                  when: Instant,
                                                  snapshot: ItemStateSnapshot)
}
