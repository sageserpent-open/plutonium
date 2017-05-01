package com.sageserpent.plutonium

import java.time.Instant

import scala.reflect.runtime.universe.TypeTag

/**
  * Created by gerardMurphy on 01/05/2017.
  */
trait ItemStateSnapshotRevisionBuilder {
  // Once this has been called, the receiver will throw precondition failures on subsequent use.
  def build(): ItemStateSnapshotStorage

  def recordSnapshot[Item <: Identified: TypeTag](id: Item#Id,
                                                  when: Instant,
                                                  snapshot: ItemStateSnapshot)
}
