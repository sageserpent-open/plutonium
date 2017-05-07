package com.sageserpent.plutonium

import scala.reflect.runtime.universe.TypeTag

/**
  * Created by gerardMurphy on 01/05/2017.
  */
trait ItemStateSnapshot[+EventId] {
  val eventId: EventId
  // Called from implementations of 'ItemStateReferenceResolutionContext.itemFor' - if it needs
  // to resolve an (item id, type tag) pair, it uses 'itemStateReferenceResolutionContext' to do it.
  def reconstitute[Item <: Identified: TypeTag](
      itemStateReferenceResolutionContext: ItemStateReferenceResolutionContext)
    : Item
}
