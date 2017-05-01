package com.sageserpent.plutonium

import scala.reflect.runtime.universe.TypeTag

/**
  * Created by gerardMurphy on 20/04/2017.
  */
trait ItemStateReferenceResolutionContext extends ItemIdQueryApi {
  // This will go fetch a snapshot from somewhere - storage or whatever and self-populate if necessary.
  def itemsFor[Item <: Identified: TypeTag](id: Item#Id): Stream[Item]
}
