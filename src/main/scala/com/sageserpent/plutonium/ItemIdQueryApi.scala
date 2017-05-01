package com.sageserpent.plutonium

import scala.reflect.runtime.universe.TypeTag

/**
  * Created by gerardMurphy on 20/04/2017.
  */
trait ItemIdQueryApi {
  def idsFor[Item <: Identified: TypeTag]: Stream[Item#Id]
}
