package com.sageserpent.plutonium

import scala.reflect.runtime.universe._

/**
  * Created by Gerard on 17/01/2016.
  */
// TODO - document the mysterious behaviour of the items returned.
trait RecorderFactory {
  def apply[Item: TypeTag](id: Any): Item
}
