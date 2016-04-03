package com.sageserpent.plutonium
import scala.reflect.runtime.universe._

/**
  * Created by Gerard on 03/04/2016.
  */
protected [plutonium] trait Recorder {
  def typeTag: TypeTag[this.type] = ???
}
