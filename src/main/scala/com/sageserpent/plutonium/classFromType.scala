package com.sageserpent.plutonium

import scala.reflect.runtime.{currentMirror, universe}
import scala.util.Try

object classFromType {
  def apply[Item](reflectedType: universe.Type): Class[Item] = {
    Try { currentMirror.runtimeClass(reflectedType) }.toOption
      .getOrElse(classOf[Any])
      .asInstanceOf[Class[Item]]
  }
}
