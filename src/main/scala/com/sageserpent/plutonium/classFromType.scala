package com.sageserpent.plutonium

import scala.reflect.runtime.{currentMirror, universe}

object classFromType {
  def apply[Item](reflectedType: universe.Type): Class[Item] =
    currentMirror
      .runtimeClass(reflectedType)
      .asInstanceOf[Class[Item]]
}
