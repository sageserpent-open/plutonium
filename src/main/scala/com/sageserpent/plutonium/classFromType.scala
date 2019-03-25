package com.sageserpent.plutonium

import scala.reflect.runtime.{currentMirror, universe}
import universe.typeOf

object classFromType {
  def apply[Item](reflectedType: universe.Type): Class[Item] =
    (if (typeOf[Any] =:= reflectedType) classOf[Any]
     else
       currentMirror
         .runtimeClass(reflectedType))
      .asInstanceOf[Class[Item]]
}
