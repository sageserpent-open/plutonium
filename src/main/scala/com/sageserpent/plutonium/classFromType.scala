package com.sageserpent.plutonium

import com.google.common.cache.{Cache, CacheBuilder}

import scala.reflect.runtime.{currentMirror, universe}
import universe.typeOf

object classFromType {

  val clazzCache: Cache[universe.Type, Class[_]] =
    CacheBuilder.newBuilder().build()

  def apply[Item](reflectedType: universe.Type): Class[Item] =
    clazzCache
      .get(reflectedType, { () =>
        if (typeOf[Any] =:= reflectedType) classOf[Any]
        else
          currentMirror
            .runtimeClass(reflectedType)
      })
      .asInstanceOf[Class[Item]]
}
