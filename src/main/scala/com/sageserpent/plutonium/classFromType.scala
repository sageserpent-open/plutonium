package com.sageserpent.plutonium

import scala.reflect.runtime.{currentMirror, universe}
import universe.typeOf
import scalacache._
import scalacache.caffeine._
import scalacache.memoization._
import scalacache.modes.sync._

import scala.concurrent.duration._

object classFromType {
  private val cacheTimeToLive = Some(10 minutes)

  implicit val clazzCache: Cache[Class[Any]] = CaffeineCache[Class[Any]](
    CacheConfig.defaultCacheConfig.copy(
      memoization = MemoizationConfig(
        (fullClassName: String,
         constructorParameters: IndexedSeq[IndexedSeq[Any]],
         methodName: String,
         parameters: IndexedSeq[IndexedSeq[Any]]) => parameters.head.toString)))

  def apply[Item](reflectedType: universe.Type): Class[Item] =
    memoizeSync(cacheTimeToLive) {
      if (typeOf[Any] =:= reflectedType) classOf[Any]
      else
        currentMirror
          .runtimeClass(reflectedType)
          .asInstanceOf[Class[Any]]
    }.asInstanceOf[Class[Item]]
}
