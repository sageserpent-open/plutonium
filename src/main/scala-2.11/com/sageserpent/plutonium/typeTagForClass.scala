package com.sageserpent.plutonium

import scala.reflect.api.{TypeCreator, Universe}
import scala.reflect.runtime._
import scala.reflect.runtime.universe._

/**
  * Created by Gerard on 22/04/2016.
  */
private[plutonium] object typeTagForClass {
  def apply[T](clazz: Class[T]): TypeTag[T] = {
    val classSymbol = currentMirror.classSymbol(clazz)
    val classType   = classSymbol.toType
    TypeTag(
      currentMirror,
      new TypeCreator {
        def apply[U <: Universe with Singleton](
            m: scala.reflect.api.Mirror[U]): U#Type =
          if (m eq currentMirror) classType.asInstanceOf[U#Type]
          else
            throw new IllegalArgumentException(
              s"Type tag defined in $currentMirror cannot be migrated to other mirrors.")
      }
    )
  }
}
