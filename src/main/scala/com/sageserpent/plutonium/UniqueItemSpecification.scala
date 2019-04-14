package com.sageserpent.plutonium

import scala.reflect.runtime.universe.Type

object UniqueItemSpecification {
  def apply(id: Any, typeTagType: Type): UniqueItemSpecification =
    UniqueItemSpecification(id, classFromType(typeTagType))
}

case class UniqueItemSpecification(id: Any, clazz: Class[_])
