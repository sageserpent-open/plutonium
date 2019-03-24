package com.sageserpent.plutonium

import scala.reflect.runtime.universe.TypeTag

case class UniqueItemSpecification(id: Any, typeTag: TypeTag[_ <: Any])
