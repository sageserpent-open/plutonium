package com.sageserpent.plutonium

/**
 * Created by Gerard on 06/09/2015.
 */

import scala.reflect.runtime.universe._

class IdentifiedItemsBitemporalReferenceImplementation[Raw <: Identified: TypeTag](id: Raw#Id) extends AbstractBitemporalReferenceImplementation[Raw] {
  override def interpret(scope: Bitemporal.Scope): Stream[Raw] = scope.itemsFor(id)
}
