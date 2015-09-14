package com.sageserpent.plutonium

import scala.reflect.runtime.universe._

/**
 * Created by Gerard on 03/09/2015.
 */
class WildcardBitemporalReferenceImplementation[Raw <: Identified: TypeTag]() extends AbstractBitemporalReferenceImplementation[Raw] {
  override def interpret(scope: Bitemporal.IdentifiedItemsScope): Stream[Raw] = scope.allItems[Raw]()
}
