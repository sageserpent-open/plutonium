package com.sageserpent.plutonium

/**
 * Created by Gerard on 03/09/2015.
 */

import scala.reflect.runtime.universe._

class ZeroOrOneItemBitemporalReferenceImplementation[Raw <: Identified: TypeTag](id: Raw#Id) extends AbstractBitemporalReferenceImplementation[Raw] {
  override def interpret(scope: Bitemporal.Scope): Stream[Raw] = scope.itemsFor(id) match {
    case _ #:: _ => throw new RuntimeException(s"Id: '${id}' matches more than one item of type: '${implicitly[TypeTag[Raw]].tpe}'.")
    case zeroOrOneItems => zeroOrOneItems
  }
}
