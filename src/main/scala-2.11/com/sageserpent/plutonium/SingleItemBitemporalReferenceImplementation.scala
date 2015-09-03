package com.sageserpent.plutonium

/**
 * Created by Gerard on 03/09/2015.
 */

import scala.reflect.runtime.universe._

class SingleItemBitemporalReferenceImplementation[Raw <: Identified: TypeTag](id: Raw#Id) extends ZeroOrOneItemBitemporalReferenceImplementation[Raw](id) {
  override def interpret(scope: Bitemporal.Scope): Stream[Raw] = super.interpret(scope) match {
    case Stream.Empty => throw new RuntimeException(s"Id: '${id}' does not match any items of type: '${implicitly[TypeTag[Raw]].tpe}'.")
    case result@Stream(_) => result
  }
}
