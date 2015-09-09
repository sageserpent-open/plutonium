package com.sageserpent.plutonium

/**
 * Created by Gerard on 03/09/2015.
 */

import scala.reflect.runtime.universe._



class ZeroOrOneIdentifiedItemBitemporalReferenceImplementation[Raw <: Identified: TypeTag](id: Raw#Id) extends IdentifiedItemsBitemporalReferenceImplementation[Raw](id) {
  override def interpret(scope: Bitemporal.IdentifiedItemsScope): Stream[Raw] = super.interpret(scope) match {
    case zeroOrOneItems @ (Stream.Empty | _#:: Stream.Empty) => zeroOrOneItems
    case _ => throw new RuntimeException(s"Id: '${id}' matches more than one item of type: '${implicitly[TypeTag[Raw]].tpe}'.")
  }
}
